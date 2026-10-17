"""Alert handler: fraud.alerts -> MongoDB investigation with evidence and explanation.

For each alert (a ``ScoredTransaction`` at or above the alert threshold):

1. read the customer's profile (MongoDB) and build the evidence
   (``fraudx.explain.evidence``);
2. explain it (template, or Claude when configured; always grounded);
3. create the investigation (``_id`` = transaction id, status OPEN) and,
   optionally, POST a summary to a webhook.

Idempotent: an investigation is created once (``$setOnInsert``). A
redelivered alert never resets an analyst's decision or pays for a second
explanation; the batch's existing investigations are skipped up front.
"""

from __future__ import annotations

import json
import logging
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol

from confluent_kafka import Message
from prometheus_client import Counter
from pymongo import UpdateOne
from pymongo.database import Database

from fraudx.core.config import RiskConfig
from fraudx.core.schemas import ScoredTransaction
from fraudx.explain import evidence as evidence_mod
from fraudx.explain.template import Explanation
from fraudx.messaging.consumer import PermanentError, decode

log = logging.getLogger(__name__)

INVESTIGATIONS = Counter("fraudx_investigations_total", "Investigations opened", ["generator"])
WEBHOOK_FAILURES = Counter("fraudx_alert_webhook_failures_total", "Alert webhook failures")


class Explainer(Protocol):
    def explain(self, evidence: dict[str, Any]) -> Explanation: ...


def investigation(
    scored: ScoredTransaction, evidence: dict[str, Any], explanation: Explanation, now: datetime
) -> dict[str, Any]:
    risk = scored.risk
    return {
        "_id": scored.event.transaction_id,
        "customer_id": scored.event.customer_id,
        "event_time": scored.event.event_time,
        "risk": {
            "score": risk.score,
            "level": risk.level.value,
            "components": risk.components,
            "rule_hits": list(risk.rule_hits),
            "missing_signals": list(risk.missing_signals),
        },
        "models": scored.models,
        "evidence": evidence,
        "explanation": {
            "summary": explanation.summary,
            "reasons": [r.text for r in explanation.reasons],
            "reason_evidence": [list(r.evidence) for r in explanation.reasons],
            "generator": explanation.generator,
            "dropped_ungrounded": explanation.dropped,
            "created_at": now,
        },
        "status": "OPEN",
        "created_at": now,
        "updated_at": now,
    }


def alert_text(doc: dict[str, Any]) -> str:
    """The alert as plain text, in the format of the spec (§16)."""
    evidence = doc["evidence"]
    amount = f"{evidence['transaction.amount']:,.2f} {evidence['transaction.currency']}"
    reasons = "\n".join(f"- {r}" for r in doc["explanation"]["reasons"][:3])
    return (
        f"🚨 {doc['risk']['level']} RISK TRANSACTION\n\n"
        f"Transaction: {doc['_id']}\nCustomer: {doc['customer_id']}\n"
        f"Amount: {amount}\nRisk Score: {doc['risk']['score']}/100\n\n"
        f"Primary reasons:\n{reasons}"
    )


def post_webhook(url: str, payload: dict[str, Any], timeout_s: float = 5.0) -> None:
    request = urllib.request.Request(  # noqa: S310 - operator-configured http(s) URL
        url, data=json.dumps(payload, default=str).encode(), method="POST",
        headers={"content-type": "application/json"},
    )  # fmt: skip
    with urllib.request.urlopen(request, timeout=timeout_s):  # noqa: S310
        pass


class AlertHandler:
    def __init__(
        self,
        db: Database,
        explainer: Explainer,
        config: RiskConfig | Callable[[], RiskConfig],
        webhook_url: str | None = None,
        notify: Callable[[str, dict[str, Any]], None] = post_webhook,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.db, self.explainer = db, explainer
        self._config = config if callable(config) else (lambda: config)
        self.webhook_url, self._notify, self._now = webhook_url, notify, now

    def __call__(self, messages: list[Message]) -> list[tuple[Message, PermanentError]]:
        alerts: dict[str, ScoredTransaction] = {}
        rejects: list[tuple[Message, PermanentError]] = []
        for message in messages:
            try:
                scored = decode(message, ScoredTransaction)
            except PermanentError as exc:
                rejects.append((message, exc))
                continue
            alerts[scored.event.transaction_id] = scored
        if not alerts:
            return rejects
        existing = {d["_id"] for d in self.db.investigations.find(
            {"_id": {"$in": list(alerts)}}, {"_id": 1})}  # fmt: skip
        new = [a for tx, a in alerts.items() if tx not in existing]
        profiles = {d["_id"]: d for d in self.db.customers.find(
            {"_id": {"$in": list({a.event.customer_id for a in new})}})}  # fmt: skip
        now, config = self._now(), self._config()
        docs = []
        for scored in new:
            evidence = evidence_mod.build(scored, config, profiles.get(scored.event.customer_id))
            explanation = self.explainer.explain(evidence)
            docs.append(investigation(scored, evidence, explanation, now))
        if docs:
            self.db.investigations.bulk_write(
                [UpdateOne({"_id": d["_id"]}, {"$setOnInsert": d}, upsert=True) for d in docs],
                ordered=False,
            )
        for doc in docs:
            INVESTIGATIONS.labels(doc["explanation"]["generator"].split("/")[0]).inc()
            if self.webhook_url:
                self._send(doc)
        return rejects

    def _send(self, doc: dict[str, Any]) -> None:
        payload = {
            "text": alert_text(doc),  # what Slack, Teams or Mattermost incoming webhooks display
            "transaction_id": doc["_id"], "customer_id": doc["customer_id"],
            "score": doc["risk"]["score"], "level": doc["risk"]["level"],
            "summary": doc["explanation"]["summary"],
            "reasons": doc["explanation"]["reasons"],
        }  # fmt: skip
        try:
            self._notify(self.webhook_url, payload)
        except OSError as exc:  # best effort: the investigation is already stored
            WEBHOOK_FAILURES.inc()
            log.warning("alert webhook failed", extra={"error": str(exc)})
