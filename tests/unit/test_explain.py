import json
from datetime import UTC, datetime
from types import SimpleNamespace

import anthropic
import httpx2

from fraudx.core.config import RiskConfig
from fraudx.core.schemas import (
    RiskAssessment,
    RiskLevel,
    ScoredTransaction,
    TransactionEvent,
    TransactionFeatures,
    TransactionType,
)
from fraudx.explain import evidence as ev
from fraudx.explain.llm import ClaudeExplainer
from fraudx.explain.template import Reason, explain, grounded

CONFIG = RiskConfig.model_validate({
    "weights": {"ml": 0.5, "anomaly": 0.1, "graph": 0.4},
    "thresholds": {"medium": 8, "high": 23}, "alert_threshold": 23,
})  # fmt: skip
T = datetime(2026, 1, 9, 22, 15, tzinfo=UTC)


def _scored(rules=(), graph=None, **features):
    event = TransactionEvent.model_validate({
        "transaction_id": "TX-7", "event_time": T, "customer_id": "C-42",
        "source_account": "A-42", "destination_account": "A-99", "amount": 900,
        "transaction_type": "TRANSFER", "device_id": "D-1", "location": {"country": "FR"},
    })  # fmt: skip
    base = {
        "cust_txn_count_5m": 1, "cust_txn_count_1h": 1, "cust_txn_count_24h": 1,
        "cust_amount_sum_5m": 900, "cust_amount_sum_1h": 900, "cust_amount_sum_24h": 900,
        "cust_history_count": 20, "cust_amount_mean": 100.0, "amount_to_mean_ratio": 9.0,
        "is_new_device": True, "is_new_ip": False, "is_new_country": True,
        "balance_drain_ratio": 0.95, "hour_of_day": 22,
    }  # fmt: skip
    risk = RiskAssessment(score=61, level=RiskLevel.HIGH,
                          components={"ml": 0.9, "anomaly": 0.2, "graph": 0.3},
                          rule_hits=tuple(rules), assessed_at=T)  # fmt: skip
    return ScoredTransaction(
        event=event, features=TransactionFeatures.model_validate({**base, **features}),
        graph_features=graph, risk=risk, models={"ml": "fraud_lgbm:v1"}, scored_at=T,
    )  # fmt: skip


def test_evidence_is_flat_complete_and_pseudonymisable():
    graph = {"payee_owner_ring_size": 4.0, "is_new_payee": 1.0}
    e = ev.build(_scored(["takeover_pattern"], graph), CONFIG, {"country": "TN", "devices": ["a"]})
    assert e["rules.takeover_pattern"] is True and e["graph.payee_owner_ring_size"] == 4.0
    assert e["contribution.ml"] == round(0.5 * 0.9 / (0.45 + 0.02 + 0.12), 4)
    assert e["profile.home_country"] == "TN" and "features.seconds_since_last_txn" not in e
    public = ev.pseudonymise(e)
    assert "transaction.id" not in public and "customer.id" not in public
    assert "C-42" not in json.dumps(public) and "TX-7" not in json.dumps(public)


def test_template_reasons_are_grounded_and_strongest_first():
    graph = {"payee_owner_ring_size": 4.0, "customer_ring_size": 0.0, "is_new_payee": 1.0}
    e = ev.build(_scored(["takeover_pattern"], graph), CONFIG)
    out = explain(e)
    assert out.generator == "template" and out.dropped == 0
    assert out.reasons[0].text.startswith("Account-takeover pattern: a new device moved 95%")
    assert "mule ring" in out.reasons[1].text
    assert all(k in e for r in out.reasons for k in r.evidence)
    assert "TRANSFER of 900.00 TND scored 61/100 (HIGH)" in out.summary
    assert "rule hit: takeover_pattern" in out.summary


def test_fan_in_is_only_suspicious_for_a_customer_payee():
    graph = {"payee_payer_count": 120.0}
    to_customer = ev.build(_scored(graph=graph), CONFIG)
    to_agent = ev.build(
        _scored(graph=graph).model_copy(
            update={
                "event": _scored().event.model_copy(
                    update={
                        "destination_account": "M-00155",
                        "transaction_type": TransactionType.CASH_OUT,
                    }
                )
            }
        ),
        CONFIG,
    )
    assert to_customer["payee.kind"] == "CUSTOMER" and to_agent["payee.kind"] == "MERCHANT"
    assert any("received money from 120" in r.text for r in explain(to_customer).reasons)
    assert not any("received money" in r.text for r in explain(to_agent).reasons)


def test_grounding_drops_fabricated_or_uncited_reasons():
    e = {"risk.score": 61}
    kept, dropped = grounded(
        [Reason("ok", ("risk.score",)), Reason("made up", ("profile.criminal_record",)),
         Reason("no citation", ())], e,
    )  # fmt: skip
    assert [r.text for r in kept] == ["ok"] and dropped == 2


class FakeMessages:
    def __init__(self, response=None, error=None):
        self.response, self.error, self.calls = response, error, []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.response


def _client(response=None, error=None):
    messages = FakeMessages(response, error)
    return SimpleNamespace(beta=SimpleNamespace(messages=messages)), messages


def _response(payload, stop_reason="end_turn"):
    block = SimpleNamespace(type="text", text=json.dumps(payload))
    return SimpleNamespace(content=[block], stop_reason=stop_reason, model="claude-opus-5-5")


def test_claude_explanation_is_grounded_and_sends_no_identifiers():
    e = ev.build(_scored(["takeover_pattern"]), CONFIG)
    payload = {
        "summary": "High risk: a new device drained the account from abroad.",
        "reasons": [
            {"text": "Takeover rule fired.", "evidence": ["rules.takeover_pattern"]},
            {"text": "Known fraudster.", "evidence": ["profile.criminal_record"]},
        ],
    }
    client, messages = _client(_response(payload))
    out = ClaudeExplainer(client=client).explain(e)
    assert out.generator == "llm:anthropic/claude-opus-5-5"
    assert [r.text for r in out.reasons] == ["Takeover rule fired."] and out.dropped == 1
    call = messages.calls[0]
    assert call["model"] == "claude-opus-5-5" and call["fallbacks"] == "default"
    assert call["output_config"]["effort"] == "low"
    assert call["output_config"]["format"]["type"] == "json_schema"
    assert "C-42" not in call["messages"][0]["content"]


def test_claude_failures_fall_back_to_the_template():
    e = ev.build(_scored(["takeover_pattern"]), CONFIG)
    refusal, _ = _client(_response({"summary": "", "reasons": []}, stop_reason="refusal"))
    assert ClaudeExplainer(client=refusal).explain(e).generator == "template"
    request = httpx2.Request("POST", "https://api.anthropic.com/v1/messages")
    down, _ = _client(error=anthropic.APIConnectionError(request=request))
    assert ClaudeExplainer(client=down).explain(e).generator == "template"
    ungrounded, _ = _client(_response({"summary": "x", "reasons": [
        {"text": "y", "evidence": ["nope"]}]}))  # fmt: skip
    assert ClaudeExplainer(client=ungrounded).explain(e).generator == "template"
