"""Optional LLM explanations with Claude (``EXPLAIN_PROVIDER=anthropic``, ADR-009).

Claude receives the **pseudonymised** evidence only (no identifiers; IPs and
raw balances are never part of the evidence) and returns JSON constrained by
a schema: a summary and reasons, each citing the evidence keys it relies on.
Reasons citing a key that isn't in the evidence are dropped (``grounded``).
Any failure (API error, refusal, no grounded reason left) falls back to the
deterministic template, so an alert always gets an explanation.

The client reads ``ANTHROPIC_API_KEY`` from the environment.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import anthropic

from fraudx.explain.evidence import pseudonymise
from fraudx.explain.template import Explanation, Reason, grounded
from fraudx.explain.template import explain as template_explain

log = logging.getLogger(__name__)

SYSTEM = """\
You explain fraud-risk decisions to bank fraud analysts.
You receive the evidence behind one decision as JSON: flat keys such as
"signals.graph" (a calibrated fraud probability from the graph model),
"contribution.graph" (its share of the weighted score), "rules.<name>"
(a deterministic rule that fired), "features.*" (behaviour of the customer
before this transaction) and "graph.*" (relationships in the transaction graph).
Write a one-sentence summary and up to five reasons, strongest first.
Use only facts present in the evidence and cite, for every reason, the exact
evidence keys it relies on. Do not speculate about intent, identity or facts
the evidence does not contain. Plain, precise language; no markdown."""

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "summary": {"type": "string"},
        "reasons": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "evidence": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["text", "evidence"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["summary", "reasons"],
    "additionalProperties": False,
}


class ClaudeExplainer:
    def __init__(
        self, model: str = "claude-opus-5-5", timeout_s: float = 30.0, client: Any = None
    ) -> None:
        self.model = model
        self.client = client or anthropic.Anthropic(timeout=timeout_s, max_retries=2)

    def explain(self, evidence: dict[str, Any]) -> Explanation:
        try:
            response = self.client.beta.messages.create(
                model=self.model,
                max_tokens=4096,
                system=SYSTEM,
                messages=[{"role": "user", "content": json.dumps(pseudonymise(evidence))}],
                output_config={
                    "effort": "low",
                    "format": {"type": "json_schema", "schema": SCHEMA},
                },
                betas=["server-side-fallback-2026-07-01"],
                fallbacks="default",
            )
        except anthropic.APIError as exc:
            log.warning("LLM explanation failed; using the template",
                        extra={"error": f"{type(exc).__name__}: {exc}"})  # fmt: skip
            return template_explain(evidence)
        if response.stop_reason != "end_turn":  # refusal, max_tokens, ...
            log.warning("LLM explanation stopped early; using the template",
                        extra={"stop_reason": response.stop_reason})  # fmt: skip
            return template_explain(evidence)
        text = next((b.text for b in response.content if b.type == "text"), "")
        data = json.loads(text)  # the schema guarantees valid JSON
        reasons = [Reason(r["text"], tuple(r["evidence"])) for r in data["reasons"]]
        kept, dropped = grounded(reasons, evidence)
        if not kept:
            log.warning("LLM explanation had no grounded reason; using the template")
            return template_explain(evidence)
        return Explanation(data["summary"], tuple(kept[:5]), f"llm:anthropic/{response.model}",
                           dropped)  # fmt: skip
