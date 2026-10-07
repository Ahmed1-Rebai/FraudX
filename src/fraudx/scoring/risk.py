"""Risk engine: combines detector signals into one 0-100 score.

score = 100 * sum(weight_i * signal_i) over the signals that are available.

* Signals are probabilities/normalised scores in [0, 1].
* If a detector is unavailable (timeout, model not loaded) its weight is
  redistributed over the remaining signals instead of silently counting as 0,
  and the gap is reported in ``missing_signals``.
* Deterministic rules can impose a minimum score ("floor"), e.g. a hard
  blocklist hit is HIGH regardless of what the models say.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime

from fraudx.core.config import RiskConfig
from fraudx.core.schemas import RiskAssessment, RiskLevel


@dataclass(frozen=True, slots=True)
class RiskSignals:
    ml: float | None = None
    anomaly: float | None = None
    graph: float | None = None


@dataclass(frozen=True, slots=True)
class RuleHit:
    name: str
    floor: int  # minimum final score imposed by this rule, 0-100


class NoSignalsError(ValueError):
    """Raised when neither a detector signal nor a rule is available."""


def classify(score: int, config: RiskConfig) -> RiskLevel:
    if score >= config.thresholds.high:
        return RiskLevel.HIGH
    if score >= config.thresholds.medium:
        return RiskLevel.MEDIUM
    return RiskLevel.LOW


def assess(
    signals: RiskSignals,
    config: RiskConfig,
    rule_hits: tuple[RuleHit, ...] = (),
) -> RiskAssessment:
    weights = config.weights.model_dump()
    components: dict[str, float] = {}
    missing: list[str] = []
    for name in weights:
        value = getattr(signals, name)
        if value is None:
            missing.append(name)
            continue
        if not 0.0 <= value <= 1.0 or math.isnan(value):
            raise ValueError(f"signal {name!r} must be in [0, 1], got {value}")
        components[name] = value

    available_weight = sum(weights[name] for name in components)
    if available_weight > 0:
        weighted = sum(weights[n] * v for n, v in components.items()) / available_weight
    elif rule_hits:
        weighted = 0.0
    else:
        raise NoSignalsError("no detector signal or rule hit to score")

    # Round half up (x.5 -> x+1) rather than Python's banker's rounding.
    score = math.floor(weighted * 100 + 0.5)
    score = max([score, *(hit.floor for hit in rule_hits)])
    score = min(max(score, 0), 100)

    return RiskAssessment(
        score=score,
        level=classify(score, config),
        components=components,
        missing_signals=tuple(missing),
        rule_hits=tuple(hit.name for hit in rule_hits),
        assessed_at=datetime.now(UTC),
    )


def should_alert(assessment: RiskAssessment, config: RiskConfig) -> bool:
    return assessment.score >= config.alert_threshold
