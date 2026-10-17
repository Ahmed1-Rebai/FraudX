"""One risk decision, shared by the live scorer and ``POST /models/predict``.

Calibrated signals from the active detectors, the deterministic rules (on the
model's feature vector plus the live known-fraud values) and the risk engine
(``scoring.risk.assess``). Keeping this in one place means the API's
prediction and the pipeline's decision cannot drift apart.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from fraudx.core.config import RiskConfig
from fraudx.core.schemas import RiskAssessment
from fraudx.scoring import rules
from fraudx.scoring.detectors import Detector
from fraudx.scoring.risk import RiskSignals, RuleHit, assess, floor
from fraudx.storage.graph_features import GRAPH_FEATURE_NAMES


@dataclass(frozen=True)
class Decision:
    risk: RiskAssessment
    models: dict[str, str]  # signal -> name:version of the models that ran
    graph_features: dict[str, float] | None  # model graph features + known-fraud values


def decide(
    detectors: dict[str, Detector],
    X: np.ndarray,
    G: np.ndarray | None,
    known: list[dict[str, float]] | None,
    config: RiskConfig,
) -> list[Decision]:
    """Decisions for a batch: ``X`` transaction feature vectors, ``G`` graph feature
    vectors (None: graph signal missing), ``known`` known-fraud values per row."""
    signals: dict[str, np.ndarray] = {}
    for name, detector in detectors.items():
        if detector.inputs == "graph":
            if G is not None:
                signals[name] = detector.score(G)
        else:
            signals[name] = detector.score(X)
    models = {name: detectors[name].model for name in signals}

    out = []
    for i in range(len(X)):
        extra = known[i] if known else {}
        graph = dict(zip(GRAPH_FEATURE_NAMES, G[i].tolist(), strict=True)) if G is not None else {}
        hits = tuple(
            RuleHit(name, floor(level, config))
            for name, level in rules.evaluate(rules.as_values(X[i]) | graph | extra)
        )
        risk = assess(RiskSignals(**{k: float(v[i]) for k, v in signals.items()}), config, hits)
        out.append(Decision(risk, models, (graph | extra) or None))
    return out
