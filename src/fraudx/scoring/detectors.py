"""The three risk signals, loaded from the active versions in the model registry.

| signal | model | input |
|---|---|---|
| ``ml`` | ``fraud_lgbm`` (LightGBM, §6.1) | transaction features (``fraudx.ml.vectorize``) |
| ``anomaly`` | ``anomaly_iforest`` (Isolation Forest, §6.2) | transaction features |
| ``graph`` | ``graph_lgbm`` (LightGBM, §6.3) | graph features (``fraudx.storage.graph_features``) |

Every signal is the detector's score passed through its own calibration
(P(fraud | score), fitted on validation and stored with the model), so the
risk engine combines three probabilities. A model that isn't registered is
simply absent: the risk engine reweights and reports the missing signal.
The tuning command and the live scorer both use this module.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import numpy as np
from pymongo.database import Database

from fraudx.ml import calibration, registry
from fraudx.ml.anomaly import AnomalyModel
from fraudx.ml.model import FraudModel
from fraudx.storage.graph_features import GRAPH_FEATURE_NAMES, GRAPH_FEATURE_VERSION

log = logging.getLogger(__name__)

SIGNAL_MODELS: dict[str, str] = {
    "ml": "fraud_lgbm",
    "anomaly": "anomaly_iforest",
    "graph": "graph_lgbm",
}


@dataclass(frozen=True)
class Detector:
    signal: str
    model: str  # registry id, name:version
    inputs: Literal["transaction", "graph"]
    raw: Callable[[np.ndarray], np.ndarray]
    calibration: dict[str, Any]

    def score(self, X: np.ndarray) -> np.ndarray:
        """Calibrated fraud probabilities for a matrix of feature vectors."""
        return calibration.apply(self.calibration, self.raw(np.asarray(X, dtype=np.float32)))


def load(signal: str, artifact: Path) -> Detector:
    if signal == "anomaly":
        model: Any = AnomalyModel.load(artifact)
        raw, inputs = model.score_vectors, "transaction"
    elif signal == "graph":
        model = FraudModel.load(artifact, GRAPH_FEATURE_NAMES, GRAPH_FEATURE_VERSION)
        raw, inputs = model.predict_vectors, "graph"
    else:
        model = FraudModel.load(artifact)
        raw, inputs = model.predict_vectors, "transaction"
    metadata = model.metadata
    if "calibration" not in metadata:
        raise ValueError(f"{artifact} has no calibration; retrain it")
    return Detector(
        signal=signal,
        model=f"{metadata['name']}:{metadata['version']}",
        inputs=inputs,
        raw=raw,
        calibration=metadata["calibration"],
    )


def load_active(db: Database) -> dict[str, Detector]:
    """Active detectors by signal name; unregistered signals are left out."""
    detectors = {}
    for signal, name in SIGNAL_MODELS.items():
        doc = registry.active(db, name)
        if doc is None:
            log.warning("no active %s model: the %s signal will be missing", name, signal)
            continue
        detectors[signal] = load(signal, Path(doc["artifact_path"]))
    return detectors
