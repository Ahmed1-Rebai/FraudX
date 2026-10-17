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
import time
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


class ActiveModels:
    """The active detectors, reloaded when the registry's active versions change."""

    def __init__(
        self,
        active_ids: Callable[[], dict[str, str]],
        load: Callable[[], dict[str, Detector]],
        check_every_s: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._active_ids, self._load = active_ids, load
        self._every, self._clock = check_every_s, clock
        self._ids: dict[str, str] | None = None
        self._detectors: dict[str, Detector] = {}
        self._checked = -float("inf")

    def get(self) -> dict[str, Detector]:
        if self._clock() - self._checked >= self._every:
            self._checked = self._clock()
            try:
                ids = self._active_ids()
                if ids != self._ids:
                    self._detectors, self._ids = self._load(), ids
                    log.info("models loaded", extra={"models": ids})
            except Exception:
                log.exception("model reload failed; keeping the loaded models")
        return self._detectors


def registry_ids(db: Database) -> dict[str, str]:
    """Active model ids by model name (what ``ActiveModels`` watches)."""
    return {
        name: doc["_id"]
        for name in SIGNAL_MODELS.values()
        if (doc := registry.active(db, name)) is not None
    }
