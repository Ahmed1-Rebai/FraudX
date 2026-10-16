"""Unsupervised anomaly detector (Isolation Forest), the risk engine's ``anomaly`` signal.

* Trained on **legitimate** rows of the training split only (the same
  feature matrix and time split as the supervised model), so it learns what
  normal traffic looks like and needs no fraud labels to score.
* Uses the numeric features; categorical codes have no meaningful distance.
  Missing values are filled with medians of the training rows (stored in the
  artifact, so inference fills them the same way).
* **Calibration:** the raw score is mapped to its percentile among
  legitimate validation traffic. 0.99 means "more unusual than 99 % of normal
  transactions", a [0, 1] signal with the same meaning on every dataset.
* Evaluated like the supervised model; the decision threshold is the 99th
  percentile (flags the most unusual 1 % of normal traffic).
"""

from __future__ import annotations

import json
import pickle
import warnings
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import sklearn
from sklearn.ensemble import IsolationForest

from fraudx.core.schemas import FEATURE_VERSION, TransactionEvent, TransactionFeatures
from fraudx.ml import calibration
from fraudx.ml.dataset import FeatureMatrix
from fraudx.ml.train import evaluate, time_split
from fraudx.ml.vectorize import CATEGORICAL, FEATURE_NAMES, vectorize

MODEL_NAME = "anomaly_iforest"
FEATURES: tuple[str, ...] = tuple(n for n in FEATURE_NAMES if n not in CATEGORICAL)
COLUMNS = [FEATURE_NAMES.index(n) for n in FEATURES]
PARAMS: dict[str, Any] = {"n_estimators": 200, "max_samples": 256, "random_state": 42}
THRESHOLD = 0.99
QUANTILES = np.linspace(0, 1, 1001)


@dataclass(frozen=True)
class Calibrator:
    """Raw anomaly score -> percentile among legitimate validation traffic."""

    medians: np.ndarray  # fill values for missing features
    reference: np.ndarray  # raw-score quantiles of legitimate validation rows

    def prepare(self, X: np.ndarray) -> np.ndarray:
        """Select the numeric columns and fill missing values."""
        X = np.asarray(X, dtype=np.float64)[:, COLUMNS]
        return np.where(np.isnan(X), self.medians, X)


def raw_scores(forest: IsolationForest, X: np.ndarray) -> np.ndarray:
    return -forest.score_samples(X)  # higher = more anomalous


def percentile(raw: np.ndarray, reference: np.ndarray) -> np.ndarray:
    return np.interp(raw, reference, QUANTILES)


@dataclass(frozen=True)
class AnomalyModel:
    forest: IsolationForest
    calibrator: Calibrator
    metadata: dict[str, Any]

    def score_vectors(self, rows: np.ndarray | list[list[float]]) -> np.ndarray:
        """Calibrated anomaly scores in [0, 1] for full feature vectors."""
        X = self.calibrator.prepare(np.asarray(rows, dtype=np.float64))
        return percentile(raw_scores(self.forest, X), self.calibrator.reference)

    def score(self, event: TransactionEvent, features: TransactionFeatures) -> float:
        return float(self.score_vectors([vectorize(event, features)])[0])

    def save(self, models_dir: Path) -> Path:
        out = models_dir / MODEL_NAME / self.metadata["version"]
        out.mkdir(parents=True, exist_ok=False)
        with (out / "model.pkl").open("wb") as fh:
            pickle.dump({"forest": self.forest, "calibrator": self.calibrator}, fh)
        text = json.dumps(self.metadata, indent=2) + "\n"
        (out / "metadata.json").write_text(text, encoding="utf-8")
        return out

    @classmethod
    def load(cls, artifact_dir: Path) -> AnomalyModel:
        """Load an artifact written by ``save``. Only load artifacts you trained:
        pickle executes code from the file."""
        metadata = json.loads((artifact_dir / "metadata.json").read_text(encoding="utf-8"))
        if (
            metadata["feature_version"] != FEATURE_VERSION
            or tuple(metadata["feature_names"]) != FEATURES
        ):
            raise ValueError(f"{artifact_dir} was trained on different features")
        with (artifact_dir / "model.pkl").open("rb") as fh:
            parts = pickle.load(fh)  # noqa: S301 - our own artifact
        return cls(parts["forest"], parts["calibrator"], metadata)


def train(
    data: FeatureMatrix, data_sha256: str, dataset: str, holdout: FeatureMatrix | None = None
) -> AnomalyModel:
    split = time_split(data.event_ms)
    X, y = np.asarray(data.X, dtype=np.float64)[:, COLUMNS], data.y
    legit_train = split.train[y[split.train] == 0]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)  # a column with no values at all
        medians = np.nanmedian(X[legit_train], axis=0)
    medians = np.where(np.isnan(medians), 0.0, medians)
    filled = np.where(np.isnan(X), medians, X)

    forest = IsolationForest(**PARAMS, n_jobs=4).fit(filled[legit_train])
    legit_valid = split.valid[y[split.valid] == 0]
    reference = np.quantile(raw_scores(forest, filled[legit_valid]), QUANTILES)
    calibrator = Calibrator(medians=medians, reference=reference)

    def scores(idx: np.ndarray) -> np.ndarray:
        return percentile(raw_scores(forest, filled[idx]), reference)

    prevalence = float(y.mean())
    valid_scores = scores(split.valid)
    metrics: dict[str, Any] = {
        "split": {k: len(getattr(split, k)) for k in ("train", "valid", "test")},
        "valid": evaluate(y[split.valid], valid_scores, THRESHOLD,
                          reference_prevalence=prevalence),
        "test": evaluate(y[split.test], scores(split.test), THRESHOLD,
                         data.scenario[split.test], prevalence),
    }  # fmt: skip
    trained_at = datetime.now(UTC)
    version = f"{trained_at:%Y%m%d%H%M%S}-{data_sha256[:8]}"
    model = AnomalyModel(forest, calibrator, {})
    if holdout is not None:
        metrics["holdout"] = evaluate(
            holdout.y, model.score_vectors(holdout.X), THRESHOLD, holdout.scenario, prevalence
        )
    metadata = {
        "name": MODEL_NAME,
        "version": version,
        "trained_at": trained_at.isoformat(),
        "feature_version": FEATURE_VERSION,
        "feature_names": list(FEATURES),
        "threshold": THRESHOLD,
        "calibration": calibration.fit(valid_scores, y[split.valid]),
        "params": {**PARAMS, "trained_on": "legitimate rows of the training split",
                   "legit_train_rows": len(legit_train)},
        "metrics": metrics,
        "data": {"dataset": dataset, "sha256": data_sha256},
        "runtime": {"scikit-learn": sklearn.__version__},
    }  # fmt: skip
    return AnomalyModel(forest, calibrator, metadata)
