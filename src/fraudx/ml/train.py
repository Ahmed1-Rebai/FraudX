"""Offline training of the supervised fraud model (LightGBM).

Protocol (ADR-016):

* **Time-based split** (70 % / 15 % / 15 % by event time). The model never
  sees the future, as in production.
* Candidates: unweighted vs ``scale_pos_weight = sqrt(neg/pos)``. Each is
  early-stopped on validation PR-AUC, and the best validation PR-AUC wins.
* Decision threshold chosen on validation (max F1), reported on test.
* Metrics: PR-AUC (primary), ROC-AUC, precision/recall/F1 at the threshold,
  Brier score (calibration: the risk engine consumes the probability),
  recall per fraud scenario, false-positive rate per legit scenario.
* PR-AUC and precision are also reported at the dataset's overall fraud rate
  (``at_reference_prevalence``), so sets with different fraud rates compare.
* Ablations: retrain without each feature group (e.g. balance leakage check).
"""

from __future__ import annotations

import json
import math
import platform
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    precision_recall_curve,
    roc_auc_score,
)

from fraudx.core.schemas import FEATURE_VERSION
from fraudx.ml.dataset import FeatureMatrix
from fraudx.ml.vectorize import CATEGORICAL, FEATURE_GROUPS

MODEL_NAME = "fraud_lgbm"
SEED = 42
BASE_PARAMS: dict[str, Any] = {
    "objective": "binary",
    "metric": "average_precision",
    "learning_rate": 0.05,
    "num_leaves": 31,
    "min_child_samples": 20,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "lambda_l2": 1.0,
    "seed": SEED,
    "deterministic": True,
    "force_col_wise": True,
    "num_threads": 4,
    "verbose": -1,
}
MAX_ROUNDS = 2000
EARLY_STOPPING = 100


@dataclass(frozen=True)
class Split:
    train: np.ndarray
    valid: np.ndarray
    test: np.ndarray


def time_split(event_ms: np.ndarray, valid_frac: float = 0.15, test_frac: float = 0.15) -> Split:
    order = np.argsort(event_ms, kind="stable")
    n = len(order)
    n_test, n_valid = int(n * test_frac), int(n * valid_frac)
    n_train = n - n_valid - n_test
    return Split(order[:n_train], order[n_train : n_train + n_valid], order[n_train + n_valid :])


def best_f1_threshold(y: np.ndarray, p: np.ndarray) -> float:
    precision, recall, thresholds = precision_recall_curve(y, p)
    f1 = 2 * precision * recall / np.maximum(precision + recall, 1e-12)
    return float(thresholds[int(np.argmax(f1[:-1]))])


def prevalence_weights(y: np.ndarray, prevalence: float) -> np.ndarray:
    """Sample weights that give ``y`` the positive rate ``prevalence``.

    PR-AUC and precision depend on the base rate, so sets whose fraud rates
    differ (PaySim's test period has 6x the validation rate) are compared at
    one reference rate. Positives are reweighted; ranking and recall are unchanged.
    """
    pos = float(y.sum())
    neg = len(y) - pos
    return np.where(y == 1, prevalence * neg / ((1 - prevalence) * pos), 1.0)


def evaluate(
    y: np.ndarray,
    p: np.ndarray,
    threshold: float,
    scenario: np.ndarray | None = None,
    reference_prevalence: float | None = None,
) -> dict[str, Any]:
    pred = p >= threshold
    tp = int(np.sum(pred & (y == 1)))
    fp = int(np.sum(pred & (y == 0)))
    fn = int(np.sum(~pred & (y == 1)))
    tn = int(np.sum(~pred & (y == 0)))
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    metrics: dict[str, Any] = {
        "n": len(y),
        "positives": int(y.sum()),
        "pr_auc": round(float(average_precision_score(y, p)), 4),
        "roc_auc": round(float(roc_auc_score(y, p)), 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(2 * precision * recall / (precision + recall), 4) if tp else 0.0,
        "brier": round(float(brier_score_loss(y, p)), 6),
        "confusion": {"tp": tp, "fp": fp, "fn": fn, "tn": tn},
    }
    if reference_prevalence is not None:
        w = prevalence_weights(y, reference_prevalence)
        w_pos = float(w[y == 1][0])
        metrics["at_reference_prevalence"] = {
            "prevalence": round(reference_prevalence, 6),
            "pr_auc": round(float(average_precision_score(y, p, sample_weight=w)), 4),
            "precision": round(w_pos * tp / (w_pos * tp + fp), 4) if tp + fp else 0.0,
        }
    if scenario is not None:
        per: dict[str, dict[str, Any]] = {}
        for name in sorted(set(scenario.tolist())):
            mask = scenario == name
            is_fraud = bool(y[mask].max())
            flagged = int(pred[mask].sum())
            per[name] = {
                "n": int(mask.sum()),
                ("recall" if is_fraud else "false_positive_rate"): round(flagged / mask.sum(), 4),
            }
        metrics["by_scenario"] = per
    return metrics


def _fit(
    X: np.ndarray, y: np.ndarray, split: Split, names: list[str], params: dict[str, Any]
) -> lgb.Booster:
    categorical = [c for c in CATEGORICAL if c in names]
    train = lgb.Dataset(X[split.train], y[split.train], feature_name=names,
                        categorical_feature=categorical, free_raw_data=False)  # fmt: skip
    valid = lgb.Dataset(X[split.valid], y[split.valid], reference=train)
    return lgb.train(
        params,
        train,
        num_boost_round=MAX_ROUNDS,
        valid_sets=[valid],
        callbacks=[lgb.early_stopping(EARLY_STOPPING, verbose=False)],
    )


@dataclass
class TrainingResult:
    booster: lgb.Booster
    params: dict[str, Any]
    threshold: float
    metrics: dict[str, Any]
    candidates: list[dict[str, Any]] = field(default_factory=list)
    ablations: dict[str, dict[str, float]] = field(default_factory=dict)
    importance: list[tuple[str, float]] = field(default_factory=list)


def train(data: FeatureMatrix, holdout: FeatureMatrix | None = None) -> TrainingResult:
    X, y, names = data.X, data.y, list(data.feature_names)
    split = time_split(data.event_ms)
    if y[split.valid].sum() == 0 or y[split.test].sum() == 0:
        raise ValueError("validation/test split has no fraud; use a larger dataset")

    neg, pos = int((y[split.train] == 0).sum()), int(y[split.train].sum())
    candidates = []
    best: tuple[float, lgb.Booster, dict[str, Any]] | None = None
    for label, weight in (("unweighted", 1.0), ("sqrt_balanced", math.sqrt(neg / pos))):
        params = {**BASE_PARAMS, "scale_pos_weight": round(weight, 3)}
        booster = _fit(X, y, split, names, params)
        valid_ap = float(average_precision_score(y[split.valid], booster.predict(X[split.valid])))
        candidates.append({"name": label, "scale_pos_weight": params["scale_pos_weight"],
                           "best_iteration": booster.best_iteration,
                           "valid_pr_auc": round(valid_ap, 4)})  # fmt: skip
        if best is None or valid_ap > best[0]:
            best = (valid_ap, booster, params)
    assert best is not None
    _, booster, params = best

    threshold = best_f1_threshold(y[split.valid], booster.predict(X[split.valid]))
    reference = float(y.mean())  # the dataset's fraud rate
    metrics = {
        "split": {k: len(getattr(split, k)) for k in ("train", "valid", "test")},
        "valid": evaluate(
            y[split.valid], booster.predict(X[split.valid]), threshold,
            reference_prevalence=reference,
        ),
        "test": evaluate(
            y[split.test], booster.predict(X[split.test]), threshold, data.scenario[split.test],
            reference_prevalence=reference,
        ),
    }  # fmt: skip
    if holdout is not None:
        metrics["holdout"] = evaluate(
            holdout.y, booster.predict(holdout.X), threshold, holdout.scenario, reference
        )

    ablations = {}
    for group, columns in FEATURE_GROUPS.items():
        keep = [i for i, n in enumerate(names) if n not in columns]
        reduced = _fit(X[:, keep], y, split, [names[i] for i in keep], params)
        p = reduced.predict(X[split.test][:, keep])
        ablations[f"without_{group}"] = {
            "test_pr_auc": round(float(average_precision_score(y[split.test], p)), 4),
            "test_roc_auc": round(float(roc_auc_score(y[split.test], p)), 4),
        }

    gains = booster.feature_importance(importance_type="gain")
    total = float(gains.sum()) or 1.0
    importance = sorted(
        ((n, round(float(g) / total, 4)) for n, g in zip(names, gains, strict=True)),
        key=lambda item: -item[1],
    )
    return TrainingResult(booster, params, threshold, metrics, candidates, ablations, importance)


def save(
    result: TrainingResult, models_dir: Path, data_sha256: str, dataset: str
) -> tuple[str, Path, dict[str, Any]]:
    """Write ``model.txt`` + ``metadata.json`` under ``<models_dir>/<name>/<version>/``."""
    trained_at = datetime.now(UTC)
    version = f"{trained_at:%Y%m%d%H%M%S}-{data_sha256[:8]}"
    out = models_dir / MODEL_NAME / version
    out.mkdir(parents=True, exist_ok=False)
    result.booster.save_model(out / "model.txt", num_iteration=result.booster.best_iteration)
    metadata = {
        "name": MODEL_NAME,
        "version": version,
        "trained_at": trained_at.isoformat(),
        "feature_version": FEATURE_VERSION,
        "feature_names": list(result.booster.feature_name()),
        "threshold": round(result.threshold, 6),
        "params": result.params,
        "best_iteration": result.booster.best_iteration,
        "metrics": result.metrics,
        "candidates": result.candidates,
        "ablations": result.ablations,
        "importance": result.importance,
        "data": {"dataset": dataset, "sha256": data_sha256},
        "runtime": {"lightgbm": lgb.__version__, "python": platform.python_version()},
    }
    (out / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return version, out, metadata
