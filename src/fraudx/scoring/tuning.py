"""Risk-engine tuning and evaluation: weights and thresholds chosen on validation.

``python -m fraudx.ml tune-risk --dataset data/training/synthetic --holdout data/generated``

The three calibrated signals (``fraudx.scoring.detectors``) and the rules are
computed on the same rows (transaction and graph feature matrices are
row-aligned). Then, using the **validation** split only:

* weights: grid search (steps of 0.1, summing to 1) for the best PR-AUC of
  the weighted average; ties go to the spec's 0.4 / 0.3 / 0.3;
* HIGH (= alert) threshold: best F1 of the 0-100 score; MEDIUM: best F2
  (recall-weighted: the review queue).

Test and holdout are only evaluated. The report compares each signal alone,
the spec's weights on raw and on calibrated signals, the tuned engine, and a
learned stacking model (logistic regression, reference only), and measures
each signal's contribution by dropping it (the engine reweights, as live).
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score

from fraudx.core.schemas import RiskLevel
from fraudx.ml.dataset import FeatureMatrix, matrix_path
from fraudx.ml.graph_dataset import graph_matrix_path
from fraudx.ml.train import evaluate, time_split
from fraudx.scoring import rules
from fraudx.scoring.detectors import Detector
from fraudx.storage.graph_features import GRAPH_FEATURE_NAMES

SPEC_WEIGHTS = {"ml": 0.4, "anomaly": 0.3, "graph": 0.3}


@dataclass(frozen=True)
class Signals:
    """Calibrated signals, raw scores and rule levels for one set of rows."""

    calibrated: dict[str, np.ndarray]
    raw: dict[str, np.ndarray]
    rule_level: np.ndarray  # 0 none, 1 MEDIUM, 2 HIGH
    y: np.ndarray
    scenario: np.ndarray


def compute(dataset_dir: Path, detectors: dict[str, Detector]) -> tuple[Signals, FeatureMatrix]:
    tx = FeatureMatrix.load(matrix_path(dataset_dir))
    graph = FeatureMatrix.load(graph_matrix_path(dataset_dir), GRAPH_FEATURE_NAMES)
    if not np.array_equal(tx.transaction_id, graph.transaction_id):
        raise ValueError(f"{dataset_dir}: transaction and graph matrices are not row-aligned")
    calibrated, raw = {}, {}
    for signal, det in detectors.items():
        X = tx.X if det.inputs == "transaction" else graph.X
        raw[signal] = det.raw(X)
        calibrated[signal] = det.score(X)
    values = rules.as_values(tx.X)
    level = np.zeros(len(tx.y), dtype=np.int8)
    for _, (rule_level, rule) in rules.RULES.items():
        hit = np.asarray(rule(values), dtype=bool)
        level = np.maximum(level, np.where(hit, 2 if rule_level == RiskLevel.HIGH else 1, 0))
    return Signals(calibrated, raw, level, tx.y, tx.scenario), tx


def subset(s: Signals, idx: np.ndarray) -> Signals:
    return Signals(
        {k: v[idx] for k, v in s.calibrated.items()},
        {k: v[idx] for k, v in s.raw.items()},
        s.rule_level[idx], s.y[idx], s.scenario[idx],
    )  # fmt: skip


def combine(signals: dict[str, np.ndarray], weights: dict[str, float]) -> np.ndarray:
    """The risk engine's weighted average over the available signals, in [0, 1]."""
    used = {k: w for k, w in weights.items() if k in signals and w > 0}
    total = sum(used.values())
    return sum(w * signals[k] for k, w in used.items()) / total


def to_score(combined: np.ndarray, level: np.ndarray, medium: int, high: int) -> np.ndarray:
    """0-100 score with round-half-up and rule floors, as ``scoring.risk.assess``."""
    score = np.floor(combined * 100 + 0.5).astype(int)
    score = np.where(level == 2, np.maximum(score, high), score)
    return np.where(level == 1, np.maximum(score, medium), score)


def _pr_auc(y: np.ndarray, p: np.ndarray) -> float:
    return round(float(average_precision_score(y, p)), 4)


def _best_threshold(y: np.ndarray, score: np.ndarray, beta: float) -> int:
    best, best_t = -1.0, 1
    for t in range(1, 101):
        pred = score >= t
        tp = int((pred & (y == 1)).sum())
        if tp == 0:
            continue
        precision, recall = tp / int(pred.sum()), tp / int(y.sum())
        f = (1 + beta**2) * precision * recall / (beta**2 * precision + recall)
        if f > best:
            best, best_t = f, t
    return best_t


def grid() -> list[dict[str, float]]:
    steps = [i / 10 for i in range(11)]
    return [{"ml": a, "anomaly": b, "graph": round(1 - a - b, 1)}
            for a, b in itertools.product(steps, steps) if a + b <= 1.0 + 1e-9]  # fmt: skip


def tune(
    dataset_dir: Path, detectors: dict[str, Detector], holdout_dir: Path | None = None
) -> dict[str, Any]:
    full, matrix = compute(dataset_dir, detectors)
    split = time_split(matrix.event_ms)
    valid, test = subset(full, split.valid), subset(full, split.test)
    sets = {"valid": valid, "test": test}
    if holdout_dir is not None:
        sets["holdout"] = compute(holdout_dir, detectors)[0]

    def weights_key(w: dict[str, float]) -> tuple[float, float]:
        spec_distance = sum(abs(w[k] - SPEC_WEIGHTS[k]) for k in w)
        return (_pr_auc(valid.y, combine(valid.calibrated, w)), -spec_distance)

    weights = max(grid(), key=weights_key)
    combined_valid = combine(valid.calibrated, weights)
    no_floor = np.zeros_like(valid.rule_level)
    high = _best_threshold(valid.y, to_score(combined_valid, no_floor, 0, 0), beta=1.0)
    medium = _best_threshold(valid.y, to_score(combined_valid, no_floor, 0, 0), beta=2.0)
    medium = medium if medium < high else max(1, high // 2)

    # Comparison of combiners (PR-AUC per set).
    stack_X = {
        name: np.column_stack([np.log(np.clip(s.calibrated[k], 1e-6, 1 - 1e-6) /
                                      (1 - np.clip(s.calibrated[k], 1e-6, 1 - 1e-6)))
                               for k in sorted(s.calibrated)])
        for name, s in sets.items()
    }  # fmt: skip
    stacking = LogisticRegression(max_iter=1000).fit(stack_X["valid"], valid.y)
    combiners: dict[str, dict[str, float]] = {}
    for signal in sorted(full.calibrated):
        combiners[f"{signal} alone"] = {
            n: _pr_auc(s.y, s.calibrated[signal]) for n, s in sets.items()
        }
    combiners["spec weights 0.4/0.3/0.3, raw scores"] = {
        n: _pr_auc(s.y, combine(s.raw, SPEC_WEIGHTS)) for n, s in sets.items()
    }
    combiners["spec weights, calibrated"] = {
        n: _pr_auc(s.y, combine(s.calibrated, SPEC_WEIGHTS)) for n, s in sets.items()
    }
    combiners["tuned weights, calibrated (engine)"] = {
        n: _pr_auc(s.y, combine(s.calibrated, weights)) for n, s in sets.items()
    }
    combiners["stacking (logistic regression, reference)"] = {
        n: _pr_auc(s.y, stacking.predict_proba(stack_X[n])[:, 1]) for n, s in sets.items()
    }
    contribution: dict[str, dict[str, Any]] = {}
    for signal in sorted(full.calibrated):
        if sum(w for k, w in weights.items() if k != signal and k in full.calibrated) == 0:
            contribution[f"without {signal}"] = dict.fromkeys(sets, "n/a (no weighted signal left)")
            continue
        contribution[f"without {signal}"] = {
            n: _pr_auc(
                s.y, combine({k: v for k, v in s.calibrated.items() if k != signal}, weights)
            )
            for n, s in sets.items()
        }

    # The final engine (tuned weights + rule floors) at its thresholds.
    engine: dict[str, Any] = {}
    for name, s in sets.items():
        score = to_score(combine(s.calibrated, weights), s.rule_level, medium, high)
        alert = evaluate(s.y, score / 100, high / 100, s.scenario)
        review = evaluate(s.y, score / 100, medium / 100)
        engine[name] = {
            "alert": alert,
            "review": {k: review[k] for k in ("precision", "recall", "confusion")},
            "alert_rate": round(float((score >= high).mean()), 5),
            "rule_hits": {"HIGH": int((s.rule_level == 2).sum()),
                          "MEDIUM": int((s.rule_level == 1).sum())},
        }  # fmt: skip
    return {
        "tuned_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "dataset": str(dataset_dir),
        "holdout": str(holdout_dir) if holdout_dir else None,
        "models": {k: d.model for k, d in detectors.items()},
        "weights": weights,
        "thresholds": {"medium": medium, "high": high},
        "combiners": combiners,
        "contribution": contribution,
        "engine": engine,
    }


# --- report and config ------------------------------------------------------------


def _row(cells: list[Any]) -> str:
    return "| " + " | ".join(str(c) for c in cells) + " |"


def render(result: dict[str, Any]) -> str:
    sets = list(next(iter(result["combiners"].values())))
    w, t = result["weights"], result["thresholds"]
    lines = [
        "# Risk engine tuning",
        "",
        f"- Tuned: {result['tuned_at']} on `{result['dataset']}` (time split; weights and "
        f"thresholds chosen on validation only); holdout `{result['holdout']}`",
        "- Models: " + ", ".join(f"{k} `{v}`" for k, v in result["models"].items()),
        f"- **Weights:** ml {w['ml']}, anomaly {w['anomaly']}, graph {w['graph']} "
        f"(signals calibrated to P(fraud))",
        f"- **Thresholds:** MEDIUM ≥ {t['medium']} (best F2 on validation), HIGH = alert ≥ "
        f"{t['high']} (best F1); rules floor `takeover_pattern` at HIGH and "
        "`burst_on_shared_device` at MEDIUM",
        "",
        "## Combiners compared (PR-AUC)",
        "",
        _row(["combiner", *sets]),
        _row(["---"] * (len(sets) + 1)),
        *(_row([k, *v.values()]) for k, v in result["combiners"].items()),
        "",
        "## Contribution of each signal (engine PR-AUC without it, weights renormalised)",
        "",
        _row(["variant", *sets]),
        _row(["---"] * (len(sets) + 1)),
        _row(["all signals", *result["combiners"]["tuned weights, calibrated (engine)"].values()]),
        *(_row([k, *v.values()]) for k, v in result["contribution"].items()),
        "",
        "## Engine at its thresholds",
        "",
        _row(["set", "alert precision", "alert recall", "alert F1", "alert rate",
              "review (≥ MEDIUM) precision", "review recall", "rule hits HIGH / MEDIUM"]),
        _row(["---"] * 8),
    ]  # fmt: skip
    for name, e in result["engine"].items():
        a, r = e["alert"], e["review"]
        lines.append(_row([name, a["precision"], a["recall"], a["f1"], f"{e['alert_rate']:.3%}",
                           r["precision"], r["recall"],
                           f"{e['rule_hits']['HIGH']} / {e['rule_hits']['MEDIUM']}"]))  # fmt: skip
    last = list(result["engine"])[-1]
    lines += [
        "",
        f"## Alert recall / false-positive rate by scenario ({last})",
        "",
        _row(["scenario", "n", "recall / FPR"]),
        _row(["---"] * 3),
    ]
    for scenario, s in result["engine"][last]["alert"].get("by_scenario", {}).items():
        value = s.get("recall", s.get("false_positive_rate"))
        kind = "recall" if "recall" in s else "FPR"
        lines.append(_row([scenario, s["n"], f"{kind} {value}"]))
    return "\n".join([*lines, ""])


def config_yaml(result: dict[str, Any], report: Path) -> str:
    w, t = result["weights"], result["thresholds"]
    return f"""# Risk engine parameters. Validated at load time (weights must sum to 1.0).
# Generated by `python -m fraudx.ml tune-risk` ({result["tuned_at"]}): weights and
# thresholds were chosen on the validation split; evidence in {report}.
# Signals are calibrated fraud probabilities (fraudx.scoring.detectors).
# Re-tune after retraining a model; scores are only comparable across runs
# that used the same configuration.
risk:
  weights:
    ml: {w["ml"]}
    anomaly: {w["anomaly"]}
    graph: {w["graph"]}
  thresholds:
    medium: {t["medium"]}   # score >= medium -> MEDIUM (review queue, best F2)
    high: {t["high"]}     # score >= high -> HIGH (best F1)
  alert_threshold: {t["high"]}
"""
