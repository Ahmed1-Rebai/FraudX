"""Markdown experiment report for a trained model."""

from __future__ import annotations

from typing import Any


def _row(cells: list[Any]) -> str:
    return "| " + " | ".join(str(c) for c in cells) + " |"


def _metrics_table(metrics: dict[str, dict[str, Any]]) -> list[str]:
    cols = ["n", "positives", "pr_auc", "roc_auc", "precision", "recall", "f1", "brier"]
    lines = [_row(["set", *cols]), _row(["---"] * (len(cols) + 1))]
    lines += [_row([name, *(m[c] for c in cols)]) for name, m in metrics.items()]
    return lines


def prevalence_table(sets: dict[str, dict[str, Any]]) -> list[str]:
    """PR-AUC and precision as measured and at the dataset's fraud rate."""
    rows = {k: m for k, m in sets.items() if "at_reference_prevalence" in m}
    if not rows:
        return []
    reference = next(iter(rows.values()))["at_reference_prevalence"]["prevalence"]
    lines = [
        f"PR-AUC and precision depend on the fraud rate. Matched = positives reweighted "
        f"to the dataset's overall rate ({reference:.4%}), so the sets compare:",
        "",
        _row(["set", "fraud rate", "PR-AUC", "PR-AUC matched", "precision", "precision matched"]),
        _row(["---"] * 6),
    ]
    for name, m in rows.items():
        matched = m["at_reference_prevalence"]
        rate = f"{m['positives'] / m['n']:.4%}"
        lines.append(_row([name, rate, m["pr_auc"], matched["pr_auc"],
                           m["precision"], matched["precision"]]))  # fmt: skip
    return [*lines, ""]


def _scenario_table(metrics: dict[str, Any]) -> list[str]:
    lines = [_row(["scenario", "n", "recall / false-positive rate"]), _row(["---"] * 3)]
    for name, s in metrics.get("by_scenario", {}).items():
        value = s.get("recall", s.get("false_positive_rate"))
        kind = "recall" if "recall" in s else "FPR"
        lines.append(_row([name, s["n"], f"{kind} {value}"]))
    return lines


def render_anomaly(metadata: dict[str, Any]) -> str:
    """Report for the Isolation Forest detector (``fraudx.ml.anomaly``)."""
    m = metadata["metrics"]
    sets = {k: m[k] for k in ("valid", "test", "holdout") if k in m}
    lines = [
        f"# Experiment: {metadata['name']} {metadata['version']}",
        "",
        f"- Trained: {metadata['trained_at']}",
        f"- Dataset: `{metadata['data']['dataset']}` (sha256 `{metadata['data']['sha256'][:16]}…`)",
        f"- Isolation Forest on {metadata['params']['legit_train_rows']:,} legitimate training "
        f"rows, {len(metadata['feature_names'])} numeric features; split by event time: "
        f"{m['split']}",
        "- Score = percentile among legitimate validation traffic; threshold "
        f"{metadata['threshold']} (the most unusual 1 % of normal traffic). The score is a "
        "percentile, not a probability, so the Brier score does not apply.",
        "",
        "## Metrics",
        "",
        *_metrics_table(sets),
        "",
        *prevalence_table(sets),
        "## By scenario (test)",
        "",
        *_scenario_table(m["test"]),
        "",
    ]
    return "\n".join(lines)


def render(metadata: dict[str, Any]) -> str:
    m = metadata["metrics"]
    n_features = len(metadata["feature_names"])
    lightgbm_version = metadata["runtime"]["lightgbm"]
    sets = {k: m[k] for k in ("valid", "test", "holdout") if k in m}
    lines = [
        f"# Experiment: {metadata['name']} {metadata['version']}",
        "",
        f"- Trained: {metadata['trained_at']}",
        f"- Dataset: `{metadata['data']['dataset']}` (sha256 `{metadata['data']['sha256'][:16]}…`)",
        f"- Feature version: {metadata['feature_version']} ({n_features} features)",
        f"- Split by event time: {m['split']}",
        f"- Decision threshold (max F1 on validation): {metadata['threshold']:.4f}",
        f"- Best iteration: {metadata['best_iteration']}; LightGBM {lightgbm_version}",
        "",
        "## Candidates (selected by validation PR-AUC)",
        "",
        _row(["candidate", "scale_pos_weight", "best_iteration", "valid PR-AUC"]),
        _row(["---"] * 4),
        *(_row([c["name"], c["scale_pos_weight"], c["best_iteration"], c["valid_pr_auc"]])
          for c in metadata["candidates"]),
        "",
        "## Metrics",
        "",
        *_metrics_table(sets),
        "",
        "Confusion at the threshold: "
        + "; ".join(f"{k}: {v['confusion']}" for k, v in sets.items()),
        "",
        *prevalence_table(sets),
        "## By scenario (test)",
        "",
        *_scenario_table(m["test"]),
    ]  # fmt: skip
    lines += [
        "",
        "## Ablations (test)",
        "",
        _row(["variant", "PR-AUC", "ROC-AUC"]),
        _row(["---"] * 3),
        _row(["full model", m["test"]["pr_auc"], m["test"]["roc_auc"]]),
        *(_row([k, v["test_pr_auc"], v["test_roc_auc"]]) for k, v in metadata["ablations"].items()),
        "",
        "## Feature importance (gain share, top 10)",
        "",
        _row(["feature", "gain share"]),
        _row(["---"] * 2),
        *(_row([n, g]) for n, g in metadata["importance"][:10]),
        "",
    ]
    return "\n".join(lines)
