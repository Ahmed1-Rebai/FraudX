"""Shared building blocks for the public-dataset pipelines.

Every dataset goes through the same explicit, auditable stages:

1. **profile** - measure the raw data (shape, nulls, duplicates, ranges,
   label distribution, known quirks) → ``profile.json``.
2. **clean** - apply named cleaning rules; each rule records *why* it exists,
   what it does and how many rows (and how many positive labels) it touched
   → ``cleaned.parquet`` + ``cleaning_report.json``.
3. **canonicalize** - map cleaned rows onto ``TransactionEvent`` (the
   platform contract) → the standard dataset layout used by replay, feature
   building and training (``enriched_transactions.jsonl``,
   ``ground_truth.csv``, ``summary.json``).
4. **features** / 5. **train** - unchanged platform code
   (``python -m fraudx.ml build-features|train``).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

import polars as pl

Action = Literal["drop_rows", "drop_column", "transform", "flag"]


@dataclass(frozen=True, slots=True)
class RuleResult:
    rule: str
    action: Action
    rationale: str
    rows_affected: int
    positives_affected: int


@dataclass
class CleaningReport:
    dataset: str
    label_column: str
    rows_in: int = 0
    positives_in: int = 0
    rows_out: int = 0
    positives_out: int = 0
    rules: list[RuleResult] = field(default_factory=list)
    notes: dict[str, Any] = field(default_factory=dict)

    def start(self, df: pl.DataFrame) -> None:
        self.rows_in = df.height
        self.positives_in = int(df[self.label_column].sum())

    def finish(self, df: pl.DataFrame) -> None:
        self.rows_out = df.height
        self.positives_out = int(df[self.label_column].sum())

    def drop_rows(
        self, df: pl.DataFrame, rule: str, condition: pl.Expr, rationale: str
    ) -> pl.DataFrame:
        """Remove rows matching ``condition`` and record the impact."""
        hit = df.filter(condition)
        self._record(rule, "drop_rows", rationale, hit)
        return df.filter(~condition)

    def transform(
        self, df: pl.DataFrame, rule: str, changed: pl.Expr, rationale: str, result: pl.DataFrame
    ) -> pl.DataFrame:
        """Record a value transformation; ``changed`` marks rows whose value changed."""
        self._record(rule, "transform", rationale, df.filter(changed))
        return result

    def flag(self, df: pl.DataFrame, rule: str, condition: pl.Expr, rationale: str) -> None:
        """Record a data-quality issue that is kept on purpose (documented, not fixed)."""
        self._record(rule, "flag", rationale, df.filter(condition))

    def drop_column(self, df: pl.DataFrame, column: str, rationale: str) -> pl.DataFrame:
        self.rules.append(RuleResult(f"drop_{column}", "drop_column", rationale, 0, 0))
        return df.drop(column)

    def _record(self, rule: str, action: Action, rationale: str, hit: pl.DataFrame) -> None:
        positives = int(hit[self.label_column].sum()) if hit.height else 0
        self.rules.append(RuleResult(rule, action, rationale, hit.height, positives))

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "rules": [asdict(r) for r in self.rules]}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    tmp.replace(path)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def frame_records(df: pl.DataFrame) -> list[dict[str, Any]]:
    """A small DataFrame as JSON-friendly records (for profiles and reports)."""
    return [{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()}
            for row in df.to_dicts()]  # fmt: skip
