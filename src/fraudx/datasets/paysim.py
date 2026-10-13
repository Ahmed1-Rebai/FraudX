"""PaySim pipeline: profile → clean → canonicalize.

Source: Lopez-Rojas, Elmir & Axelsson, "PaySim: A financial mobile money
simulator for fraud detection", EMSS 2016. Kaggle ``ealaxi/paysim1``,
licence CC BY-SA 4.0. One month of mobile-money transactions simulated
from aggregated real logs of an African operator; 1 step = 1 hour.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl

from fraudx.core.schemas import Balances, TransactionEvent
from fraudx.datasets.base import CleaningReport, frame_records
from fraudx.datasets.info import DatasetInfo

INFO = DatasetInfo(
    name="paysim",
    title="PaySim: synthetic mobile-money transactions",
    source_url="https://www.kaggle.com/datasets/ealaxi/paysim1",
    kaggle_ref="ealaxi/paysim1",
    raw_files=("PS_20174392719_1491204439457_log.csv",),
    license="CC BY-SA 4.0",
    citation=(
        "E. A. Lopez-Rojas, A. Elmir, S. Axelsson. PaySim: A financial mobile money "
        "simulator for fraud detection. 28th European Modeling and Simulation "
        "Symposium (EMSS), Larnaca, Cyprus, 2016."
    ),
    mapping=(
        ("(raw row number)", "transaction_id", "`PS-<row>`, traceable to the raw file line"),
        ("step", "event_time", "2026-01-01T00:00Z + (step-1) h + deterministic offset in the hour"),
        ("nameOrig", "customer_id, source_account", "PaySim has one account per customer"),
        ("nameDest", "destination_account", "`M…` = merchant"),
        ("type", "transaction_type", "same five values"),
        ("amount", "amount", "2 dp; currency `XXX` (ISO 4217: no currency; PaySim has none)"),
        (
            "oldbalanceOrg/newbalanceOrig/oldbalanceDest/newbalanceDest",
            "balances",
            "kept for lineage; destination balances are **not** used by any feature (leak)",
        ),
        ("isFraud", "is_fraud_label; scenario", "scenario = `fraud_<type>` or `normal`"),
        ("isFlaggedFraud", "-", "dropped (see cleaning)"),
        ("-", "device_id, ip_address, location", "absent in PaySim: left empty, not invented"),
    ),
    limitations=(
        "Synthetic (simulated from aggregated real logs), not real customer data.",
        "`nameOrig` is ~99.9 % unique: almost every customer appears once, so per-customer "
        "history/velocity features are near-constant on PaySim.",
        "No device, IP or location: network features are empty on PaySim.",
        "Fraud is injected at a ~constant rate per day while legitimate volume varies "
        "strongly, so fraud *rates* in low-volume periods are a simulator artefact "
        "(see the `time` ablation).",
        "Fraud almost always empties the origin account (`balance_drain_ratio` ≈ 1): "
        "a strong signal that is partly a simulator design choice (see the `balance` ablation).",
    ),
)

RAW_SCHEMA = {
    "step": pl.Int32,
    "type": pl.String,
    "amount": pl.Float64,
    "nameOrig": pl.String,
    "oldbalanceOrg": pl.Float64,
    "newbalanceOrig": pl.Float64,
    "nameDest": pl.String,
    "oldbalanceDest": pl.Float64,
    "newbalanceDest": pl.Float64,
    "isFraud": pl.Int8,
    "isFlaggedFraud": pl.Int8,
}
TYPES = ("PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT")
LABEL = "isFraud"
START = datetime(2026, 1, 1, tzinfo=UTC)
_day = ((pl.col("step") - 1) // 24).alias("day")


def load_raw(raw_dir: Path) -> pl.DataFrame:
    path = raw_dir / INFO.raw_files[0]
    header = path.open(encoding="utf-8").readline().strip().split(",")
    if header != list(RAW_SCHEMA):
        raise ValueError(f"{path}: unexpected header {header}")
    return pl.read_csv(path, schema=RAW_SCHEMA).with_row_index("raw_row", offset=1)


def profile(df: pl.DataFrame) -> dict[str, Any]:
    fraud = df.filter(pl.col(LABEL) == 1)
    debit_types = ("PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT")
    xfer = pl.col("type").is_in(["TRANSFER", "CASH_OUT"])
    dest_zero = (pl.col("oldbalanceDest") == 0) & (pl.col("newbalanceDest") == 0)
    daily = (
        df.group_by(_day)
        .agg(pl.len().alias("rows"), pl.col(LABEL).sum().alias("fraud"))
        .sort("day")
        .with_columns((pl.col("fraud") / pl.col("rows")).round(5).alias("fraud_rate"))
    )
    customers = df.group_by("nameOrig").len()
    return {
        "rows": df.height,
        "columns": list(RAW_SCHEMA),
        "null_cells": int(df.null_count().sum_horizontal().item()),
        "duplicate_rows": int(df.drop("raw_row").is_duplicated().sum()),
        "steps": [int(df["step"].min()), int(df["step"].max())],
        "fraud": int(df[LABEL].sum()),
        "fraud_rate": round(df[LABEL].mean(), 6),
        "by_type": frame_records(
            df.group_by("type")
            .agg(pl.len().alias("rows"), pl.col(LABEL).sum().alias("fraud"))
            .sort("rows", descending=True)
        ),
        "unique_origins": customers.height,
        "origins_with_more_than_one_txn": int((customers["len"] > 1).sum()),
        "unique_destinations": int(df["nameDest"].n_unique()),
        "amount_quantiles": {q: round(df["amount"].quantile(q), 2) for q in (0.5, 0.99)},
        "zero_amount_rows": int((df["amount"] <= 0).sum()),
        "zero_amount_fraud": int(fraud.filter(pl.col("amount") <= 0).height),
        "fraud_amount_at_10m_cap": int((fraud["amount"] == 10_000_000).sum()),
        "is_flagged_fraud": {
            "flagged": int(df["isFlaggedFraud"].sum()),
            "flagged_and_fraud": int(fraud["isFlaggedFraud"].sum()),
        },
        "origin_balance_inconsistent": int(
            df.filter(
                pl.col("type").is_in(debit_types)
                & (
                    (pl.col("oldbalanceOrg") - pl.col("amount") - pl.col("newbalanceOrig")).abs()
                    > 0.01
                )
            ).height
        ),
        "origin_balances_both_zero": int(
            df.filter((pl.col("oldbalanceOrg") == 0) & (pl.col("newbalanceOrig") == 0)).height
        ),
        "leaks": {
            "fraud_amount_equals_origin_balance": round(
                (fraud["amount"] == fraud["oldbalanceOrg"]).mean(), 4
            ),
            "fraud_destination_balances_zero": round(fraud.select(dest_zero.mean()).item(), 4),
            "legit_transfer_destination_balances_zero": round(
                df.filter((pl.col(LABEL) == 0) & xfer & ~pl.col("nameDest").str.starts_with("M"))
                .select(dest_zero.mean())
                .item(),
                4,
            ),
        },
        "daily": frame_records(daily),
    }


def clean(df: pl.DataFrame) -> tuple[pl.DataFrame, CleaningReport]:
    report = CleaningReport(dataset=INFO.name, label_column=LABEL)
    report.start(df)

    df = report.drop_rows(
        df,
        "invalid_values",
        pl.any_horizontal(pl.all().is_null())
        | ~pl.col("type").is_in(TYPES)
        | (pl.col("amount") < 0)
        | (pl.col(LABEL) < 0)
        | (pl.col(LABEL) > 1),
        "Rows that don't match the documented schema (nulls, unknown type, negative "
        "amount, label outside {0,1}) can't be mapped to the event contract.",
    )
    df = report.drop_rows(
        df,
        "exact_duplicates",
        pl.struct(pl.exclude("raw_row")).is_duplicated()
        & ~pl.struct(pl.exclude("raw_row")).is_first_distinct(),
        "An identical row twice is a load artefact, not two transactions (keep the first).",
    )
    df = report.drop_rows(
        df,
        "zero_amount",
        pl.col("amount") <= 0,
        "A transaction that moves no money is not a transaction. In PaySim all of them are "
        "labelled fraud (attempts on already-emptied accounts), so they would teach the model "
        "'amount = 0 → fraud', a simulator artefact.",
    )
    legit_days = df.filter(pl.col(LABEL) == 0).select(_day).unique()
    no_legit = df.select(_day).unique().join(legit_days, on="day", how="anti")["day"]
    df = report.drop_rows(
        df,
        "simulation_tail",
        ((pl.col("step") - 1) // 24).is_in(no_legit.implode()),
        "Days with no legitimate traffic at all are the simulator still emitting fraud "
        "after normal activity stopped (100 % fraud). They would dominate a time-based test "
        "set and make evaluation meaningless.",
    )
    df = report.drop_column(
        df,
        "isFlaggedFraud",
        "The simulator's own rule (transfers > 200k) fired 16 times in 6.3 M rows. It is not "
        "an input a bank would have, and as a label proxy it would leak.",
    )
    debit = pl.col("type").is_in(["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT"])
    report.flag(
        df,
        "origin_balance_inconsistent",
        debit
        & ((pl.col("oldbalanceOrg") - pl.col("amount") - pl.col("newbalanceOrig")).abs() > 0.01),
        "Kept: the simulator often doesn't update origin balances. Only `oldbalanceOrg` feeds "
        "a feature (`balance_drain_ratio`), and only when > 0.",
    )
    report.flag(
        df,
        "destination_balance_not_updated",
        pl.col("type").is_in(["TRANSFER", "CASH_OUT"])
        & ~pl.col("nameDest").str.starts_with("M")
        & (pl.col("oldbalanceDest") == 0)
        & (pl.col("newbalanceDest") == 0),
        "Kept for lineage but excluded from all features: ~50 % of fraud vs ~0.03 % of "
        "legitimate transfers have both destination balances at 0 (simulator leak).",
    )
    overall = df[LABEL].mean()
    daily = df.group_by(_day).agg(pl.col(LABEL).mean().alias("rate"))
    inflated = daily.filter(pl.col("rate") > 3 * overall)["day"]
    report.flag(
        df,
        "fraud_rate_inflated_period",
        ((pl.col("step") - 1) // 24).is_in(inflated.implode()),
        "Kept: days whose fraud share exceeds 3x the overall rate. The simulator injects "
        "fraud at a ~constant count per day while legitimate volume swings (1k-574k rows/day), "
        "so these shares are an artefact, not a pattern to learn. Measured by the `time` "
        "ablation and visible in the daily profile.",
    )
    report.finish(df)
    return df, report


def to_events(df: pl.DataFrame) -> Iterator[tuple[TransactionEvent, str]]:
    """Map cleaned rows to events, in event-time order."""
    df = df.with_columns(
        # Deterministic, well-spread offset inside the hour (PaySim has hour resolution).
        ((pl.col("raw_row").cast(pl.Int64) * 2_654_435_761) % 3600).alias("offset_s")
    ).sort("step", "offset_s", "raw_row")
    for row in df.iter_rows(named=True):
        event = TransactionEvent(
            transaction_id=f"PS-{row['raw_row']:09d}",
            event_time=START + timedelta(hours=row["step"] - 1, seconds=row["offset_s"]),
            customer_id=row["nameOrig"],
            source_account=row["nameOrig"],
            destination_account=row["nameDest"],
            amount=Decimal(str(row["amount"])),
            currency="XXX",
            transaction_type=row["type"],
            balances=Balances(
                source_before=Decimal(str(row["oldbalanceOrg"])),
                source_after=Decimal(str(row["newbalanceOrig"])),
                destination_before=Decimal(str(row["oldbalanceDest"])),
                destination_after=Decimal(str(row["newbalanceDest"])),
            ),
            is_fraud_label=bool(row[LABEL]),
        )
        scenario = f"fraud_{row['type'].lower()}" if row[LABEL] else "normal"
        yield event, scenario
