"""IBM AML (HI-Small) pipeline: profile → clean → canonicalize.

Source: Altman et al., "Realistic Synthetic Financial Transactions for
Anti-Money Laundering Models", NeurIPS 2023 Datasets & Benchmarks. Kaggle
``ealtman2019/ibm-transactions-for-anti-money-laundering-aml``, licence
CDLA-Sharing-1.0. A simulated world of individuals, companies and banks,
calibrated to real data, with labelled laundering patterns (fan-in, fan-out,
cycles, scatter-gather…).
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from datetime import UTC
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

import polars as pl

from fraudx.core.schemas import Location, PaymentMethod, TransactionEvent
from fraudx.datasets.base import CleaningReport, frame_records
from fraudx.datasets.info import DatasetInfo

INFO = DatasetInfo(
    name="ibm_aml",
    title="IBM Transactions for Anti-Money Laundering (HI-Small)",
    source_url="https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml",
    kaggle_ref="ealtman2019/ibm-transactions-for-anti-money-laundering-aml",
    raw_files=("HI-Small_Trans.csv", "HI-Small_accounts.csv", "HI-Small_Patterns.txt"),
    license="CDLA-Sharing-1.0",
    citation=(
        "E. Altman, J. Blanuša, L. von Niederhäusern, B. Egressy, A. Anghel, "
        "K. Atasu. Realistic Synthetic Financial Transactions for Anti-Money "
        "Laundering Models. NeurIPS 2023 Datasets and Benchmarks Track."
    ),
    mapping=(
        ("(raw row number)", "transaction_id", "`AML-<row>`, traceable to the raw file line"),
        ("Timestamp", "event_time", "minute resolution, interpreted as UTC"),
        (
            "From Bank + Account",
            "source_account",
            "`<bank>:<account>`, bank id without leading zeros",
        ),
        ("To Bank + Account", "destination_account", "same normalisation"),
        (
            "accounts.Entity ID (of sender)",
            "customer_id",
            "`E-<entity>`: the account owner (one entity can own several accounts)",
        ),
        (
            "Amount Paid x FX(Payment Currency→USD)",
            "amount, currency=USD",
            "rates derived from the data's own cross-currency transfers (median)",
        ),
        (
            "Amount Paid, Payment Currency",
            "original_amount, original_currency",
            "exact original value; Bitcoin → `XBT`",
        ),
        ("Payment Format", "payment_method", "ACH, WIRE, CHEQUE, CREDIT_CARD, CASH, BITCOIN"),
        ("-", "transaction_type", "TRANSFER (every row moves money between two accounts)"),
        (
            "accounts.Bank Name (sender)",
            "location.country",
            "only when the bank name states a country (`Germany Bank #…`); else empty",
        ),
        ("Is Laundering", "is_fraud_label", "laundering is the positive class"),
        (
            "Patterns file",
            "scenario",
            "pattern type (`fan_out`, `cycle`…), `laundering_unattributed` or `normal`",
        ),
        ("Amount Received, Receiving Currency", "-", "dropped after FX derivation"),
    ),
    limitations=(
        "Synthetic (calibrated simulator), not real bank data.",
        "Laundering is rare (≈0.1 %) and most of it is ACH, so `payment_method` is a strong "
        "and partly simulator-specific signal (see the `channel` ablation).",
        "No device or IP data; location is known only for banks whose name states a country.",
        "Timestamps have minute resolution.",
        "Laundering *patterns* are multi-transaction graph structures; per-transaction "
        "features can only partly capture them. This is the case for the graph detector.",
    ),
)

COLUMNS = (
    "timestamp",
    "from_bank",
    "from_account",
    "to_bank",
    "to_account",
    "amount_received",
    "receiving_currency",
    "amount_paid",
    "payment_currency",
    "payment_format",
    "is_laundering",
)
LABEL = "label"  # typed copy of the raw ``is_laundering`` column
CURRENCY_ISO = {
    "US Dollar": "USD",
    "Euro": "EUR",
    "Swiss Franc": "CHF",
    "Yuan": "CNY",
    "Shekel": "ILS",
    "Rupee": "INR",
    "UK Pound": "GBP",
    "Yen": "JPY",
    "Ruble": "RUB",
    "Bitcoin": "XBT",
    "Canadian Dollar": "CAD",
    "Australian Dollar": "AUD",
    "Mexican Peso": "MXN",
    "Saudi Riyal": "SAR",
    "Brazil Real": "BRL",
}
PAYMENT_METHODS = {
    "ACH": PaymentMethod.ACH,
    "Wire": PaymentMethod.WIRE,
    "Cheque": PaymentMethod.CHEQUE,
    "Credit Card": PaymentMethod.CREDIT_CARD,
    "Cash": PaymentMethod.CASH,
    "Bitcoin": PaymentMethod.BITCOIN,
    "Reinvestment": PaymentMethod.REINVESTMENT,
}
COUNTRY_ISO = {
    "Australia": "AU",
    "Austria": "AT",
    "Belgium": "BE",
    "Brazil": "BR",
    "Canada": "CA",
    "China": "CN",
    "Croatia": "HR",
    "Cyprus": "CY",
    "Estonia": "EE",
    "Finland": "FI",
    "France": "FR",
    "Germany": "DE",
    "Greece": "GR",
    "India": "IN",
    "Ireland": "IE",
    "Israel": "IL",
    "Italy": "IT",
    "Japan": "JP",
    "Latvia": "LV",
    "Lithuania": "LT",
    "Luxembourg": "LU",
    "Malta": "MT",
    "Mexico": "MX",
    "Netherlands": "NL",
    "Portugal": "PT",
    "Russia": "RU",
    "Saudi Arabia": "SA",
    "Slovakia": "SK",
    "Slovenia": "SI",
    "Spain": "ES",
    "Switzerland": "CH",
    "UK": "GB",
}
_PATTERN_HEADER = re.compile(r"^BEGIN LAUNDERING ATTEMPT - ([A-Z-]+)")


def _bank(column: str) -> pl.Expr:
    """Bank ids appear as '010' in transactions and '10' in accounts: strip zeros."""
    return pl.col(column).str.strip_chars_start("0").replace("", "0")


def load_raw(raw_dir: Path) -> pl.DataFrame:
    path = raw_dir / INFO.raw_files[0]
    header = path.open(encoding="utf-8").readline().strip().split(",")
    if len(header) != len(COLUMNS) or header[0] != "Timestamp":
        raise ValueError(f"{path}: unexpected header {header}")
    # Read everything as text: bank ids have significant leading zeros, and raw
    # strings are needed to match rows against the patterns file exactly.
    return pl.read_csv(path, new_columns=list(COLUMNS), infer_schema_length=0).with_row_index(
        "raw_row", offset=1
    )


def load_accounts(raw_dir: Path) -> pl.DataFrame:
    acc = pl.read_csv(raw_dir / INFO.raw_files[1], infer_schema_length=0)
    country = (
        acc["Bank Name"].str.replace(r" Bank #\d+$", "").replace_strict(COUNTRY_ISO, default=None)
    )
    return acc.select(
        _bank("Bank ID").alias("bank"),
        pl.col("Account Number").alias("account"),
        pl.col("Entity ID").alias("entity_id"),
        pl.col("Entity Name").str.replace(r" #\d+$", "").alias("entity_type"),
        country.alias("bank_country"),
    )


def load_patterns(raw_dir: Path) -> pl.DataFrame:
    """Rows of the patterns file with their laundering pattern type."""
    rows, kind, attempt = [], None, 0
    for line in (raw_dir / INFO.raw_files[2]).read_text(encoding="utf-8").splitlines():
        if match := _PATTERN_HEADER.match(line):
            kind, attempt = match.group(1).lower().replace("-", "_"), attempt + 1
        elif line.startswith("END LAUNDERING"):
            kind = None
        elif kind and line.strip():
            rows.append([*line.split(","), kind, attempt])
    return pl.DataFrame(rows, schema=[*COLUMNS, "pattern", "attempt"], orient="row").with_columns(
        pl.col("attempt").cast(pl.Int32)
    )


def typed(df: pl.DataFrame) -> pl.DataFrame:
    """Add typed columns next to the raw text ones (raw text stays untouched: it is
    the join key with the patterns file and the exact original amount)."""
    return df.with_columns(
        pl.col("timestamp")
        .str.strptime(pl.Datetime("us", "UTC"), "%Y/%m/%d %H:%M", strict=False)
        .alias("ts"),
        pl.col("amount_paid").cast(pl.Float64, strict=False).alias("paid"),
        pl.col("amount_received").cast(pl.Float64, strict=False).alias("received"),
        pl.col("is_laundering").cast(pl.Int8, strict=False).alias(LABEL),
        _bank("from_bank").alias("from_bank_n"),
        _bank("to_bank").alias("to_bank_n"),
    )


def derive_fx(df: pl.DataFrame) -> pl.DataFrame:
    """USD value of one unit of each currency, from the data's own conversions.

    Every currency is exchanged directly with USD in the data; the rate is the
    median of ``received / paid`` over those transfers (spread p95/p5 is
    ~1.0001 except Bitcoin, whose tiny amounts are rounded).
    """
    pairs = df.filter(pl.col("receiving_currency") != pl.col("payment_currency")).with_columns(
        (pl.col("received") / pl.col("paid")).alias("ratio")
    )
    usd_paid = (
        pairs.filter(pl.col("payment_currency") == "US Dollar")
        .group_by(pl.col("receiving_currency").alias("currency"))
        .agg((1 / pl.col("ratio").median()).alias("usd_per_unit"), pl.len().alias("n"))
    )
    rates = pl.concat(
        [
            usd_paid,
            pl.DataFrame(
                {"currency": ["US Dollar"], "usd_per_unit": [1.0], "n": [0]},
                schema={"currency": pl.String, "usd_per_unit": pl.Float64, "n": pl.UInt32},
            ),
        ]
    )
    missing = set(df["payment_currency"].unique()) - set(rates["currency"])
    if missing:
        raise ValueError(f"no USD conversion observed for {sorted(missing)}")
    return rates.sort("currency")


def profile(df: pl.DataFrame, accounts: pl.DataFrame, patterns: pl.DataFrame) -> dict[str, Any]:
    t = typed(df)
    self_transfer = (t["from_bank_n"] == t["to_bank_n"]) & (t["from_account"] == t["to_account"])
    daily = (
        t.group_by(pl.col("ts").dt.date().alias("day"))
        .agg(pl.len().alias("rows"), pl.col(LABEL).sum().alias("laundering"))
        .sort("day")
    )
    senders = t.select(
        pl.col("from_bank_n").alias("bank"), pl.col("from_account").alias("account")
    ).unique()
    known = senders.join(accounts, on=["bank", "account"], how="semi").height
    return {
        "rows": t.height,
        "null_cells": int(df.null_count().sum_horizontal().item()),
        "unparseable_timestamps": int(t["ts"].null_count()),
        "duplicate_rows": int(df.drop("raw_row").is_duplicated().sum()),
        "time_range": [str(t["ts"].min()), str(t["ts"].max())],
        "laundering": int(t[LABEL].sum()),
        "laundering_rate": round(t[LABEL].mean(), 6),
        "self_transfers": int(self_transfer.sum()),
        "self_transfers_laundering": int(t.filter(self_transfer)[LABEL].sum()),
        "cross_currency": int((t["receiving_currency"] != t["payment_currency"]).sum()),
        "non_positive_amounts": int((t["paid"] <= 0).sum()),
        "amount_paid_max": t["paid"].max(),
        "by_payment_format": frame_records(
            t.group_by("payment_format")
            .agg(pl.len().alias("rows"), pl.col(LABEL).sum().alias("laundering"))
            .sort("rows", descending=True)
        ),
        "currencies": int(t["payment_currency"].n_unique()),
        "daily": frame_records(daily),
        "accounts": {
            "rows": accounts.height,
            "entities": int(accounts["entity_id"].n_unique()),
            "entities_with_several_accounts": int(
                (accounts.group_by("entity_id").len()["len"] > 1).sum()
            ),
            "senders": senders.height,
            "senders_with_known_owner": known,
            "entity_types": frame_records(
                accounts.group_by("entity_type").len().sort("len", descending=True)
            ),
        },
        "patterns": {
            "attempts": int(patterns["attempt"].n_unique()),
            "rows": patterns.height,
            "by_type": frame_records(
                patterns.group_by("pattern")
                .agg(pl.col("attempt").n_unique().alias("attempts"), pl.len().alias("rows"))
                .sort("pattern")
            ),
        },
    }


def clean(
    df: pl.DataFrame, accounts: pl.DataFrame, patterns: pl.DataFrame
) -> tuple[pl.DataFrame, CleaningReport, pl.DataFrame]:
    """Returns cleaned rows, the cleaning report and the FX table used."""
    report = CleaningReport(dataset=INFO.name, label_column=LABEL)
    t = typed(df)
    report.start(t.with_columns(pl.col(LABEL).fill_null(0)))

    t = report.drop_rows(
        t,
        "invalid_values",
        pl.any_horizontal(pl.col("ts", "paid", "received", LABEL).is_null())
        | ~pl.col("payment_currency").is_in(list(CURRENCY_ISO))
        | ~pl.col("payment_format").is_in(list(PAYMENT_METHODS)),
        "Unparseable timestamps/amounts/labels or unknown currency/format can't be mapped "
        "to the event contract.",
    )
    key = pl.struct(*COLUMNS)
    t = report.drop_rows(
        t,
        "exact_duplicates",
        key.is_duplicated() & ~key.is_first_distinct(),
        "Identical rows (same minute, accounts, amounts, format, label) are load artefacts; "
        "keep the first.",
    )
    # FX first: most currency conversions in the data are self-transfers
    # (an account converting its own money), dropped by the next rule.
    fx = derive_fx(t)
    t = t.join(
        fx.select("currency", "usd_per_unit"),
        left_on="payment_currency",
        right_on="currency",
        how="left",
    )
    t = t.with_columns((pl.col("paid") * pl.col("usd_per_unit")).alias("amount_usd"))
    t = report.transform(
        t,
        "fx_to_usd",
        pl.col("payment_currency") != "US Dollar",
        "Amounts in 15 currencies are not comparable; features (customer mean/std, window "
        "sums) need one unit. Converted to USD with rates derived from the data itself; "
        "the original amount and currency are kept on the event.",
        t,
    )
    t = report.drop_rows(
        t,
        "self_transfers",
        (pl.col("from_bank_n") == pl.col("to_bank_n"))
        & (pl.col("from_account") == pl.col("to_account")),
        "Money moved from an account to itself ('Reinvestment' and currency conversions: "
        "96 % of USD conversions are self-transfers) is not a payment between parties, adds "
        "self-loops to the graph, and is rejected by the platform's validation "
        "(source == destination). FX rates are derived before this rule.",
    )
    volume = t.group_by(pl.col("ts").dt.date().alias("day")).len()
    sparse_days = volume.filter(pl.col("len") < 0.01 * volume["len"].median())["day"]
    t = report.drop_rows(
        t,
        "simulation_tail",
        pl.col("ts").dt.date().is_in(sparse_days.implode()),
        "After the simulated period ends, the generator keeps emitting only the remaining "
        "steps of laundering patterns: days with < 1 % of median volume are mostly "
        "laundering. Kept, they would dominate a time-based test set.",
    )
    t = report.drop_rows(
        t,
        "non_positive_amount",
        pl.col("paid") <= 0,
        "A payment of zero or less is not a money movement.",
    )

    t = report.drop_rows(
        t,
        "below_one_cent",
        pl.col("amount_usd") < 0.005,
        "Amounts under half a cent round to 0.00 USD (mostly tiny Bitcoin transfers) and "
        "become zero-value payments in a 2-decimal contract.",
    )

    t = t.join(
        accounts.select(
            pl.col("bank").alias("from_bank_n"),
            pl.col("account").alias("from_account"),
            "entity_id",
            "bank_country",
        ),
        on=["from_bank_n", "from_account"],
        how="left",
    )
    report.flag(
        t,
        "unknown_sender_owner",
        pl.col("entity_id").is_null(),
        "Sender account missing from the accounts file: the account itself becomes the "
        "customer id.",
    )
    report.flag(
        t,
        "no_bank_country",
        pl.col("bank_country").is_null(),
        "Kept with empty location: the sender's bank name doesn't state a country (generic "
        "or city names). The country isn't guessed.",
    )

    attributed = patterns.select(*COLUMNS, "pattern").unique(subset=list(COLUMNS), keep="first")
    t = t.join(attributed, on=list(COLUMNS), how="left")
    report.flag(
        t,
        "laundering_without_pattern",
        (pl.col(LABEL) == 1) & pl.col("pattern").is_null(),
        "Laundering rows not listed in the patterns file keep scenario `laundering_unattributed`.",
    )
    report.finish(t)
    report.notes["fx_usd_per_unit"] = {r["currency"]: r["usd_per_unit"] for r in fx.to_dicts()}
    return t, report, fx


def to_events(df: pl.DataFrame) -> Iterator[tuple[TransactionEvent, str]]:
    cent = Decimal("0.01")
    for row in df.sort("ts", "raw_row").iter_rows(named=True):
        entity = row["entity_id"]
        source = f"{row['from_bank_n']}:{row['from_account']}"
        country = row["bank_country"]
        event = TransactionEvent(
            transaction_id=f"AML-{row['raw_row']:09d}",
            event_time=row["ts"].replace(tzinfo=UTC),
            customer_id=f"E-{entity}" if entity else f"ACC-{source}",
            source_account=source,
            destination_account=f"{row['to_bank_n']}:{row['to_account']}",
            amount=Decimal(str(row["amount_usd"])).quantize(cent, rounding=ROUND_HALF_UP),
            currency="USD",
            original_amount=Decimal(row["amount_paid"]),
            original_currency=CURRENCY_ISO[row["payment_currency"]],
            transaction_type="TRANSFER",
            payment_method=PAYMENT_METHODS[row["payment_format"]],
            location=Location(country=country) if country else None,
            is_fraud_label=bool(row[LABEL]),
        )
        scenario = (row["pattern"] or "laundering_unattributed") if row[LABEL] else "normal"
        yield event, scenario
