"""Public dataset pipelines on small fixtures where every cleaning rule fires."""

import csv
from decimal import Decimal

import polars as pl
import pytest

from fraudx.core.schemas import PaymentMethod
from fraudx.datasets import ibm_aml, paysim
from fraudx.datasets.__main__ import run

# --- PaySim ------------------------------------------------------------------------


def _ps(step, type_, amount, orig, dest, old_o, new_o, old_d=0.0, new_d=0.0, fraud=0):
    return [step, type_, amount, orig, old_o, new_o, dest, old_d, new_d, fraud, 0]


PAYSIM_ROWS = [
    # day 0: normal traffic + one fraud (transfer draining the account)
    *[_ps(1, "PAYMENT", 10.0, f"C{i}", f"M{i}", 100.0, 90.0) for i in range(40)],
    _ps(2, "TRANSFER", 500.0, "C900", "C901", 500.0, 0.0, fraud=1),
    _ps(2, "TRANSFER", 500.0, "C900", "C901", 500.0, 0.0, fraud=1),  # exact duplicate
    _ps(3, "TRANSFER", 0.0, "C902", "C903", 0.0, 0.0, fraud=1),  # zero amount
    # day 1: low volume, fraud share inflated (kept, flagged)
    _ps(25, "CASH_OUT", 80.0, "C50", "C51", 80.0, 0.0, fraud=1),
    _ps(26, "PAYMENT", 5.0, "C52", "M52", 50.0, 45.0),
    # day 2: fraud only = simulation tail (dropped)
    _ps(49, "CASH_OUT", 70.0, "C60", "C61", 70.0, 0.0, fraud=1),
]


@pytest.fixture
def paysim_raw(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    with (raw / paysim.INFO.raw_files[0]).open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(list(paysim.RAW_SCHEMA))
        writer.writerows(PAYSIM_ROWS)
    return raw


def _rules(report):
    return {r.rule: r for r in report.rules}


def test_paysim_cleaning_rules(paysim_raw):
    df, report = paysim.clean(paysim.load_raw(paysim_raw))
    rules = _rules(report)
    assert (report.rows_in, report.positives_in) == (46, 5)
    assert rules["exact_duplicates"].rows_affected == 1
    assert rules["zero_amount"].positives_affected == 1
    assert rules["simulation_tail"].rows_affected == 1
    assert rules["fraud_rate_inflated_period"].rows_affected == 2  # day 1 kept but flagged
    assert "isFlaggedFraud" not in df.columns
    assert (report.rows_out, report.positives_out) == (43, 2)


def test_paysim_events_are_traceable_and_not_invented(paysim_raw):
    df, _ = paysim.clean(paysim.load_raw(paysim_raw))
    rows = list(paysim.to_events(df))
    events = [e for e, _ in rows]
    assert [e.event_time for e in events] == sorted(e.event_time for e in events)
    fraud = next((e, s) for e, s in rows if e.is_fraud_label)
    assert fraud[1] == "fraud_transfer"
    assert fraud[0].transaction_id == "PS-000000041"  # raw file line
    assert fraud[0].currency == "XXX"
    assert fraud[0].device_id is fraud[0].ip_address is fraud[0].location is None
    assert fraud[0].balances.source_before == Decimal("500.00")


def test_paysim_rejects_unexpected_header(tmp_path):
    (tmp_path / paysim.INFO.raw_files[0]).write_text("a,b\n1,2\n")
    with pytest.raises(ValueError, match="unexpected header"):
        paysim.load_raw(tmp_path)


# --- IBM AML -----------------------------------------------------------------------

HEADER = ("Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,"
          "Amount Paid,Payment Currency,Payment Format,Is Laundering")  # fmt: skip


def _aml(ts, fb, fa, tb, ta, recv, rcur, paid, pcur, fmt, label):
    return f"{ts},{fb},{fa},{tb},{ta},{recv},{rcur},{paid},{pcur},{fmt},{label}"


LAUNDERING = _aml("2022/09/02 10:00", "010", "A1", "020", "B1", "100.00", "US Dollar",
                  "100.00", "US Dollar", "ACH", 1)  # fmt: skip


@pytest.fixture
def aml_raw(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    lines = [HEADER]
    for day in (1, 2):  # two full days of normal traffic
        for i in range(100):
            lines.append(
                _aml(
                    f"2022/09/0{day} 0{i % 10}:{i % 60:02d}",
                    "010",
                    "A1",
                    "020",
                    f"B{i}",
                    "50.00",
                    "US Dollar",
                    "50.00",
                    "US Dollar",
                    "Cheque",
                    0,
                )
            )
    lines += [
        # currency conversions inside one account: self-transfers, but FX evidence
        _aml("2022/09/01 09:00", "010", "A2", "010", "A2", "85.00", "Euro", "100.00",
             "US Dollar", "ACH", 0),
        _aml("2022/09/01 09:01", "010", "A2", "010", "A2", "0.0084", "Bitcoin", "100.00",
             "US Dollar", "ACH", 0),
        # a Euro payment from a German bank (converted to USD)
        _aml("2022/09/01 11:00", "0030", "G1", "020", "B1", "85.00", "Euro", "85.00", "Euro",
             "Wire", 0),
        # a sub-cent Bitcoin payment (0.0000001 BTC ≈ 0.001 USD)
        _aml("2022/09/01 12:00", "010", "A1", "020", "B2", "0.0000001", "Bitcoin",
             "0.0000001", "Bitcoin", "Bitcoin", 0),
        LAUNDERING,
        LAUNDERING,  # exact duplicate
        _aml("2022/09/02 11:00", "010", "A1", "020", "B3", "70.00", "US Dollar", "70.00",
             "US Dollar", "ACH", 1),  # laundering not in the patterns file
        _aml("2022/09/12 10:00", "010", "A1", "020", "B4", "60.00", "US Dollar", "60.00",
             "US Dollar", "ACH", 1),  # simulation tail
    ]  # fmt: skip
    (raw / "HI-Small_Trans.csv").write_text("\n".join(lines) + "\n")
    (raw / "HI-Small_accounts.csv").write_text(
        "Bank Name,Bank ID,Account Number,Entity ID,Entity Name\n"
        "Acme Bank #1,10,A1,E001,Corporation #1\n"
        "Acme Bank #1,10,A2,E001,Corporation #1\n"
        "Germany Bank #3,30,G1,E002,Partnership #2\n"
    )
    (raw / "HI-Small_Patterns.txt").write_text(
        "BEGIN LAUNDERING ATTEMPT - FAN-OUT:  Max 2-degree Fan-Out\n"
        f"{LAUNDERING}\n"
        "END LAUNDERING ATTEMPT - FAN-OUT\n\n"
    )
    return raw


@pytest.fixture
def aml_clean(aml_raw):
    return ibm_aml.clean(
        ibm_aml.load_raw(aml_raw), ibm_aml.load_accounts(aml_raw), ibm_aml.load_patterns(aml_raw)
    )


def test_aml_cleaning_rules(aml_clean):
    df, report, _ = aml_clean
    rules = _rules(report)
    assert rules["exact_duplicates"].rows_affected == 1
    assert rules["self_transfers"].rows_affected == 2
    assert rules["simulation_tail"].positives_affected == 1
    assert rules["below_one_cent"].rows_affected == 1
    assert rules["laundering_without_pattern"].rows_affected == 1
    assert rules["no_bank_country"].rows_affected == df.height - 1  # only G1 is German
    assert report.positives_out == 2


def test_aml_fx_uses_conversions_dropped_as_self_transfers(aml_clean):
    _, report, _ = aml_clean
    rates = report.notes["fx_usd_per_unit"]
    assert rates["Euro"] == pytest.approx(100 / 85)
    assert rates["Bitcoin"] == pytest.approx(100 / 0.0084)
    assert rates["US Dollar"] == 1.0


def test_aml_fx_fails_loudly_without_usd_evidence(aml_raw):
    trans = aml_raw / "HI-Small_Trans.csv"
    trans.write_text("\n".join(
        line for line in trans.read_text().splitlines() if "A2,010,A2" not in line
    ) + "\n")  # fmt: skip
    with pytest.raises(ValueError, match="no USD conversion"):
        ibm_aml.clean(ibm_aml.load_raw(aml_raw), ibm_aml.load_accounts(aml_raw),
                      ibm_aml.load_patterns(aml_raw))  # fmt: skip


def test_aml_events_mapping(aml_clean):
    df, _, _ = aml_clean
    rows = {e.transaction_id: (e, s) for e, s in ibm_aml.to_events(df)}
    euro = next(e for e, _ in rows.values() if e.original_currency == "EUR")
    assert euro.source_account == "30:G1"  # bank id without leading zeros
    assert euro.customer_id == "E-E002"
    assert (euro.amount, euro.currency) == (Decimal("100.00"), "USD")
    assert euro.original_amount == Decimal("85.00")
    assert euro.payment_method is PaymentMethod.WIRE
    assert euro.location.country == "DE"
    laundering = [(e, s) for e, s in rows.values() if e.is_fraud_label]
    assert sorted(s for _, s in laundering) == ["fan_out", "laundering_unattributed"]
    assert all(e.location is None for e, _ in laundering)  # "Acme Bank": country not guessed


def test_pattern_file_parsing(aml_raw):
    patterns = ibm_aml.load_patterns(aml_raw)
    assert patterns.height == 1
    assert patterns["pattern"].to_list() == ["fan_out"]


def test_full_stage_run_writes_artefacts_and_report(aml_raw, tmp_path):
    out, docs = tmp_path / "out", tmp_path / "docs"
    run("ibm_aml", ["profile", "clean", "canonical", "report"], aml_raw, out, docs)
    for name in (
        "raw_files.json",
        "profile.json",
        "cleaning_report.json",
        "cleaned.parquet",
        "enriched_transactions.jsonl",
        "ground_truth.csv",
        "summary.json",
    ):
        assert (out / name).exists(), name  # fmt: skip
    text = (docs / "ibm_aml.md").read_text()
    for heading in ("## 1. Source and licence", "## 4. Cleaning", "## 5. Mapping",
                    "## 6. Feature engineering", "## 8. Limitations"):  # fmt: skip
        assert heading in text
    assert "`self_transfers`" in text and "CDLA-Sharing-1.0" in text
    assert pl.read_parquet(out / "cleaned.parquet").height == 203
