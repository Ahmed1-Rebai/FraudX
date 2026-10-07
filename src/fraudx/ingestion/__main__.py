"""Dataset command line.

python -m fraudx.ingestion generate [--customers N --days D --seed S --out DIR]
python -m fraudx.ingestion paysim --input data/raw/PS_log.csv [--limit N --out DIR]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

from fraudx.core.logging import configure_logging
from fraudx.ingestion import paysim
from fraudx.ingestion.dataset import write_dataset
from fraudx.ingestion.generator import GeneratorConfig, TransactionGenerator

log = logging.getLogger("fraudx.ingestion")


def _generate(args: argparse.Namespace) -> int:
    overrides = {
        k: v
        for k, v in {
            "seed": args.seed,
            "customers": args.customers,
            "days": args.days,
            "start": args.start,
            "victim_share": args.victim_share,
        }.items()
        if v is not None
    }
    config = GeneratorConfig(**overrides)
    rows = ((g.event, g.scenario) for g in TransactionGenerator(config).generate())
    started = time.perf_counter()
    summary = write_dataset(
        rows, args.out, lambda: {"source": "synthetic", "config": config.model_dump(mode="json")}
    )
    _report(summary, args.out, started)
    return 0


def _paysim(args: argparse.Namespace) -> int:
    if not args.input.exists():
        log.error(
            "PaySim file not found; download it from Kaggle (ealaxi/paysim1) into data/raw/",
            extra={"path": str(args.input)},
        )
        return 2
    report = paysim.LoadReport()
    events = paysim.load(
        args.input, report, start=args.start or paysim.DEFAULT_START, limit=args.limit
    )
    rows = ((e, "fraud" if e.is_fraud_label else "normal") for e in events)
    started = time.perf_counter()
    summary = write_dataset(
        rows,
        args.out,
        lambda: {
            "source": "paysim",
            "input": str(args.input),
            "rows_read": report.rows,
            "invalid_rows": report.invalid,
            "sample_errors": report.sample_errors,
        },
    )
    _report(summary, args.out, started)
    return 0


def _report(summary: dict, out: Path, started: float) -> None:
    log.info(
        "dataset written to %s: %d events, %d fraud (%.3f%%), %d invalid rows, %.1fs",
        out,
        summary["events"],
        summary["fraud_events"],
        summary["fraud_rate"] * 100,
        summary.get("invalid_rows", 0),
        time.perf_counter() - started,
        extra={
            "out": str(out),
            "events": summary["events"],
            "fraud_events": summary["fraud_events"],
            "fraud_rate": summary["fraud_rate"],
            "invalid_rows": summary.get("invalid_rows", 0),
            "seconds": round(time.perf_counter() - started, 2),
        },
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m fraudx.ingestion",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="generate a synthetic enriched dataset")
    gen.add_argument("--out", type=Path, default=Path("data/generated"))
    gen.add_argument("--seed", type=int)
    gen.add_argument("--customers", type=int)
    gen.add_argument("--days", type=int)
    gen.add_argument("--victim-share", type=float, help="share of customers hit by fraud")
    gen.add_argument(
        "--start",
        type=datetime.fromisoformat,
        help="ISO start time with offset, e.g. 2026-01-01T00:00:00+00:00",
    )
    gen.set_defaults(func=_generate)

    ps = sub.add_parser("paysim", help="convert and enrich a PaySim CSV")
    ps.add_argument("--input", type=Path, default=Path("data/raw/PS_log.csv"))
    ps.add_argument("--out", type=Path, default=Path("data/processed/paysim"))
    ps.add_argument("--limit", type=int, help="read at most N rows")
    ps.add_argument("--start", type=datetime.fromisoformat)
    ps.set_defaults(func=_paysim)
    return parser


def main(argv: list[str] | None = None) -> int:
    configure_logging("INFO", "text")
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
