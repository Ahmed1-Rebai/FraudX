"""Dataset command line (public datasets: ``python -m fraudx.datasets``).

python -m fraudx.ingestion generate [--customers N --days D --seed S --out DIR]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

from fraudx.core.config import KafkaSettings
from fraudx.core.logging import configure_logging
from fraudx.ingestion.dataset import EVENTS_FILE, read_events, write_dataset
from fraudx.ingestion.generator import GeneratorConfig, TransactionGenerator
from fraudx.ingestion.replay import prepare, replay
from fraudx.messaging.producer import EventProducer

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
            "rings": args.rings,
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


def _replay(args: argparse.Namespace) -> int:
    path = args.dataset / EVENTS_FILE
    if not path.exists():
        log.error("no dataset at %s; run `make data` first", path)
        return 2
    events = prepare(read_events(path), with_labels=args.with_labels, rebase_time=args.rebase_time)
    with EventProducer(KafkaSettings(), "replay") as producer:
        report = replay(events, producer, rate=args.rate, limit=args.limit)
    log.info(
        "replayed %d events in %.1fs (%.0f events/s), %d delivery failures",
        report.sent,
        report.seconds,
        report.rate,
        producer.failed,
    )
    return 1 if producer.failed else 0


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
    gen.add_argument("--rings", type=int, help="number of fraud rings")
    gen.add_argument(
        "--start",
        type=datetime.fromisoformat,
        help="ISO start time with offset, e.g. 2026-01-01T00:00:00+00:00",
    )
    gen.set_defaults(func=_generate)

    rp = sub.add_parser("replay", help="publish a dataset to transactions.raw")
    rp.add_argument("--dataset", type=Path, default=Path("data/generated"))
    rp.add_argument("--rate", type=float, default=200.0, help="events/s, 0 = unthrottled")
    rp.add_argument("--limit", type=int)
    rp.add_argument("--rebase-time", action="store_true", help="shift event times to start now")
    rp.add_argument(
        "--with-labels", action="store_true", help="keep is_fraud_label (evaluation runs only)"
    )
    rp.set_defaults(func=_replay)
    return parser


def main(argv: list[str] | None = None) -> int:
    configure_logging("INFO", "text")
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
