"""Storage benchmarks (spec Phase 11): MongoDB vs Cassandra, MongoDB vs Redis.

python -m fraudx.bench [--dataset data/generated] [--events 20000] [--samples 2000]

The same transactions are stored in each database the way that database is
meant to be used, to serve the same two queries (one transaction by id, a
customer's transactions on one day):

* Cassandra: the production schema (``infra/cassandra/schema.cql``) in a
  separate ``fraudx_bench`` keyspace, written by the production
  ``HistoryWriter`` (two denormalised tables) and read by ``HistoryReader``.
* MongoDB: one ``bench_transactions`` collection, ``_id`` = transaction id,
  plus a secondary index on ``(customer_id, event_date, event_time)``.
* Redis (cache comparison): one JSON string per transaction.

Measured: batch write throughput (batches of 500, as the stream writers do),
single-write latency, read latency (sequential, so ops/s = 1 / mean), and
storage size after a flush. Every benchmark namespace is dropped at the end.
Numbers come from one local Docker node and are only comparable with each other.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import platform
import random
import subprocess
import sys
import time
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from itertools import batched, islice
from pathlib import Path
from typing import Any

import numpy as np
from bson import Decimal128
from pymongo import ASCENDING, DESCENDING, ReplaceOne

from fraudx.core.config import CassandraSettings, MongoSettings, RedisSettings
from fraudx.core.logging import configure_logging
from fraudx.core.schemas import TransactionEvent
from fraudx.ingestion.dataset import EVENTS_FILE, read_events
from fraudx.storage import clients
from fraudx.storage.history_writer import HistoryWriter
from fraudx.storage.readers import HistoryReader

log = logging.getLogger("fraudx.bench")

KEYSPACE = "fraudx_bench"
COLLECTION = "bench_transactions"
REDIS_PREFIX = "fx:bench:tx:"
SCHEMA = Path("infra/cassandra/schema.cql")
BATCH = 500
DAY_LIMIT = 50


# --- measurement ----------------------------------------------------------------


def latency(op: Callable[[Any], object], items: Iterable[Any]) -> dict[str, float]:
    """Run ``op`` once per item, sequentially; percentiles in milliseconds."""
    times = []
    for item in items:
        start = time.perf_counter()
        op(item)
        times.append(time.perf_counter() - start)
    ms = np.array(times) * 1000
    return {
        "p50_ms": round(float(np.percentile(ms, 50)), 3),
        "p95_ms": round(float(np.percentile(ms, 95)), 3),
        "p99_ms": round(float(np.percentile(ms, 99)), 3),
        "ops_per_s": round(1000 / float(ms.mean())),
    }


def throughput(write_batch: Callable[[list[Any]], object], items: list[Any]) -> float:
    start = time.perf_counter()
    for chunk in batched(items, BATCH):
        write_batch(list(chunk))
    return round(len(items) / (time.perf_counter() - start))


# --- stores -----------------------------------------------------------------------


def to_document(event: TransactionEvent) -> dict[str, Any]:
    location = event.location
    doc = {
        "_id": event.transaction_id,
        "customer_id": event.customer_id,
        "event_date": datetime.combine(event.event_time.date(), datetime.min.time(), UTC),
        "event_time": event.event_time,
        "source_account": event.source_account,
        "destination_account": event.destination_account,
        "transaction_type": event.transaction_type.value,
        "amount": Decimal128(str(event.amount)),
        "currency": event.currency,
        "device_id": event.device_id,
        "ip_address": str(event.ip_address) if event.ip_address else None,
        "country": location.country if location else None,
        "city": location.city if location else None,
    }
    return {k: v for k, v in doc.items() if v is not None}


def cassandra_schema() -> list[str]:
    """The production DDL, retargeted at the benchmark keyspace."""
    lines = [ln for ln in SCHEMA.read_text().splitlines() if not ln.lstrip().startswith("--")]
    ddl = "\n".join(lines).replace("EXISTS fraudx\n", f"EXISTS {KEYSPACE}\n")
    ddl = ddl.replace("fraudx.", f"{KEYSPACE}.")
    return [s.strip() for s in ddl.split(";") if s.strip()]


def cassandra_size_bytes() -> int | None:
    """Live SSTable bytes of the benchmark keyspace, after a flush (via nodetool)."""
    nodetool = ["docker", "compose", "exec", "-T", "cassandra", "nodetool"]
    try:
        subprocess.run([*nodetool, "flush", KEYSPACE], check=True, capture_output=True)  # noqa: S603
        out = subprocess.run(  # noqa: S603
            [*nodetool, "tablestats", "-F", "json", KEYSPACE],
            check=True, capture_output=True, text=True,
        ).stdout  # fmt: skip
    except (OSError, subprocess.CalledProcessError) as exc:
        log.warning("cassandra size unavailable: %s", exc)
        return None
    tables = json.loads(out)[KEYSPACE]["tables"]
    return sum(int(t["space_used_live"]) for t in tables.values())


def mongo_size_bytes(db: Any, timeout_s: float = 90) -> int:
    """Data + index bytes on disk.

    WiredTiger updates on-disk sizes at a checkpoint (every 60 s by default),
    and forcing one (fsync) needs admin rights, so wait until the size changes.
    """

    def size() -> int:
        stats = db.command("collStats", COLLECTION)
        return int(stats["storageSize"] + stats["totalIndexSize"])

    initial = current = size()
    deadline = time.monotonic() + timeout_s
    while current == initial and time.monotonic() < deadline:
        time.sleep(5)
        current = size()
    return current


# --- benchmark --------------------------------------------------------------------


def run(events: list[TransactionEvent], samples: int, seed: int = 7) -> dict[str, Any]:
    rng = random.Random(seed)  # noqa: S311 - sampling, not security
    single = events[:samples]  # written one by one for latency, then the rest in batches
    rest = events[samples:]
    ids = [e.transaction_id for e in rng.sample(events, samples)]
    days = [(e.customer_id, e.event_time.date()) for e in rng.sample(events, samples)]
    results: dict[str, Any] = {}

    # Cassandra ---------------------------------------------------------------
    settings = CassandraSettings()
    cluster = clients.cassandra_cluster(settings, timeout_s=10)
    session = clients.cassandra_session(cluster)
    try:
        session.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE}")
        for statement in cassandra_schema():
            session.execute(statement)
        writer = HistoryWriter(session, KEYSPACE)
        reader = HistoryReader(session, KEYSPACE)
        results["cassandra"] = {
            "write_single": latency(lambda e: writer.write([e]), single),
            "write_batch_events_per_s": throughput(writer.write, rest),
            "read_by_id": latency(reader.transaction, ids),
            "read_customer_day": latency(lambda d: reader.customer_day(*d, DAY_LIMIT), days),
            "size_bytes": cassandra_size_bytes(),
        }
        session.execute(f"DROP KEYSPACE {KEYSPACE}")
    finally:
        cluster.shutdown()

    # MongoDB -----------------------------------------------------------------
    mongo_settings = MongoSettings()
    mongo = clients.mongo_client(mongo_settings, timeout_ms=10_000)
    db = mongo[mongo_settings.db]
    try:
        coll = db[COLLECTION]
        coll.drop()
        coll.create_index(
            [("customer_id", ASCENDING), ("event_date", ASCENDING), ("event_time", DESCENDING)]
        )

        def upsert(batch: list[TransactionEvent]) -> None:
            docs = [to_document(e) for e in batch]
            coll.bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs],
                            ordered=False)  # fmt: skip

        def customer_day(day: tuple[str, Any]) -> list[dict[str, Any]]:
            start = datetime.combine(day[1], datetime.min.time(), UTC)
            query = {"customer_id": day[0], "event_date": start}
            return list(coll.find(query).sort("event_time", DESCENDING).limit(DAY_LIMIT))

        results["mongodb"] = {
            "write_single": latency(lambda e: upsert([e]), single),
            "write_batch_events_per_s": throughput(upsert, rest),
            "read_by_id": latency(lambda i: coll.find_one({"_id": i}), ids),
            "read_customer_day": latency(customer_day, days),
        }
        results["mongodb"]["size_bytes"] = mongo_size_bytes(db)

        # Redis as a cache in front of MongoDB --------------------------------
        redis = clients.redis_client(RedisSettings())
        try:
            pipe = redis.pipeline(transaction=False)
            for e in events:
                pipe.set(REDIS_PREFIX + e.transaction_id, e.model_dump_json(), ex=3600)
            pipe.execute()
            results["cache"] = {
                "mongodb_read_by_id": results["mongodb"]["read_by_id"],
                "redis_read_by_id": latency(lambda i: json.loads(redis.get(REDIS_PREFIX + i)), ids),
            }
            redis.delete(*(REDIS_PREFIX + e.transaction_id for e in events))
        finally:
            redis.close()
        coll.drop()
    finally:
        mongo.close()
    return results


# --- report -----------------------------------------------------------------------


def _row(cells: list[Any]) -> str:
    return "| " + " | ".join(str(c) for c in cells) + " |"


def _mib(value: int | None) -> str:
    return "n/a" if value is None else f"{value / 2**20:.2f} MiB"


def render(results: dict[str, Any], meta: dict[str, Any]) -> str:
    c, m, cache = results["cassandra"], results["mongodb"], results["cache"]
    lat_cols = ["p50 ms", "p95 ms", "p99 ms", "ops/s"]

    def lat(name: str, store: str, r: dict[str, float]) -> str:
        return _row([name, store, r["p50_ms"], r["p95_ms"], r["p99_ms"], r["ops_per_s"]])

    return "\n".join([
        "# Storage benchmark",
        "",
        f"- Run: {meta['run_at']}; dataset `{meta['dataset']}`, {meta['events']:,} transactions; "
        f"{meta['samples']:,} sequential samples per latency test.",
        f"- Host: {meta['cpus']} CPUs, {meta['platform']}; single-node Docker stores "
        "(Cassandra RF=1, MongoDB standalone), client on the same host.",
        "- Generated by `python -m fraudx.bench` (`make bench`). Local numbers: compare the "
        "stores with each other, not with production figures.",
        "",
        "## MongoDB vs Cassandra (transaction history)",
        "",
        "Each store serves the same two queries with its own data model: Cassandra writes "
        "two denormalised tables per transaction (`transactions_by_id`, "
        "`transactions_by_customer`); MongoDB writes one document plus a secondary index.",
        "Write path per batch: Cassandra gets two prepared INSERTs per transaction, sent "
        "concurrently (64 in flight) by the Python driver; MongoDB gets one unordered "
        "`bulk_write` request.",
        "",
        _row(["metric", "Cassandra", "MongoDB"]),
        _row(["---"] * 3),
        _row(["batch write throughput (events/s, batches of 500)",
              f"{c['write_batch_events_per_s']:,}", f"{m['write_batch_events_per_s']:,}"]),
        _row(["storage on disk after flush/checkpoint (data + indexes)", _mib(c["size_bytes"]),
              _mib(m["size_bytes"])]),
        "",
        _row(["operation", "store", *lat_cols]),
        _row(["---"] * 6),
        lat("single write", "Cassandra", c["write_single"]),
        lat("single write", "MongoDB", m["write_single"]),
        lat("read by id", "Cassandra", c["read_by_id"]),
        lat("read by id", "MongoDB", m["read_by_id"]),
        lat("customer-day read", "Cassandra", c["read_customer_day"]),
        lat("customer-day read", "MongoDB", m["read_customer_day"]),
        "",
        "## MongoDB vs Redis (cache lookup by id)",
        "",
        _row(["operation", "store", *lat_cols]),
        _row(["---"] * 6),
        lat("read by id", "MongoDB", cache["mongodb_read_by_id"]),
        lat("read by id", "Redis", cache["redis_read_by_id"]),
        "",
    ])  # fmt: skip


def main(argv: list[str] | None = None) -> int:
    configure_logging("INFO", "text")
    parser = argparse.ArgumentParser(
        prog="python -m fraudx.bench",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dataset", type=Path, default=Path("data/generated"))
    parser.add_argument("--events", type=int, default=20_000)
    parser.add_argument("--samples", type=int, default=2_000)
    parser.add_argument("--out", type=Path, default=Path("docs/benchmarks/storage.md"))
    args = parser.parse_args(argv)
    if args.samples * 2 > args.events:
        parser.error("--events must be at least twice --samples")

    events = list(islice(read_events(args.dataset / EVENTS_FILE), args.events))
    log.info("benchmarking %d events from %s", len(events), args.dataset)
    results = run(events, args.samples)
    meta = {
        "run_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "dataset": str(args.dataset),
        "events": len(events),
        "samples": args.samples,
        "cpus": os.cpu_count(),
        "platform": platform.platform(terse=True),
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(render(results, meta), encoding="utf-8")
    log.info("wrote %s", args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
