"""ML command line.

python -m fraudx.ml build-features --dataset data/training/synthetic [--redis-db 1]
python -m fraudx.ml train --dataset data/training/synthetic [--holdout data/generated]
                          [--register] [--activate]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from fraudx.core.config import MongoSettings, RedisSettings
from fraudx.core.logging import configure_logging
from fraudx.ml import registry, report
from fraudx.ml.dataset import FeatureMatrix, build, file_sha256, matrix_path
from fraudx.ml.train import save, train
from fraudx.storage.clients import mongo_client, redis_client

log = logging.getLogger("fraudx.ml")


def _build(args: argparse.Namespace) -> int:
    live = RedisSettings()
    if args.redis_db == live.db:
        log.error("refusing to use Redis db %d: it holds the live feature state", live.db)
        return 2
    redis = redis_client(live.model_copy(update={"db": args.redis_db}), timeout_s=30)
    try:
        matrix = build(args.dataset, redis, batch_size=args.batch_size)
    finally:
        redis.close()
    out = matrix_path(args.dataset)
    matrix.save(out)
    log.info("wrote %s: %d rows, %d fraud, %d features", out, len(matrix.y),
             int(matrix.y.sum()), matrix.X.shape[1])  # fmt: skip
    return 0


def _train(args: argparse.Namespace) -> int:
    path = matrix_path(args.dataset)
    if not path.exists():
        log.error("%s not found; run build-features first", path)
        return 2
    data = FeatureMatrix.load(path)
    holdout = FeatureMatrix.load(matrix_path(args.holdout)) if args.holdout else None
    result = train(data, holdout)
    version, out, metadata = save(result, args.models_dir, file_sha256(path), str(args.dataset))

    report_path = (
        args.reports_dir / f"{metadata['trained_at'][:10]}-{metadata['name']}-{version}.md"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report.render(metadata), encoding="utf-8")
    test = metadata["metrics"]["test"]
    log.info("model %s saved to %s; test PR-AUC %.4f, precision %.3f, recall %.3f; report %s",
             version, out, test["pr_auc"], test["precision"], test["recall"],
             report_path)  # fmt: skip

    if args.register or args.activate:
        settings = MongoSettings()
        client = mongo_client(settings)
        try:
            doc_id = registry.register(client[settings.db], metadata, str(out), args.activate)
        finally:
            client.close()
        log.info("registered %s%s", doc_id, " (active)" if args.activate else "")
    return 0


def main(argv: list[str] | None = None) -> int:
    configure_logging("INFO", "text")
    parser = argparse.ArgumentParser(
        prog="python -m fraudx.ml",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    b = sub.add_parser("build-features", help="online features for a dataset -> .npz")
    b.add_argument("--dataset", type=Path, default=Path("data/training/synthetic"))
    b.add_argument("--redis-db", type=int, default=1, help="dedicated Redis db (flushed!)")
    b.add_argument("--batch-size", type=int, default=500)
    b.set_defaults(func=_build)

    t = sub.add_parser("train", help="train, evaluate and save a versioned model")
    t.add_argument("--dataset", type=Path, default=Path("data/training/synthetic"))
    t.add_argument("--holdout", type=Path, help="dataset dir evaluated out-of-sample")
    t.add_argument("--models-dir", type=Path, default=Path("models"))
    t.add_argument("--reports-dir", type=Path, default=Path("docs/experiments"))
    t.add_argument("--register", action="store_true", help="record in MongoDB model_registry")
    t.add_argument("--activate", action="store_true", help="register and mark as active")
    t.set_defaults(func=_train)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
