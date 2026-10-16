"""ML command line.

python -m fraudx.ml build-features --dataset data/training/synthetic   (uses redis-offline)
python -m fraudx.ml build-graph-features --dataset data/training/synthetic  (uses neo4j-offline)
python -m fraudx.ml train --dataset data/training/synthetic [--holdout data/generated]
                          [--graph] [--register] [--activate]
python -m fraudx.ml train-anomaly --dataset data/training/synthetic [--holdout data/generated]
                                  [--register] [--activate]
python -m fraudx.ml register models/<name>/<version> [--activate]
python -m fraudx.ml tune-risk --dataset data/training/synthetic --holdout data/generated
                              [--write-config]   (uses the active models)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

from fraudx.core.config import MongoSettings, OfflineNeo4jSettings, OfflineRedisSettings
from fraudx.core.logging import configure_logging
from fraudx.core.schemas import FEATURE_VERSION
from fraudx.ml import anomaly, graph_dataset, registry, report
from fraudx.ml.dataset import FeatureMatrix, build, file_sha256, matrix_path
from fraudx.ml.train import MODEL_NAME, save, train
from fraudx.ml.vectorize import FEATURE_NAMES
from fraudx.storage.clients import mongo_client, neo4j_driver, redis_client
from fraudx.storage.graph_features import GRAPH_FEATURE_NAMES, GRAPH_FEATURE_VERSION

log = logging.getLogger("fraudx.ml")


def _build(args: argparse.Namespace) -> int:
    settings = OfflineRedisSettings().as_redis_settings()
    redis = redis_client(settings, timeout_s=60)
    try:
        redis.ping()
    except Exception:
        log.error(
            "offline Redis not reachable at %s:%d; start it with `make offline-redis`",
            settings.host,
            settings.port,
        )
        return 2
    try:
        matrix = build(args.dataset, redis, batch_size=args.batch_size)
    finally:
        redis.close()
    out = matrix_path(args.dataset)
    matrix.save(out)
    log.info("wrote %s: %d rows, %d fraud, %d features", out, len(matrix.y),
             int(matrix.y.sum()), matrix.X.shape[1])  # fmt: skip
    return 0


def _build_graph(args: argparse.Namespace) -> int:
    settings = OfflineNeo4jSettings().as_neo4j_settings()
    driver = neo4j_driver(settings, timeout_s=30)
    try:
        driver.verify_connectivity()
    except Exception:
        log.error("offline Neo4j not reachable at %s; start it with `make offline-neo4j`",
                  settings.uri)  # fmt: skip
        driver.close()
        return 2
    try:
        matrix = graph_dataset.build(args.dataset, driver)
    finally:
        driver.close()
    out = graph_dataset.graph_matrix_path(args.dataset)
    matrix.save(out)
    log.info("wrote %s: %d rows, %d fraud, %d graph features", out, len(matrix.y),
             int(matrix.y.sum()), matrix.X.shape[1])  # fmt: skip
    return 0


def _train(args: argparse.Namespace) -> int:
    if args.graph:  # graph signal: LightGBM on the graph features only
        path_of, names = graph_dataset.graph_matrix_path, GRAPH_FEATURE_NAMES
        name, feature_version = "graph_lgbm", GRAPH_FEATURE_VERSION
    else:
        path_of, names = matrix_path, FEATURE_NAMES
        name, feature_version = MODEL_NAME, FEATURE_VERSION
    path = path_of(args.dataset)
    if not path.exists():
        log.error("%s not found; run build-features / build-graph-features first", path)
        return 2
    data = FeatureMatrix.load(path, names)
    holdout = FeatureMatrix.load(path_of(args.holdout), names) if args.holdout else None
    result = train(data, holdout)
    version, out, metadata = save(
        result, args.models_dir, file_sha256(path), str(args.dataset), name, feature_version
    )

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
        _record(metadata, out, args.activate)
    return 0


def _train_anomaly(args: argparse.Namespace) -> int:
    path = matrix_path(args.dataset)
    if not path.exists():
        log.error("%s not found; run build-features first", path)
        return 2
    holdout = FeatureMatrix.load(matrix_path(args.holdout)) if args.holdout else None
    model = anomaly.train(FeatureMatrix.load(path), file_sha256(path), str(args.dataset), holdout)
    out = model.save(args.models_dir)
    metadata = model.metadata
    report_path = (
        args.reports_dir
        / f"{metadata['trained_at'][:10]}-{metadata['name']}-{metadata['version']}.md"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report.render_anomaly(metadata), encoding="utf-8")
    test = metadata["metrics"]["test"]
    log.info("anomaly model %s saved to %s; test PR-AUC %.4f, recall %.3f at %.2f; report %s",
             metadata["version"], out, test["pr_auc"], test["recall"], metadata["threshold"],
             report_path)  # fmt: skip
    if args.register or args.activate:
        _record(metadata, out, args.activate)
    return 0


def _tune_risk(args: argparse.Namespace) -> int:
    from fraudx.scoring import tuning
    from fraudx.scoring.detectors import load_active

    settings = MongoSettings()
    client = mongo_client(settings)
    try:
        detectors = load_active(client[settings.db])
    finally:
        client.close()
    if not detectors:
        log.error("no active models in the registry; run `make train` first")
        return 2
    result = tuning.tune(args.dataset, detectors, args.holdout)
    report_path = args.reports_dir / f"{result['tuned_at'][:10]}-risk-engine.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(tuning.render(result), encoding="utf-8")
    log.info("weights %s, thresholds %s; report %s", result["weights"], result["thresholds"],
             report_path)  # fmt: skip
    if args.write_config:
        args.config.write_text(tuning.config_yaml(result, report_path), encoding="utf-8")
        log.info("wrote %s", args.config)
    return 0


def _register(args: argparse.Namespace) -> int:
    path = args.artifact / "metadata.json"
    if not path.exists():
        log.error("%s not found; pass a models/<name>/<version> directory", path)
        return 2
    _record(json.loads(path.read_text(encoding="utf-8")), args.artifact, args.activate)
    return 0


def _record(metadata: dict[str, Any], artifact: Path, activate: bool) -> None:
    """Write the artifact's own metadata to the MongoDB model registry."""
    settings = MongoSettings()
    client = mongo_client(settings)
    try:
        doc_id = registry.register(client[settings.db], metadata, str(artifact), activate)
    finally:
        client.close()
    log.info("registered %s%s", doc_id, " (active)" if activate else "")


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
    b.add_argument("--batch-size", type=int, default=500)
    b.set_defaults(func=_build)

    g = sub.add_parser("build-graph-features", help="point-in-time graph features -> .npz")
    g.add_argument("--dataset", type=Path, default=Path("data/training/synthetic"))
    g.set_defaults(func=_build_graph)

    t = sub.add_parser("train", help="train, evaluate and save a versioned model")
    t.add_argument("--dataset", type=Path, default=Path("data/training/synthetic"))
    t.add_argument("--holdout", type=Path, help="dataset dir evaluated out-of-sample")
    t.add_argument("--models-dir", type=Path, default=Path("models"))
    t.add_argument("--reports-dir", type=Path, default=Path("docs/experiments"))
    t.add_argument("--graph", action="store_true", help="train the graph model (graph_lgbm)")
    t.add_argument("--register", action="store_true", help="record in MongoDB model_registry")
    t.add_argument("--activate", action="store_true", help="register and mark as active")
    t.set_defaults(func=_train)

    a = sub.add_parser("train-anomaly", help="train the Isolation Forest anomaly detector")
    a.add_argument("--dataset", type=Path, default=Path("data/training/synthetic"))
    a.add_argument("--holdout", type=Path, help="dataset dir evaluated out-of-sample")
    a.add_argument("--models-dir", type=Path, default=Path("models"))
    a.add_argument("--reports-dir", type=Path, default=Path("docs/experiments"))
    a.add_argument("--register", action="store_true", help="record in MongoDB model_registry")
    a.add_argument("--activate", action="store_true", help="register and mark as active")
    a.set_defaults(func=_train_anomaly)

    k = sub.add_parser("tune-risk", help="tune and evaluate the risk engine on the active models")
    k.add_argument("--dataset", type=Path, default=Path("data/training/synthetic"))
    k.add_argument("--holdout", type=Path, help="dataset dir evaluated out-of-sample")
    k.add_argument("--reports-dir", type=Path, default=Path("docs/experiments"))
    k.add_argument("--config", type=Path, default=Path("config/risk.yaml"))
    k.add_argument("--write-config", action="store_true", help="write the tuned config file")
    k.set_defaults(func=_tune_risk)

    r = sub.add_parser("register", help="record a saved model in MongoDB model_registry")
    r.add_argument("artifact", type=Path, help="model directory: models/<name>/<version>")
    r.add_argument("--activate", action="store_true", help="also mark it as active")
    r.set_defaults(func=_register)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
