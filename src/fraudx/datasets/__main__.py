"""Public dataset pipelines.

    python -m fraudx.datasets paysim  [--stages profile,clean,canonical,report]
    python -m fraudx.datasets ibm_aml [--stages ...]

Stages run in order and each reads the previous stage's output from
``data/processed/<name>/``, so any stage can be re-run on its own.
Features and training use the platform code:
``python -m fraudx.ml build-features|train --dataset data/processed/<name>``.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import sys
import time
from pathlib import Path

import polars as pl

from fraudx.core.logging import configure_logging
from fraudx.datasets import ibm_aml, paysim, report
from fraudx.datasets.base import write_json
from fraudx.ingestion.dataset import write_dataset

log = logging.getLogger("fraudx.datasets")
STAGES = ("profile", "clean", "canonical", "report")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _raw_inventory(info, raw_dir: Path, out_dir: Path) -> None:
    files = []
    for name in info.raw_files:
        path = raw_dir / name
        if not path.exists():
            raise FileNotFoundError(f"{path} missing; run `make download-datasets`")
        files.append({"name": name, "bytes": path.stat().st_size, "sha256": _sha256(path)})
    write_json(out_dir / "raw_files.json", {"files": files})


def run(name: str, stages: list[str], raw_dir: Path, out_dir: Path, docs_dir: Path) -> None:
    module = {"paysim": paysim, "ibm_aml": ibm_aml}[name]
    info = module.INFO
    out_dir.mkdir(parents=True, exist_ok=True)

    def timed(stage: str):
        log.info("[%s] stage %s ...", name, stage)
        return time.perf_counter()

    if "profile" in stages or "clean" in stages:
        _raw_inventory(info, raw_dir, out_dir)
        raw = module.load_raw(raw_dir)
        extra = (
            (ibm_aml.load_accounts(raw_dir), ibm_aml.load_patterns(raw_dir))
            if module is ibm_aml
            else ()
        )

    if "profile" in stages:
        t = timed("profile")
        write_json(out_dir / "profile.json", module.profile(raw, *extra))
        log.info("[%s] profile.json written (%.1fs)", name, time.perf_counter() - t)

    if "clean" in stages:
        t = timed("clean")
        result = module.clean(raw, *extra)
        cleaned, cleaning = result[0], result[1]
        cleaned.write_parquet(out_dir / "cleaned.parquet")
        write_json(out_dir / "cleaning_report.json", cleaning.to_dict())
        log.info(
            "[%s] %d -> %d rows, %d -> %d positives (%.1fs)",
            name,
            cleaning.rows_in,
            cleaning.rows_out,
            cleaning.positives_in,
            cleaning.positives_out,
            time.perf_counter() - t,
        )

    if "canonical" in stages:
        t = timed("canonical")
        cleaned = pl.read_parquet(out_dir / "cleaned.parquet")
        summary = write_dataset(
            module.to_events(cleaned), out_dir, lambda: {"source": name, "license": info.license}
        )
        log.info(
            "[%s] %d events, %d positive (%.1fs)",
            name,
            summary["events"],
            summary["fraud_events"],
            time.perf_counter() - t,
        )

    if "report" in stages:
        docs_dir.mkdir(parents=True, exist_ok=True)
        path = docs_dir / f"{name}.md"
        path.write_text(report.render(info, out_dir), encoding="utf-8")
        log.info("[%s] report written to %s", name, path)


def main(argv: list[str] | None = None) -> int:
    configure_logging("INFO", "text")
    parser = argparse.ArgumentParser(
        prog="python -m fraudx.datasets",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("dataset", choices=["paysim", "ibm_aml"])
    parser.add_argument(
        "--stages", default=",".join(STAGES), help=f"comma-separated subset of {','.join(STAGES)}"
    )
    parser.add_argument("--raw-dir", type=Path)
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--docs-dir", type=Path, default=Path("docs/data"))
    args = parser.parse_args(argv)

    stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    if unknown := set(stages) - set(STAGES):
        parser.error(f"unknown stages: {sorted(unknown)}")
    run(
        args.dataset,
        [s for s in STAGES if s in stages],
        args.raw_dir or Path("data/raw") / args.dataset,
        args.out_dir or Path("data/processed") / args.dataset,
        args.docs_dir,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
