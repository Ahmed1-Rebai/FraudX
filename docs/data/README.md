# FraudX data pipeline

FraudX uses three data sources. Each one proves something different:

| Source | What it is | Used to show | Report |
|---|---|---|---|
| **Synthetic generator** (`make data`) | Our own deterministic simulator with labelled scenarios, incl. hard cases (ADR-013, ADR-023) | The live end-to-end demo, and per-detector contribution (scenarios are known) | `docs/experiments/` |
| **PaySim** (Kaggle `ealaxi/paysim1`, CC BY-SA 4.0) | 6.36 M mobile-money transactions simulated from aggregated **real** operator logs | Supervised model on a recognised benchmark, including leakage ablations | [paysim.md](paysim.md) |
| **IBM AML HI-Small** (Kaggle `ealtman2019/…`, CDLA-Sharing-1.0) | 5.08 M bank transfers from a calibrated simulator, with labelled laundering **patterns** | Graph detection (fan-in/out, cycles…) and multi-currency, multi-account customers | [ibm_aml.md](ibm_aml.md) |

No openly licensed dataset of *real* bank transactions with customer, account
and device identities exists (privacy). The two public datasets are
simulators calibrated on real data; their limitations are stated in each
report.

## The five stages

Every public dataset goes through the same explicit stages. Each stage
writes an artefact you can inspect, and can be re-run on its own.

```text
data/raw/<name>/            data/processed/<name>/
  original CSVs ──► 1 PROFILE ──► profile.json            what the raw data looks like
                ──► 2 CLEAN   ──► cleaning_report.json    every rule: why, rows hit, labels hit
                                  cleaned.parquet
                ──► 3 CANONICALIZE ► enriched_transactions.jsonl   TransactionEvent v1 (validated)
                                  ground_truth.csv, summary.json
                ──► 4 FEATURES ──► features_v1.npz        online feature code, disposable Redis
                ──► 5 TRAIN    ──► models/fraud_lgbm/<version>/ + docs/experiments/*.md
                    REPORT     ──► docs/data/<name>.md    generated from 1–5
```

1. **Profile** measures shape, nulls, duplicates, ranges, label balance,
   time distribution, and the dataset's known quirks (leaks, artefacts).
   The cleaning rules are derived from this evidence, not assumed.
2. **Clean** applies named rules in a fixed order. Each rule has an action:
   - `drop_rows`: invalid, duplicated or artefact rows
   - `drop_column`: leaky or meaningless columns
   - `transform`: e.g. FX normalisation
   - `flag`: a documented issue kept on purpose

   For each rule the report records the rationale and how many rows *and
   how many positive labels* it touched, so you can see whether cleaning
   biases the labels.
3. **Canonicalize** maps cleaned rows onto the platform contract
   (`TransactionEvent`). Every event passes contract validation, ids trace
   back to the raw file line (`PS-<line>`, `AML-<line>`), and nothing is
   invented: fields a source doesn't have stay empty.
4. **Features** replays the canonical events in event-time order through the
   *same* feature code as the live pipeline (Redis Lua scripts) on a
   disposable Redis (`redis-offline`). No training/serving skew. The report
   shows each feature's coverage per dataset.
5. **Train** trains LightGBM with a time-based 70/15/15 split, a threshold
   chosen on validation, and PR-AUC as the primary metric, plus per-scenario
   results and ablations by feature group (`balance`, `time`, `channel`,
   `network`) to expose leakage.

## Commands

```bash
make download-datasets   # Kaggle token in ~/.kaggle/access_token (once; ~1 GB)
make paysim              # all stages for PaySim  (~30 min, mostly feature building)
make ibm-aml             # all stages for IBM AML (~35 min)

# or stage by stage
uv run python -m fraudx.datasets paysim --stages profile
uv run python -m fraudx.datasets paysim --stages clean
uv run python -m fraudx.datasets paysim --stages canonical
make offline-redis && uv run python -m fraudx.ml build-features --dataset data/processed/paysim
uv run python -m fraudx.ml train --dataset data/processed/paysim --register
uv run python -m fraudx.datasets paysim --stages report
```

A canonical dataset can also be streamed through the live platform
(`make replay ARGS="--dataset data/processed/ibm_aml --rate 1000"`). Use a
fresh stack (`make clean && make up`), because datasets share no id space
with the demo data.

## Rules of thumb used in cleaning

- Measure first; every rule cites the number that justified it.
- Prefer dropping an **artefact** (simulation tail, zero-amount "fraud") to
  letting the model learn it, and say how many labels it removed.
- Don't delete suspicious-but-real-looking data; **flag** it and measure its
  influence with an ablation (e.g. PaySim's inflated fraud share in
  low-volume days).
- Keep raw values for lineage (balances, original currency), but exclude
  known leaks from features (PaySim destination balances).
- Never invent attributes a source doesn't have (no fake devices/IPs; no
  guessed bank country).
