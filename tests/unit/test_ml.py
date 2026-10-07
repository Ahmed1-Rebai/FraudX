import json
import math

import numpy as np
import pytest

from fraudx.core.schemas import TransactionEvent, TransactionFeatures
from fraudx.ml import report
from fraudx.ml.dataset import FeatureMatrix
from fraudx.ml.model import FraudModel, IncompatibleModelError
from fraudx.ml.train import best_f1_threshold, evaluate, save, time_split, train
from fraudx.ml.vectorize import FEATURE_NAMES, TYPE_CODES, vectorize

EVENT = TransactionEvent.model_validate({
    "transaction_id": "TX-1", "event_time": "2026-01-01T02:00:00Z", "customer_id": "C-1",
    "source_account": "A-1", "destination_account": "M-1", "amount": 99,
    "transaction_type": "CASH_OUT",
})  # fmt: skip
FEATURES = TransactionFeatures(
    cust_txn_count_5m=3, cust_txn_count_1h=3, cust_txn_count_24h=4, cust_amount_sum_5m=10,
    cust_amount_sum_1h=10, cust_amount_sum_24h=20, cust_history_count=0, is_new_device=True,
    is_new_ip=False, is_new_country=False, hour_of_day=2,
)  # fmt: skip


def test_vectorize_order_types_and_missing_values():
    row = dict(zip(FEATURE_NAMES, vectorize(EVENT, FEATURES), strict=True))
    assert len(row) == len(FEATURE_NAMES)
    assert row["amount_log"] == pytest.approx(math.log1p(99))
    assert row["transaction_type"] == TYPE_CODES[EVENT.transaction_type]
    assert row["is_new_device"] == 1.0 and row["is_new_ip"] == 0.0
    assert math.isnan(row["cust_amount_mean"])  # None -> missing
    assert math.isnan(row["balance_drain_ratio"])


def test_time_split_is_chronological_and_disjoint():
    times = np.array([5, 1, 9, 3, 7, 2, 8, 4, 6, 0] * 10)
    split = time_split(times, valid_frac=0.2, test_frac=0.2)
    assert len(split.train) == 60 and len(split.valid) == 20 and len(split.test) == 20
    assert times[split.train].max() <= times[split.valid].min()
    assert times[split.valid].max() <= times[split.test].min()
    assert not set(split.train) & set(split.test)


def test_threshold_and_metrics():
    y = np.array([0, 0, 0, 1, 1, 0, 1])
    p = np.array([0.1, 0.2, 0.7, 0.8, 0.9, 0.3, 0.4])
    assert best_f1_threshold(y, p) == pytest.approx(0.4)
    scenario = np.array(["normal"] * 3 + ["ato", "ato", "travel", "ato"])
    m = evaluate(y, p, threshold=0.75, scenario=scenario)
    assert m["confusion"] == {"tp": 2, "fp": 0, "fn": 1, "tn": 4}
    assert m["by_scenario"]["ato"]["recall"] == pytest.approx(2 / 3, abs=1e-4)
    assert m["by_scenario"]["travel"]["false_positive_rate"] == 0.0


def _toy_matrix(n=4000, seed=0):
    rng = np.random.default_rng(seed)
    X = rng.normal(size=(n, len(FEATURE_NAMES))).astype(np.float32)
    X[:, FEATURE_NAMES.index("transaction_type")] = rng.integers(0, 5, n)
    signal = X[:, FEATURE_NAMES.index("cust_txn_count_5m")]
    y = (signal + rng.normal(scale=0.3, size=n) > 2.2).astype(np.int8)
    return FeatureMatrix(
        X=X, y=y, event_ms=np.arange(n, dtype=np.int64),
        transaction_id=np.array([f"TX-{i}" for i in range(n)]),
        scenario=np.where(y == 1, "fraud_x", "normal"), feature_names=FEATURE_NAMES,
    )  # fmt: skip


@pytest.fixture(scope="module")
def trained(tmp_path_factory):
    data = _toy_matrix()
    result = train(data, holdout=_toy_matrix(seed=1))
    out_dir = tmp_path_factory.mktemp("models")
    version, path, metadata = save(result, out_dir, "a" * 64, "toy")
    return data, result, version, path, metadata


def test_training_learns_signal_and_records_everything(trained):
    _, result, version, path, metadata = trained
    assert metadata["metrics"]["test"]["pr_auc"] > 0.8
    assert {"valid", "test", "holdout", "split"} <= set(metadata["metrics"])
    assert {c["name"] for c in metadata["candidates"]} == {"unweighted", "sqrt_balanced"}
    assert set(metadata["ablations"]) == {"without_balance", "without_network"}
    assert result.importance[0][0] == "cust_txn_count_5m"
    assert version.endswith("-aaaaaaaa")
    assert (path / "model.txt").exists()
    assert json.loads((path / "metadata.json").read_text())["version"] == version


def test_saved_model_reloads_with_identical_predictions(trained):
    data, result, _, path, _ = trained
    model = FraudModel.load(path)
    rows = data.X[:50].tolist()
    expected = result.booster.predict(data.X[:50], num_iteration=result.booster.best_iteration)
    assert np.allclose(model.predict_vectors(rows), expected, atol=1e-6)
    assert 0.0 <= model.predict(EVENT, FEATURES) <= 1.0


def test_model_with_other_feature_version_is_rejected(trained, tmp_path):
    _, _, _, path, metadata = trained
    (tmp_path / "model.txt").write_text((path / "model.txt").read_text())
    (tmp_path / "metadata.json").write_text(json.dumps({**metadata, "feature_version": 99}))
    with pytest.raises(IncompatibleModelError, match="feature v99"):
        FraudModel.load(tmp_path)


def test_report_has_all_sections(trained):
    text = report.render(trained[4])
    for heading in ("## Candidates", "## Metrics", "## By scenario", "## Ablations",
                    "## Feature importance"):  # fmt: skip
        assert heading in text
    assert "| holdout |" in text


def test_feature_matrix_roundtrip(tmp_path):
    data = _toy_matrix(n=100)
    path = tmp_path / "features_v1.npz"
    data.save(path)
    loaded = FeatureMatrix.load(path)
    assert np.array_equal(loaded.X, data.X) and loaded.feature_names == FEATURE_NAMES
    assert loaded.transaction_id[0] == "TX-0"
