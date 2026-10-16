import json

import numpy as np
import pytest

from fraudx.ml import report
from fraudx.ml.anomaly import FEATURES, AnomalyModel, train
from fraudx.ml.dataset import FeatureMatrix
from fraudx.ml.vectorize import FEATURE_NAMES


def _matrix(n=4000, seed=0):
    """Normal rows near 0; positives are far outliers on several features at once."""
    rng = np.random.default_rng(seed)
    X = rng.normal(size=(n, len(FEATURE_NAMES))).astype(np.float32)
    X[:, FEATURE_NAMES.index("cust_amount_mean")] = np.nan  # missing everywhere but filled
    y = (rng.random(n) < 0.02).astype(np.int8)
    for name in ("amount_zscore", "amount_log", "cust_txn_count_5m", "device_customer_count"):
        X[y == 1, FEATURE_NAMES.index(name)] = 8.0
    return FeatureMatrix(
        X=X, y=y, event_ms=np.arange(n, dtype=np.int64),
        transaction_id=np.array([f"TX-{i}" for i in range(n)]),
        scenario=np.where(y == 1, "outlier", "normal"), feature_names=FEATURE_NAMES,
    )  # fmt: skip


@pytest.fixture(scope="module")
def model():
    return train(_matrix(), "b" * 64, "toy", holdout=_matrix(seed=1))


def test_outliers_rank_high_and_scores_are_percentiles(model):
    data = _matrix(seed=2)
    scores = model.score_vectors(data.X)
    assert scores.min() >= 0.0 and scores.max() <= 1.0
    assert np.median(scores[data.y == 1]) > 0.99  # far outliers sit above normal traffic
    assert 0.3 < np.median(scores[data.y == 0]) < 0.7  # normal traffic spreads over [0, 1]
    assert model.metadata["metrics"]["test"]["recall"] > 0.9
    assert "categorical" not in FEATURES and "transaction_type" not in FEATURES


def test_save_load_roundtrip_and_feature_check(model, tmp_path):
    out = model.save(tmp_path)
    loaded = AnomalyModel.load(out)
    rows = _matrix(n=50, seed=3).X
    assert np.allclose(loaded.score_vectors(rows), model.score_vectors(rows))
    meta = json.loads((out / "metadata.json").read_text())
    (out / "metadata.json").write_text(json.dumps({**meta, "feature_names": ["x"]}))
    with pytest.raises(ValueError, match="different features"):
        AnomalyModel.load(out)


def test_anomaly_report(model):
    text = report.render_anomaly(model.metadata)
    assert "## By scenario (test)" in text and "PR-AUC matched" in text
