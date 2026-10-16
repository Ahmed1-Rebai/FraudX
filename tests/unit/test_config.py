from pathlib import Path

import pytest
from pydantic import ValidationError

from fraudx.core.config import MongoSettings, RiskConfig

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_shipped_risk_config_is_valid():
    """The shipped values come from `make tune-risk`; check what must always hold."""
    config = RiskConfig.from_yaml(REPO_ROOT / "config" / "risk.yaml")
    w = config.weights
    assert w.ml + w.anomaly + w.graph == pytest.approx(1.0)
    assert config.thresholds.medium < config.thresholds.high == config.alert_threshold


def _config(**overrides):
    raw = {
        "weights": {"ml": 0.4, "anomaly": 0.3, "graph": 0.3},
        "thresholds": {"medium": 30, "high": 70},
        "alert_threshold": 70,
    }
    raw.update(overrides)
    return RiskConfig.model_validate(raw)


def test_weights_must_sum_to_one():
    with pytest.raises(ValidationError, match=r"sum to 1\.0"):
        _config(weights={"ml": 0.5, "anomaly": 0.3, "graph": 0.3})


def test_thresholds_must_be_ordered():
    with pytest.raises(ValidationError, match="lower than"):
        _config(thresholds={"medium": 80, "high": 70})


def test_mongo_uri_escapes_credentials(monkeypatch):
    monkeypatch.setenv("MONGO_APP_PASSWORD", "p@ss:w/rd")
    uri = MongoSettings().uri
    assert "p%40ss%3Aw%2Frd" in uri
    assert uri.endswith("/fraudx?authSource=fraudx")


def test_secrets_are_not_rendered(monkeypatch):
    monkeypatch.setenv("MONGO_APP_PASSWORD", "super-secret")
    assert "super-secret" not in repr(MongoSettings())
