import pytest

from fraudx.core.config import RiskConfig
from fraudx.core.schemas import RiskLevel
from fraudx.scoring.risk import NoSignalsError, RiskSignals, RuleHit, assess, should_alert


@pytest.fixture
def config() -> RiskConfig:
    return RiskConfig.model_validate(
        {
            "weights": {"ml": 0.4, "anomaly": 0.3, "graph": 0.3},
            "thresholds": {"medium": 30, "high": 70},
            "alert_threshold": 70,
        }
    )


def test_spec_example_scores_90_high(config):
    result = assess(RiskSignals(ml=0.91, anomaly=0.83, graph=0.96), config)
    assert result.score == 90
    assert result.level is RiskLevel.HIGH
    assert result.missing_signals == ()
    assert should_alert(result, config)


@pytest.mark.parametrize(
    ("value", "level"),
    [
        (0.0, RiskLevel.LOW),
        (0.29, RiskLevel.LOW),
        (0.30, RiskLevel.MEDIUM),
        (0.69, RiskLevel.MEDIUM),
        (0.70, RiskLevel.HIGH),
        (1.0, RiskLevel.HIGH),
    ],
)
def test_threshold_boundaries(config, value, level):
    result = assess(RiskSignals(ml=value, anomaly=value, graph=value), config)
    assert result.level is level


def test_missing_signal_reweights_instead_of_counting_as_zero(config):
    # Graph unavailable: 0.4*0.9 + 0.3*0.9 over 0.7 total weight = 0.9
    result = assess(RiskSignals(ml=0.9, anomaly=0.9), config)
    assert result.score == 90
    assert result.missing_signals == ("graph",)
    assert set(result.components) == {"ml", "anomaly"}


def test_rule_floor_overrides_low_model_score(config):
    result = assess(
        RiskSignals(ml=0.1, anomaly=0.1, graph=0.1),
        config,
        rule_hits=(RuleHit("blocklisted_device", floor=85),),
    )
    assert result.score == 85
    assert result.level is RiskLevel.HIGH
    assert result.rule_hits == ("blocklisted_device",)


def test_rules_alone_can_score(config):
    result = assess(RiskSignals(), config, rule_hits=(RuleHit("r", floor=40),))
    assert result.score == 40


def test_no_signal_and_no_rule_is_an_error(config):
    with pytest.raises(NoSignalsError):
        assess(RiskSignals(), config)


@pytest.mark.parametrize("bad", [-0.01, 1.01, float("nan")])
def test_out_of_range_signal_rejected(config, bad):
    with pytest.raises(ValueError, match="must be in"):
        assess(RiskSignals(ml=bad, anomaly=0.5, graph=0.5), config)
