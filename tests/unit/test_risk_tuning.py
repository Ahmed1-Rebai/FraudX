import math

import numpy as np

from fraudx.core.schemas import RiskLevel
from fraudx.ml import calibration
from fraudx.ml.vectorize import FEATURE_NAMES
from fraudx.scoring import rules
from fraudx.scoring.tuning import _best_threshold, combine, grid, to_score


def test_calibration_is_monotone_bounded_probability():
    rng = np.random.default_rng(0)
    scores = rng.random(5000)
    y = (rng.random(5000) < scores**4).astype(int)  # fraud more likely at high scores
    cal = calibration.fit(scores, y)
    out = calibration.apply(cal, np.linspace(-1, 2, 200))  # also outside the fitted range
    assert np.all(np.diff(out) >= 0) and out.min() >= 0 and out.max() <= 1
    assert calibration.apply(cal, np.array([0.05]))[0] < calibration.apply(cal, np.array([0.95]))[0]


def _vector(**values):
    row = [math.nan] * len(FEATURE_NAMES)
    for name, value in values.items():
        row[FEATURE_NAMES.index(name)] = value
    return row


def test_rules_fire_alike_on_one_transaction_and_on_a_matrix():
    takeover = _vector(is_new_device=1, is_new_country=0, balance_drain_ratio=0.95)
    burst = _vector(cust_txn_count_5m=6, device_customer_count=3, is_new_device=0)
    missing = _vector(is_new_device=1)  # no balance: a missing value never fires a rule
    assert rules.evaluate(rules.as_values(takeover)) == [("takeover_pattern", RiskLevel.HIGH)]
    assert rules.evaluate(rules.as_values(burst)) == [("burst_on_shared_device", RiskLevel.MEDIUM)]
    assert rules.evaluate(rules.as_values(missing)) == []
    columns = rules.as_values(np.array([takeover, burst, missing]))
    _, rule = rules.RULES["takeover_pattern"]
    assert list(np.asarray(rule(columns), dtype=bool)) == [True, False, False]


def test_engine_combination_and_floors():
    signals = {"ml": np.array([0.9, 0.0]), "graph": np.array([0.1, 0.0])}
    combined = combine(signals, {"ml": 0.5, "anomaly": 0.1, "graph": 0.4})  # anomaly missing
    assert np.allclose(combined, [(0.5 * 0.9 + 0.4 * 0.1) / 0.9, 0.0])  # reweighted
    score = to_score(np.array([0.004, 0.004, 0.004]), np.array([0, 1, 2]), medium=8, high=23)
    assert list(score) == [0, 8, 23]  # rule floors: MEDIUM -> medium, HIGH -> high
    assert len(grid()) == 66 and all(math.isclose(sum(w.values()), 1) for w in grid())
    y = np.array([0, 0, 1, 1])
    assert _best_threshold(y, np.array([5, 10, 40, 60]), beta=1.0) == 11  # first separating score
