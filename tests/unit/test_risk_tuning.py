import math

import numpy as np

from fraudx.core.schemas import RiskLevel, TransactionType
from fraudx.ml import calibration
from fraudx.ml.vectorize import FEATURE_NAMES, TYPE_CODES
from fraudx.scoring import rules
from fraudx.scoring.tuning import _best_threshold, combine, grid, to_score
from fraudx.storage.feedback import GraphFeedback, payee_account

TRANSFER = TYPE_CODES[TransactionType.TRANSFER]
PAYMENT = TYPE_CODES[TransactionType.PAYMENT]


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


def test_known_fraud_rule_needs_a_first_transfer_to_a_confirmed_account():
    transfer = rules.as_values(_vector(is_new_device=0, transaction_type=TRANSFER))
    payment = rules.as_values(_vector(is_new_device=0, transaction_type=PAYMENT))
    confirmed_first = {"distance_to_known_fraud": 0.0, "is_new_payee": 1.0}
    assert rules.evaluate(transfer | confirmed_first) == [
        ("payee_confirmed_fraud", RiskLevel.MEDIUM)
    ]
    assert rules.evaluate(transfer | confirmed_first | {"is_new_payee": 0.0}) == []  # repeat
    assert rules.evaluate(payment | confirmed_first) == []  # not a transfer
    assert rules.evaluate(transfer | {"distance_to_known_fraud": 1.0, "is_new_payee": 1.0}) == []
    assert rules.evaluate(transfer | {"payer_confirmed_fraud": 1.0}) == []  # evidence only
    assert rules.evaluate(transfer) == []  # offline: no confirmations exist


def test_feedback_marks_the_account_the_money_went_to():
    transfer = {"transaction_id": "TX-1", "transaction_type": "TRANSFER",
                "source_account": "A-1", "destination_account": "A-2"}  # fmt: skip
    assert payee_account(transfer) == "A-2"
    assert payee_account({**transfer, "transaction_type": "CASH_IN"}) == "A-1"
    to_merchant = {**transfer, "destination_account": "M-00155"}
    assert GraphFeedback(driver=None).record(to_merchant, confirmed=True) is None
