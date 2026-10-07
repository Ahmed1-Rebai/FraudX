from collections import defaultdict
from datetime import timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from fraudx.core.schemas import TransactionType
from fraudx.ingestion.generator import (
    DEBITS,
    FRAUD_SCENARIOS,
    GeneratorConfig,
    TransactionGenerator,
)

SMALL = GeneratorConfig(
    seed=7, customers=300, merchants=20, days=5, victim_share=0.04, rings=3, ring_size_max=5
)


@pytest.fixture(scope="module")
def generated():
    gen = TransactionGenerator(SMALL)
    return gen, list(gen.generate())


def _events(generated):
    return [g.event for g in generated[1]]


def test_same_seed_same_output():
    a = [g.event.model_dump_json() for g in TransactionGenerator(SMALL).generate()]
    b = [g.event.model_dump_json() for g in TransactionGenerator(SMALL).generate()]
    assert a == b


def test_different_seed_different_output(generated):
    other = TransactionGenerator(SMALL.model_copy(update={"seed": 8})).generate()
    assert [g.event for g in other][:50] != _events(generated)[:50]


def test_events_ordered_with_unique_ids(generated):
    events = _events(generated)
    times = [e.event_time for e in events]
    assert times == sorted(times)
    assert len({e.transaction_id for e in events}) == len(events)


def test_every_event_is_labelled_and_enriched(generated):
    for e in _events(generated):
        assert e.is_fraud_label is not None
        assert e.device_id and e.ip_address and e.location and e.balances


def test_balances_are_consistent(generated):
    last_after: dict[str, Decimal] = {}
    for e in _events(generated):
        b = e.balances
        sign = -1 if e.transaction_type in DEBITS else 1
        assert b.source_after == b.source_before + sign * e.amount
        assert b.source_after >= 0
        # Each account's balance continues from where its previous event left it.
        if e.source_account in last_after:
            assert b.source_before == last_after[e.source_account]
        last_after[e.source_account] = b.source_after
        last_after[e.destination_account] = b.destination_after


def test_fraud_rate_and_all_scenarios_present(generated):
    rows = generated[1]
    fraud = [g for g in rows if g.event.is_fraud_label]
    assert 0.001 < len(fraud) / len(rows) < 0.05
    assert {g.scenario for g in fraud} == set(FRAUD_SCENARIOS)
    assert all(g.scenario in FRAUD_SCENARIOS for g in fraud)


def test_legit_noise_overlaps_fraud_signals(generated):
    scenarios = {g.scenario for g in generated[1] if not g.event.is_fraud_label}
    assert {"travel", "new_device", "large_purchase", "normal"} <= scenarios
    abroad = [
        g for g in generated[1] if not g.event.is_fraud_label and g.event.location.country != "TN"
    ]
    assert abroad, "legit activity abroad must exist so geography alone isn't a label"


def test_ring_devices_are_shared_by_several_customers(generated):
    gen, _ = generated
    users = defaultdict(set)
    for e in _events(generated):
        users[e.device_id].add(e.customer_id)
    for ring in gen.rings:
        assert len(users[ring.devices[0]]) >= 3


def test_takeover_drains_victim_then_mule_cashes_out(generated):
    rows = generated[1]
    takeovers = [g.event for g in rows if g.scenario == "account_takeover"]
    cash_outs = [g.event for g in rows if g.scenario == "mule_cash_out"]
    assert takeovers and cash_outs
    for tx in takeovers:
        assert tx.transaction_type is TransactionType.TRANSFER
        assert tx.amount >= tx.balances.source_before * Decimal("0.8") - Decimal("0.01")
        mule_out = [
            c
            for c in cash_outs
            if c.customer_id != tx.customer_id
            and c.source_account == tx.destination_account
            and timedelta(0) < c.event_time - tx.event_time <= timedelta(hours=1)
        ]
        assert mule_out, f"no mule cash-out after {tx.transaction_id}"


def test_card_testing_is_a_burst_of_small_payments(generated):
    bursts = defaultdict(list)
    for g in generated[1]:
        if g.scenario == "card_testing":
            bursts[g.event.customer_id].append(g.event)
    assert bursts
    for events in bursts.values():
        assert len(events) >= 4  # 6+ planned; tiny ones always fit the balance
        small = [e for e in events if e.amount <= 20]
        assert len(small) >= 3
        assert events[-1].event_time - events[0].event_time < timedelta(minutes=20)


def test_config_rejects_impossible_population():
    with pytest.raises(ValidationError, match="not enough customers"):
        GeneratorConfig(customers=20, rings=10, ring_size_max=6)
