"""The Lua feature scripts against the real Redis."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from fraudx.core.config import RedisSettings
from fraudx.core.schemas import TransactionEvent
from fraudx.storage.clients import redis_client
from fraudx.streaming.features import OnlineFeatureStore

pytestmark = pytest.mark.integration

T0 = datetime(2026, 3, 1, 12, 0, tzinfo=UTC)


@pytest.fixture
def ctx():
    redis = redis_client(RedisSettings())
    run = uuid.uuid4().hex[:8]
    yield OnlineFeatureStore(redis), run
    for pattern in (f"fx:*{run}*", f"fx:ip:{{{_run_ip(run)}}}*"):
        for key in redis.scan_iter(match=pattern, count=1000):
            redis.delete(key)
    redis.close()


def _run_ip(run):
    """An IP unique to this test run, so leftovers from other runs can't interfere."""
    value = int(run, 16)
    return f"10.{value >> 16 & 255}.{value >> 8 & 255}.{value & 255}"


def _event(run, n, minutes, amount=100, customer="C", device="D", ip=None, country="TN"):
    return TransactionEvent(
        transaction_id=f"TX-{run}-{n}",
        event_time=T0 + timedelta(minutes=minutes),
        customer_id=f"{customer}-{run}",
        source_account=f"A-{run}",
        destination_account="M-1",
        amount=amount,
        transaction_type="PAYMENT",
        device_id=f"{device}-{run}",
        ip_address=ip or _run_ip(run),
        location={"country": country},
    )


def test_sliding_windows_use_event_time(ctx):
    store, run = ctx
    store.compute(_event(run, 1, -120, amount=50))  # 2 h before
    store.compute(_event(run, 2, -30, amount=60))  # 30 min before
    f = store.compute(_event(run, 3, 0, amount=70))
    assert (f.cust_txn_count_5m, f.cust_txn_count_1h, f.cust_txn_count_24h) == (1, 2, 3)
    assert (f.cust_amount_sum_5m, f.cust_amount_sum_1h, f.cust_amount_sum_24h) == (70, 130, 180)
    assert f.cust_history_count == 2
    assert f.cust_amount_mean == 55.0
    assert f.seconds_since_last_txn == 1800.0


def test_new_device_ip_country_flags(ctx):
    store, run = ctx
    first = store.compute(_event(run, 1, 0))
    assert first.is_new_device and first.is_new_ip and first.is_new_country
    same = store.compute(_event(run, 2, 1))
    assert not (same.is_new_device or same.is_new_ip or same.is_new_country)
    abroad = store.compute(_event(run, 3, 2, device="X", ip="105.112.1.1", country="NG"))
    assert abroad.is_new_device and abroad.is_new_ip and abroad.is_new_country


def test_redelivery_returns_identical_features_without_double_counting(ctx):
    store, run = ctx
    store.compute(_event(run, 1, 0))
    event = _event(run, 2, 1, amount=500)
    first = store.compute(event)
    again = store.compute(event)  # at-least-once redelivery
    assert again == first
    nxt = store.compute(_event(run, 3, 2))
    assert nxt.cust_history_count == 2  # not 3
    assert nxt.cust_txn_count_5m == 3


def test_shared_device_counts_distinct_customers(ctx):
    store, run = ctx
    for i, customer in enumerate(["A", "B", "C", "A"]):
        f = store.compute(_event(run, i, i, customer=customer, device="RING"))
    assert f.device_customer_count == 3
    assert f.device_txn_count_1h == 4
    assert f.ip_customer_count == 3


def test_late_event_counts_only_events_up_to_its_own_time(ctx):
    store, run = ctx
    store.compute(_event(run, 1, 0))
    store.compute(_event(run, 2, 10))
    late = store.compute(_event(run, 3, 3))  # arrives last, happened in between
    assert late.cust_txn_count_5m == 2  # itself + the one at minute 0
    assert late.cust_txn_count_1h == 2  # the minute-10 event is in its future
    assert late.seconds_since_last_txn is None  # latest seen is after it


def test_keys_have_ttls(ctx):
    store, run = ctx
    store.compute(_event(run, 1, 0))
    keys = list(store.redis.scan_iter(match=f"fx:*{run}*"))
    assert keys
    assert all(store.redis.ttl(k) > 0 for k in keys)
