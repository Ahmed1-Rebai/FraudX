"""ML against real services: offline feature build and the MongoDB registry."""

import uuid

import numpy as np
import pytest

from fraudx.core.config import MongoSettings, RedisSettings
from fraudx.ingestion.dataset import write_dataset
from fraudx.ingestion.generator import GeneratorConfig, TransactionGenerator
from fraudx.ml import registry
from fraudx.ml.dataset import build
from fraudx.storage.clients import mongo_client, redis_client
from fraudx.streaming.features import OnlineFeatureStore

pytestmark = pytest.mark.integration

SCRATCH_DB = 3  # never the live db (0) or the default build db (1)


@pytest.fixture
def scratch_redis():
    redis = redis_client(RedisSettings().model_copy(update={"db": SCRATCH_DB}))
    redis.flushdb()
    yield redis
    redis.flushdb()
    redis.close()


@pytest.fixture(scope="module")
def small_dataset(tmp_path_factory):
    out = tmp_path_factory.mktemp("ds")
    config = GeneratorConfig(customers=80, days=2, victim_share=0.1, rings=2, ring_size_max=4)
    rows = ((g.event, g.scenario) for g in TransactionGenerator(config).generate())
    write_dataset(rows, out, lambda: {})
    return out


def test_batched_features_equal_one_at_a_time(scratch_redis, small_dataset):
    from fraudx.ingestion.dataset import EVENTS_FILE, read_events

    events = list(read_events(small_dataset / EVENTS_FILE))[:300]
    store = OnlineFeatureStore(scratch_redis)
    sequential = [store.compute(e) for e in events]
    scratch_redis.flushdb()
    batched = store.compute_many(events[:150]) + store.compute_many(events[150:])
    assert batched == sequential


def test_offline_build_flushes_its_db_and_spares_the_live_one(scratch_redis, small_dataset):
    live = redis_client(RedisSettings())
    live_keys_before = live.dbsize()
    matrix = build(small_dataset, scratch_redis, batch_size=64)
    assert len(matrix.y) == len(matrix.X) > 0
    assert matrix.y.sum() > 0
    assert np.all(np.diff(matrix.event_ms) >= 0)
    assert scratch_redis.dbsize() == 0  # flushed afterwards
    assert live.dbsize() == live_keys_before  # live feature state untouched
    live.close()


@pytest.fixture
def mongo_db():
    settings = MongoSettings()
    client = mongo_client(settings)
    db = client[settings.db]
    name = f"it_model_{uuid.uuid4().hex[:6]}"
    yield db, name
    db[registry.COLLECTION].delete_many({"name": name})
    client.close()


def _metadata(name, version):
    return {
        "name": name, "version": version, "metrics": {"test": {"pr_auc": 0.9}}, "params": {},
        "feature_names": ["a"], "feature_version": 1, "threshold": 0.5,
        "data": {"dataset": "x", "sha256": "0" * 64}, "trained_at": "2026-10-07T00:00:00+00:00",
    }  # fmt: skip


def test_registry_keeps_exactly_one_active_version(mongo_db):
    db, name = mongo_db
    registry.register(db, _metadata(name, "v1"), "models/x/v1", activate=True)
    registry.register(db, _metadata(name, "v2"), "models/x/v2", activate=True)
    assert registry.active(db, name)["version"] == "v2"
    assert db[registry.COLLECTION].count_documents({"name": name, "active": True}) == 1
    registry.activate_version(db, name, "v1")  # rollback
    assert registry.active(db, name)["version"] == "v1"
    with pytest.raises(KeyError):
        registry.activate_version(db, name, "nope")


def test_offline_mode_skips_result_cache_without_changing_features(scratch_redis, small_dataset):
    from fraudx.ingestion.dataset import EVENTS_FILE, read_events

    events = list(read_events(small_dataset / EVENTS_FILE))[:200]
    cached = OnlineFeatureStore(scratch_redis).compute_many(events)
    assert any(scratch_redis.scan_iter(match="fx:cust:*:result:*"))
    scratch_redis.flushdb()
    uncached = OnlineFeatureStore(scratch_redis, cache_results=False).compute_many(events)
    assert uncached == cached
    assert not any(scratch_redis.scan_iter(match="fx:cust:*:result:*"))
