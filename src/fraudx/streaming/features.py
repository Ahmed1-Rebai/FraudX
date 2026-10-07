"""Online feature store on Redis.

Per transaction, one pipelined round trip runs three Lua scripts:

* **customer** (keys tagged ``{customer_id}``): reads the profile *before*
  this transaction, adds it to the event-time sliding window, computes
  5 m / 1 h / 24 h counts and sums, updates the profile, and caches the raw
  result under the transaction id.
* **device** and **IP** (keys tagged by device / IP): 1 h velocity and the
  number of distinct customers seen on it.

Idempotency: window ZSETs and customer SETs are naturally idempotent (members
are transaction / customer ids). The non-idempotent profile counters are
guarded by the result cache. A redelivered transaction returns its cached
result without touching the profile, so replays yield identical features.
The cache TTL matches ``transactions.raw`` retention (7 days).

Windows are evaluated at the transaction's **event time** and include
transactions up to that instant, so historical replays produce the same
features as live traffic. Each key family uses its own hash tag, so every
script touches one Redis Cluster slot.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any

from redis import Redis

from fraudx.core.schemas import DEBIT_TYPES, TransactionEvent, TransactionFeatures

WINDOWS_MS = (300_000, 3_600_000, 86_400_000)  # 5 m, 1 h, 24 h
ENTITY_WINDOW_MS = 3_600_000
WINDOW_TTL_S = 2 * 86_400  # longest window + margin
PROFILE_TTL_S = 90 * 86_400
ENTITY_SET_TTL_S = 30 * 86_400
RESULT_CACHE_TTL_S = 7 * 86_400  # = transactions.raw retention

_CUSTOMER_LUA = """
-- KEYS: 1 window zset, 2 stats hash, 3 devices set, 4 ips set, 5 countries set, 6 result cache
-- ARGV: 1 tx_id, 2 ts_ms, 3 amount, 4 device, 5 ip, 6 country,
--       7 window_ttl_s, 8 profile_ttl_s, 9 cache_ttl_s, 10.. window sizes (ms)
local cached = redis.call('GET', KEYS[6])
if cached then return cached end

local ts = tonumber(ARGV[2])
local amount = tonumber(ARGV[3])
local st = redis.call('HMGET', KEYS[2], 'n', 'sum', 'sumsq', 'last_ts')

local function known(key, value)
  if value == '' then return cjson.null end
  return redis.call('SISMEMBER', key, value) == 1
end

local result = {
  prior_n = tonumber(st[1]) or 0,
  prior_sum = tonumber(st[2]) or 0,
  prior_sumsq = tonumber(st[3]) or 0,
  last_ts = tonumber(st[4]) or cjson.null,
  known_device = known(KEYS[3], ARGV[4]),
  known_ip = known(KEYS[4], ARGV[5]),
  known_country = known(KEYS[5], ARGV[6]),
}

redis.call('ZADD', KEYS[1], ts, ARGV[1] .. '|' .. ARGV[3])
local longest = 0
local windows = {}
for i = 10, #ARGV do
  local size = tonumber(ARGV[i])
  if size > longest then longest = size end
  local items = redis.call('ZRANGEBYSCORE', KEYS[1], ts - size, ts)
  local total = 0
  for _, item in ipairs(items) do
    total = total + tonumber(string.match(item, '|([^|]+)$'))
  end
  windows[#windows + 1] = {#items, total}
end
result.windows = windows
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', '(' .. (ts - longest))

redis.call('HINCRBY', KEYS[2], 'n', 1)
redis.call('HINCRBYFLOAT', KEYS[2], 'sum', ARGV[3])
redis.call('HINCRBYFLOAT', KEYS[2], 'sumsq', amount * amount)
if result.last_ts == cjson.null or ts > result.last_ts then
  redis.call('HSET', KEYS[2], 'last_ts', ARGV[2])
end
for i, value in ipairs({ARGV[4], ARGV[5], ARGV[6]}) do
  if value ~= '' then redis.call('SADD', KEYS[i + 2], value) end
end

redis.call('EXPIRE', KEYS[1], ARGV[7])
for i = 2, 5 do redis.call('EXPIRE', KEYS[i], ARGV[8]) end
local encoded = cjson.encode(result)
redis.call('SET', KEYS[6], encoded, 'EX', ARGV[9])
return encoded
"""

_ENTITY_LUA = """
-- KEYS: 1 window zset, 2 customers set
-- ARGV: 1 tx_id, 2 ts_ms, 3 customer_id, 4 window_ms, 5 window_ttl_s, 6 set_ttl_s
local ts = tonumber(ARGV[2])
local size = tonumber(ARGV[4])
redis.call('ZADD', KEYS[1], ts, ARGV[1])
local count = redis.call('ZCOUNT', KEYS[1], ts - size, ts)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', '(' .. (ts - size))
redis.call('SADD', KEYS[2], ARGV[3])
redis.call('EXPIRE', KEYS[1], ARGV[5])
redis.call('EXPIRE', KEYS[2], ARGV[6])
return {count, redis.call('SCARD', KEYS[2])}
"""


def customer_keys(customer_id: str, transaction_id: str) -> list[str]:
    base = f"fx:cust:{{{customer_id}}}"
    return [
        f"{base}:txw",
        f"{base}:stats",
        f"{base}:devices",
        f"{base}:ips",
        f"{base}:countries",
        f"{base}:result:{transaction_id}",
    ]


def entity_keys(kind: str, entity_id: str) -> list[str]:
    base = f"fx:{kind}:{{{entity_id}}}"
    return [f"{base}:txw", f"{base}:custs"]


@dataclass(frozen=True, slots=True)
class EntityStats:
    txn_count_1h: int
    customer_count: int


def build_features(
    event: TransactionEvent,
    customer: dict[str, Any],
    device: EntityStats | None,
    ip: EntityStats | None,
) -> TransactionFeatures:
    """Turn the raw store output into the feature contract (pure, unit-tested)."""
    amount = float(event.amount)
    (c5, s5), (c1h, s1h), (c24, s24) = customer["windows"]

    n = int(customer["prior_n"])
    mean = std = zscore = ratio = None
    if n >= 1:
        mean = customer["prior_sum"] / n
        ratio = amount / mean if mean > 0 else None
    if n >= 2:
        variance = max(customer["prior_sumsq"] / n - mean * mean, 0.0)
        std = math.sqrt(variance)
        zscore = (amount - mean) / std if std > 0 else None

    last_ts = customer["last_ts"]
    since_last = None
    event_ms = event.event_time.timestamp() * 1000
    if last_ts is not None and event_ms >= last_ts:
        since_last = (event_ms - last_ts) / 1000

    drain = None
    balances = event.balances
    if balances and event.transaction_type in DEBIT_TYPES and balances.source_before > 0:
        drain = amount / float(balances.source_before)

    def is_new(flag: bool | None) -> bool:
        return flag is False  # unknown (no value on the event) is not "new"

    return TransactionFeatures(
        cust_txn_count_5m=c5,
        cust_txn_count_1h=c1h,
        cust_txn_count_24h=c24,
        cust_amount_sum_5m=round(s5, 2),
        cust_amount_sum_1h=round(s1h, 2),
        cust_amount_sum_24h=round(s24, 2),
        cust_history_count=n,
        cust_amount_mean=_round(mean),
        cust_amount_std=_round(std),
        amount_zscore=_round(zscore),
        amount_to_mean_ratio=_round(ratio),
        seconds_since_last_txn=_round(since_last),
        is_new_device=is_new(customer["known_device"]),
        is_new_ip=is_new(customer["known_ip"]),
        is_new_country=is_new(customer["known_country"]),
        device_txn_count_1h=device.txn_count_1h if device else 0,
        device_customer_count=device.customer_count if device else 0,
        ip_txn_count_1h=ip.txn_count_1h if ip else 0,
        ip_customer_count=ip.customer_count if ip else 0,
        balance_drain_ratio=_round(drain),
        hour_of_day=event.event_time.hour,
    )


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 4)


class OnlineFeatureStore:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self._customer_script = redis.register_script(_CUSTOMER_LUA)
        self._entity_script = redis.register_script(_ENTITY_LUA)

    def compute(self, event: TransactionEvent) -> TransactionFeatures:
        return self.compute_many([event])[0]

    def compute_many(self, events: list[TransactionEvent]) -> list[TransactionFeatures]:
        """Features for a batch in one round trip.

        Redis executes pipelined scripts in order, so each event sees the state
        left by the events before it: the result is identical to calling
        ``compute`` once per event, in order.
        """
        pipe = self.redis.pipeline(transaction=False)
        layout = [self._enqueue(pipe, event) for event in events]
        results = iter(pipe.execute())
        out = []
        for event, (has_device, has_ip) in zip(events, layout, strict=True):
            customer = json.loads(next(results))
            customer["last_ts"] = customer.get("last_ts")  # cjson.null -> None
            device_stats = EntityStats(*next(results)) if has_device else None
            ip_stats = EntityStats(*next(results)) if has_ip else None
            out.append(build_features(event, customer, device_stats, ip_stats))
        return out

    def _enqueue(self, pipe: Any, event: TransactionEvent) -> tuple[bool, bool]:
        ts_ms = int(event.event_time.timestamp() * 1000)
        tx = event.transaction_id
        device = event.device_id or ""
        ip = str(event.ip_address) if event.ip_address else ""
        country = event.location.country if event.location else ""

        self._customer_script(
            keys=customer_keys(event.customer_id, tx),
            args=[tx, ts_ms, str(event.amount), device, ip, country,
                  WINDOW_TTL_S, PROFILE_TTL_S, RESULT_CACHE_TTL_S, *WINDOWS_MS],
            client=pipe,
        )  # fmt: skip
        entity_args = [tx, ts_ms, event.customer_id, ENTITY_WINDOW_MS, WINDOW_TTL_S,
                       ENTITY_SET_TTL_S]  # fmt: skip
        if device:
            self._entity_script(keys=entity_keys("dev", device), args=entity_args, client=pipe)
        if ip:
            self._entity_script(keys=entity_keys("ip", ip), args=entity_args, client=pipe)
        return bool(device), bool(ip)
