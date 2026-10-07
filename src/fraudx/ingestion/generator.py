"""Deterministic synthetic transaction generator.

Produces PaySim-shaped transactions enriched with devices, IPs and locations,
with labelled fraud scenarios injected into otherwise normal behaviour:

* ``account_takeover`` - a fraudster on a fraud-ring device drains a victim's
  account with a TRANSFER to a mule, usually at night.
* ``mule_cash_out`` - the mule cashes the stolen money out minutes later.
* ``card_testing`` - a burst of tiny payments from an unknown device abroad,
  followed by one large payment.

Legitimate behaviour deliberately overlaps with fraud signals (travel abroad,
new phones, families sharing a device, occasional large purchases, mules with
ordinary activity) so no single feature separates the classes.

Generation is two-pass: all transactions are first planned with their
timestamps, then sorted and replayed against account balances so balances are
always consistent and fraud amounts can depend on the balance at that moment.
Output is a pure function of ``GeneratorConfig`` (including ``seed``).
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from random import Random

from pydantic import BaseModel, ConfigDict, Field, model_validator

from fraudx.core.schemas import (
    DEBIT_TYPES,
    Balances,
    Location,
    TransactionEvent,
    TransactionType,
)
from fraudx.ingestion import geo

CENT = Decimal("0.01")
DEBITS = DEBIT_TYPES
FRAUD_SCENARIOS = ("account_takeover", "mule_cash_out", "card_testing")
LEGIT_SCENARIOS = ("normal", "travel", "new_device", "large_purchase")

# Relative activity per hour of day (people sleep; fraudsters prefer night).
_DAY_HOURS = (1, 1, 1, 1, 1, 2, 4, 6, 8, 9, 9, 10, 11, 10, 9, 9, 10, 11, 12, 12, 10, 7, 4, 2)
_NIGHT_HOURS = (8, 9, 9, 8, 6, 4, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 4, 5, 6, 7)
# Legit transaction mix, roughly PaySim's.
_LEGIT_TYPES = (
    (TransactionType.PAYMENT, 0.40),
    (TransactionType.CASH_OUT, 0.22),
    (TransactionType.CASH_IN, 0.20),
    (TransactionType.TRANSFER, 0.13),
    (TransactionType.DEBIT, 0.05),
)
_AMOUNT_FACTOR = {
    TransactionType.PAYMENT: 1.0,
    TransactionType.DEBIT: 0.8,
    TransactionType.CASH_OUT: 2.5,
    TransactionType.CASH_IN: 3.0,
    TransactionType.TRANSFER: 3.0,
}


class GeneratorConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    seed: int = 42
    customers: int = Field(default=2000, ge=20)
    merchants: int = Field(default=200, ge=5)
    days: int = Field(default=14, ge=1, le=366)
    start: datetime = datetime(2026, 1, 1, tzinfo=UTC)
    mean_daily_txns: float = Field(default=2.0, gt=0, le=50)
    currency: str = "TND"

    # Fraud
    victim_share: float = Field(default=0.03, ge=0, le=0.5)
    ato_weight: float = Field(default=0.6, ge=0)
    card_testing_weight: float = Field(default=0.4, ge=0)
    rings: int = Field(default=5, ge=1)
    ring_size_min: int = Field(default=3, ge=2)
    ring_size_max: int = Field(default=6, ge=2)

    # Legitimate noise
    travel_share: float = Field(default=0.05, ge=0, le=1)
    new_device_rate: float = Field(default=0.005, ge=0, le=1)
    large_purchase_rate: float = Field(default=0.005, ge=0, le=1)
    shared_device_share: float = Field(default=0.02, ge=0, le=1)

    @model_validator(mode="after")
    def _check(self) -> GeneratorConfig:
        if self.ring_size_min > self.ring_size_max:
            raise ValueError("ring_size_min must be <= ring_size_max")
        if self.ato_weight + self.card_testing_weight <= 0:
            raise ValueError("at least one fraud pattern weight must be positive")
        if self.start.tzinfo is None:
            raise ValueError("start must be timezone-aware")
        mules = self.rings * self.ring_size_max
        victims = round(self.customers * self.victim_share)
        if mules + victims > self.customers:
            raise ValueError("not enough customers for the requested rings and victims")
        return self


@dataclass(frozen=True, slots=True)
class GeneratedTransaction:
    event: TransactionEvent
    scenario: str  # one of LEGIT_SCENARIOS or FRAUD_SCENARIOS


@dataclass(slots=True)
class _Customer:
    id: str
    account: str
    country: str
    city: str
    devices: list[str]
    ips: list[str]
    mean_amount: float
    daily_rate: float
    contacts: list[str] = field(default_factory=list)
    travel: tuple[int, int, str] | None = None  # (first_day, last_day, country)


@dataclass(frozen=True, slots=True)
class _Ring:
    devices: tuple[str, ...]
    ip: str
    country: str
    mules: tuple[_Customer, ...]


@dataclass(slots=True)
class _Plan:
    """A transaction before balances are applied."""

    time: datetime
    seq: int
    tx_type: TransactionType
    customer: _Customer
    destination: str
    device: str
    ip: str
    country: str
    city: str
    scenario: str
    amount: Decimal | None = None
    # Fraction of the source balance at execution time (drain patterns).
    fraction: tuple[float, float] | None = None


def _money(value: float | Decimal) -> Decimal:
    return Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)


class TransactionGenerator:
    def __init__(self, config: GeneratorConfig) -> None:
        self.config = config
        self.rng = Random(config.seed)  # noqa: S311 - reproducible simulation, not crypto
        self._seq = 0
        self.customers: list[_Customer] = []
        self.merchants: list[str] = []
        self.rings: list[_Ring] = []
        self.balances: dict[str, Decimal] = {}

    # --- public API ------------------------------------------------------------

    def generate(self) -> Iterator[GeneratedTransaction]:
        """Yield transactions in event-time order."""
        self._build_population()
        plans = self._plan_legit()
        plans += self._plan_fraud()
        plans.sort(key=lambda p: (p.time, p.seq))
        yield from self._execute(plans)

    # --- population ------------------------------------------------------------

    def _device(self) -> str:
        return f"DEV-{self.rng.getrandbits(40):010x}"

    def _build_population(self) -> None:
        cfg, rng = self.config, self.rng
        homes, weights = zip(*geo.HOME_WEIGHTS.items(), strict=True)
        for i in range(cfg.customers):
            country = rng.choices(homes, weights)[0]
            customer = _Customer(
                id=f"C-{i:06d}",
                account=f"A-{i:06d}",
                country=country,
                city=geo.random_city(rng, country),
                devices=[self._device()] + ([self._device()] if rng.random() < 0.3 else []),
                ips=[geo.random_ip(rng, country) for _ in range(rng.randint(1, 2))],
                mean_amount=rng.lognormvariate(math.log(80), 0.6),
                daily_rate=min(rng.lognormvariate(math.log(cfg.mean_daily_txns), 0.5), 20.0),
            )
            if rng.random() < cfg.travel_share:
                first = rng.randrange(cfg.days)
                dest = rng.choice([c for c in geo.TRAVEL_DESTINATIONS if c != country])
                customer.travel = (first, first + rng.randint(2, 5), dest)
            self.customers.append(customer)
            self.balances[customer.account] = _money(customer.mean_amount * rng.uniform(20, 200))

        for customer in self.customers:
            others = rng.sample(self.customers, 4)
            customer.contacts = [o.account for o in others if o is not customer][:3]

        # Families / flatmates: pairs sharing one device (legit graph overlap).
        for _ in range(round(cfg.customers * cfg.shared_device_share / 2)):
            a, b = rng.sample(self.customers, 2)
            b.devices.append(a.devices[0])

        for i in range(cfg.merchants):
            account = f"M-{i:05d}"
            self.merchants.append(account)
            self.balances[account] = Decimal("10000000.00")

        # Fraud rings recruit mules from the population: mules keep their
        # ordinary activity but also use the ring's shared devices.
        pool = rng.sample(self.customers, cfg.rings * cfg.ring_size_max)
        for r in range(cfg.rings):
            size = rng.randint(cfg.ring_size_min, cfg.ring_size_max)
            start = r * cfg.ring_size_max
            mules = tuple(pool[start : start + size])
            country = rng.choice(geo.RING_COUNTRIES)
            ring = _Ring(
                devices=tuple(self._device() for _ in range(rng.randint(1, 2))),
                ip=geo.random_ip(rng, country),
                country=country,
                mules=mules,
            )
            for mule in mules:
                mule.devices.insert(0, ring.devices[0])
                mule.ips.append(ring.ip)
            self.rings.append(ring)

    # --- planning ----------------------------------------------------------------

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _poisson(self, lam: float) -> int:
        limit, k, p = math.exp(-lam), 0, 1.0
        while True:
            p *= self.rng.random()
            if p <= limit:
                return k
            k += 1

    def _time_on(self, day: int, hours: tuple[int, ...]) -> datetime:
        hour = self.rng.choices(range(24), hours)[0]
        offset = timedelta(days=day, hours=hour, seconds=self.rng.randrange(3600))
        return self.config.start + offset

    def _plan_legit(self) -> list[_Plan]:
        cfg, rng = self.config, self.rng
        types, type_weights = zip(*_LEGIT_TYPES, strict=True)
        plans: list[_Plan] = []
        for customer in self.customers:
            for day in range(cfg.days):
                for _ in range(self._poisson(customer.daily_rate)):
                    tx_type = rng.choices(types, type_weights)[0]
                    scenario = "normal"
                    amount = customer.mean_amount * _AMOUNT_FACTOR[tx_type]
                    amount *= rng.lognormvariate(0, 0.5)

                    device = (
                        customer.devices[0] if rng.random() < 0.8 else rng.choice(customer.devices)
                    )
                    ip = rng.choice(customer.ips)
                    country, city = customer.country, customer.city
                    if customer.travel and customer.travel[0] <= day <= customer.travel[1]:
                        scenario, country = "travel", customer.travel[2]
                        city, ip = geo.random_city(rng, country), geo.random_ip(rng, country)
                    elif rng.random() < cfg.new_device_rate:
                        scenario, device = "new_device", self._device()
                        customer.devices.append(device)  # the new phone becomes known
                    elif rng.random() < cfg.large_purchase_rate:
                        scenario = "large_purchase"
                        amount *= rng.uniform(10, 30)

                    if tx_type is TransactionType.TRANSFER and customer.contacts:
                        destination = rng.choice(customer.contacts)
                    else:
                        destination = rng.choice(self.merchants)

                    plans.append(
                        _Plan(
                            time=self._time_on(day, _DAY_HOURS),
                            seq=self._next_seq(),
                            tx_type=tx_type,
                            customer=customer,
                            destination=destination,
                            device=device,
                            ip=ip,
                            country=country,
                            city=city,
                            scenario=scenario,
                            amount=_money(max(amount, 1.0)),
                        )
                    )
        return plans

    def _plan_fraud(self) -> list[_Plan]:
        cfg, rng = self.config, self.rng
        mules = {id(m) for ring in self.rings for m in ring.mules}
        candidates = [c for c in self.customers if id(c) not in mules]
        victims = rng.sample(candidates, round(cfg.customers * cfg.victim_share))
        plans: list[_Plan] = []
        for victim in victims:
            start = self._time_on(rng.randrange(cfg.days), _NIGHT_HOURS)
            pattern = rng.choices(
                ("account_takeover", "card_testing"), (cfg.ato_weight, cfg.card_testing_weight)
            )[0]
            if pattern == "account_takeover":
                plans += self._plan_takeover(victim, start)
            else:
                plans += self._plan_card_testing(victim, start)
        return plans

    def _plan_takeover(self, victim: _Customer, start: datetime) -> list[_Plan]:
        rng = self.rng
        ring = rng.choice(self.rings)
        mule = rng.choice(ring.mules)
        device = rng.choice(ring.devices)
        city = geo.random_city(rng, ring.country)
        common = {"device": device, "ip": ring.ip, "country": ring.country, "city": city}
        return [
            _Plan(
                time=start,
                seq=self._next_seq(),
                tx_type=TransactionType.TRANSFER,
                customer=victim,
                destination=mule.account,
                scenario="account_takeover",
                fraction=(0.8, 1.0),
                **common,
            ),
            _Plan(
                time=start + timedelta(minutes=rng.uniform(5, 60)),
                seq=self._next_seq(),
                tx_type=TransactionType.CASH_OUT,
                customer=mule,
                destination=rng.choice(self.merchants),
                scenario="mule_cash_out",
                fraction=(0.9, 1.0),
                **common,
            ),
        ]

    def _plan_card_testing(self, victim: _Customer, start: datetime) -> list[_Plan]:
        rng = self.rng
        if rng.random() < 0.5:
            device = rng.choice(rng.choice(self.rings).devices)
        else:
            device = self._device()
        country = rng.choice([c for c in geo.COUNTRIES if c != victim.country])
        ip, city = geo.random_ip(rng, country), geo.random_city(rng, country)
        plans, t = [], start
        for _ in range(rng.randint(5, 15) + 1):
            t += timedelta(seconds=rng.uniform(10, 60))
            plans.append(
                _Plan(
                    time=t,
                    seq=self._next_seq(),
                    tx_type=TransactionType.PAYMENT,
                    customer=victim,
                    destination=rng.choice(self.merchants),
                    device=device,
                    ip=ip,
                    country=country,
                    city=city,
                    scenario="card_testing",
                    amount=_money(rng.uniform(1, 20)),
                )
            )
        plans[-1].amount = _money(rng.uniform(500, 3000))  # the actual cash-in
        return plans

    # --- execution -----------------------------------------------------------------

    def _execute(self, plans: list[_Plan]) -> Iterator[GeneratedTransaction]:
        rng, balances = self.rng, self.balances
        tx_number = 0
        for plan in plans:
            source = plan.customer.account
            src_before = balances[source]
            if plan.fraction is not None:
                amount = _money(src_before * Decimal(str(rng.uniform(*plan.fraction))))
            else:
                assert plan.amount is not None
                amount = plan.amount
            is_fraud = plan.scenario in FRAUD_SCENARIOS
            debit = plan.tx_type in DEBITS
            if debit and amount > src_before:
                if not is_fraud:
                    continue  # customer can't afford it; the transaction never happens
                amount = src_before  # fraud takes whatever is left
            if amount <= 0:
                continue

            dst_before = balances.get(plan.destination, Decimal("0.00"))
            if debit:
                src_after, dst_after = src_before - amount, dst_before + amount
            else:  # CASH_IN: the agent/merchant pays the customer
                src_after, dst_after = src_before + amount, max(dst_before - amount, Decimal(0))
            balances[source], balances[plan.destination] = src_after, dst_after

            tx_number += 1
            event = TransactionEvent(
                transaction_id=f"TX-{tx_number:09d}",
                event_time=plan.time,
                customer_id=plan.customer.id,
                source_account=source,
                destination_account=plan.destination,
                amount=amount,
                currency=self.config.currency,
                transaction_type=plan.tx_type,
                device_id=plan.device,
                ip_address=plan.ip,
                location=Location(country=plan.country, city=plan.city),
                balances=Balances(
                    source_before=src_before,
                    source_after=src_after,
                    destination_before=dst_before,
                    destination_after=dst_after,
                ),
                is_fraud_label=is_fraud,
            )
            yield GeneratedTransaction(event=event, scenario=plan.scenario)
