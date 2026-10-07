"""Static geography used to synthesise plausible IPs and locations.

IP prefixes are illustrative /16 blocks per country; they only need to be
stable and country-consistent, not geolocation-accurate.
"""

from __future__ import annotations

from dataclasses import dataclass
from random import Random


@dataclass(frozen=True, slots=True)
class Country:
    code: str
    cities: tuple[str, ...]
    ip_prefixes: tuple[str, ...]


COUNTRIES: dict[str, Country] = {
    c.code: c
    for c in (
        Country("TN", ("Tunis", "Sfax", "Sousse", "Monastir", "Bizerte", "Gabes"),
                ("41.224", "41.226", "197.15", "196.203")),
        Country("FR", ("Paris", "Lyon", "Marseille"), ("90.12", "86.245")),
        Country("DE", ("Berlin", "Munich", "Hamburg"), ("84.150", "91.64")),
        Country("IT", ("Rome", "Milan"), ("79.20", "93.45")),
        Country("GB", ("London", "Manchester"), ("81.98", "86.150")),
        Country("US", ("New York", "Chicago"), ("73.15", "98.210")),
        Country("AE", ("Dubai",), ("94.200",)),
        Country("NG", ("Lagos",), ("105.112",)),
        Country("RU", ("Moscow",), ("95.24",)),
    )
}  # fmt: skip

# Where customers live (FraudX targets a Tunisian bank; diaspora abroad).
HOME_WEIGHTS: dict[str, float] = {"TN": 0.85, "FR": 0.06, "DE": 0.03, "IT": 0.03, "GB": 0.03}
# Where legitimate customers travel.
TRAVEL_DESTINATIONS: tuple[str, ...] = ("FR", "DE", "IT", "GB", "AE", "US")
# Where fraud infrastructure is hosted. TN is included on purpose: a model
# must not learn "foreign == fraud".
RING_COUNTRIES: tuple[str, ...] = ("NG", "RU", "AE", "TN")


def random_ip(rng: Random, country: str) -> str:
    prefix = rng.choice(COUNTRIES[country].ip_prefixes)
    return f"{prefix}.{rng.randint(1, 254)}.{rng.randint(1, 254)}"


def random_city(rng: Random, country: str) -> str:
    return rng.choice(COUNTRIES[country].cities)
