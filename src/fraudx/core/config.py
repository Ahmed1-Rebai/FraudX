"""Runtime configuration.

All settings come from environment variables (or a local ``.env`` file).
Each backing service has its own prefixed settings group so that a service
only needs the variables it actually uses. Nothing here has a real secret as
a default: credentials must be supplied by the environment.

Risk-engine parameters live in a YAML file (``RISK_CONFIG_PATH``) because they
are tuned by analysts and versioned with the code, not per deployment.
"""

from __future__ import annotations

import tempfile
from functools import lru_cache
from pathlib import Path
from typing import Literal
from urllib.parse import quote_plus

import yaml
from pydantic import BaseModel, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(**_ENV, env_prefix="FRAUDX_")

    env: Literal["local", "test", "prod"] = "local"
    log_level: str = "INFO"
    log_format: Literal["json", "text"] = "json"
    risk_config_path: Path = Path("config/risk.yaml")
    dependency_timeout_s: float = Field(default=3.0, gt=0)


class WorkerSettings(BaseSettings):
    """Runtime settings shared by every stream worker process."""

    model_config = SettingsConfigDict(**_ENV, env_prefix="WORKER_")

    metrics_port: int = 9100
    heartbeat_path: Path = Path(tempfile.gettempdir()) / "fraudx" / "heartbeat"


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(**_ENV, env_prefix="KAFKA_")

    bootstrap_servers: str = "localhost:9092"
    client_id: str = "fraudx"


class MongoSettings(BaseSettings):
    model_config = SettingsConfigDict(**_ENV, env_prefix="MONGO_")

    host: str = "localhost"
    port: int = 27017
    db: str = "fraudx"
    app_user: str = "fraudx_app"
    app_password: SecretStr

    @property
    def uri(self) -> str:
        user = quote_plus(self.app_user)
        password = quote_plus(self.app_password.get_secret_value())
        return f"mongodb://{user}:{password}@{self.host}:{self.port}/{self.db}?authSource={self.db}"


class CassandraSettings(BaseSettings):
    model_config = SettingsConfigDict(**_ENV, env_prefix="CASSANDRA_")

    contact_points: str = "localhost"
    port: int = 9042
    keyspace: str = "fraudx"
    local_dc: str = "datacenter1"

    @property
    def hosts(self) -> list[str]:
        return [h.strip() for h in self.contact_points.split(",") if h.strip()]


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(**_ENV, env_prefix="REDIS_")

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: SecretStr


class Neo4jSettings(BaseSettings):
    model_config = SettingsConfigDict(**_ENV, env_prefix="NEO4J_")

    uri: str = "bolt://localhost:7687"
    user: str = "neo4j"
    password: SecretStr


# --- Risk engine -------------------------------------------------------------


class RiskWeights(BaseModel):
    ml: float = Field(ge=0, le=1)
    anomaly: float = Field(ge=0, le=1)
    graph: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def _sum_to_one(self) -> RiskWeights:
        total = self.ml + self.anomaly + self.graph
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"risk weights must sum to 1.0, got {total:.4f}")
        return self


class RiskThresholds(BaseModel):
    medium: int = Field(ge=1, le=100)
    high: int = Field(ge=1, le=100)

    @model_validator(mode="after")
    def _ordered(self) -> RiskThresholds:
        if self.medium >= self.high:
            raise ValueError("thresholds.medium must be lower than thresholds.high")
        return self


class RiskConfig(BaseModel):
    weights: RiskWeights
    thresholds: RiskThresholds
    alert_threshold: int = Field(ge=0, le=100)

    @classmethod
    def from_yaml(cls, path: Path) -> RiskConfig:
        with path.open(encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
        return cls.model_validate(raw.get("risk", raw))


@lru_cache
def get_app_settings() -> AppSettings:
    return AppSettings()


@lru_cache
def get_risk_config() -> RiskConfig:
    return RiskConfig.from_yaml(get_app_settings().risk_config_path)
