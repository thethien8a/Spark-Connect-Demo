"""Load PostgreSQL connection settings from the project environment."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv


ENV_FILE = Path(__file__).resolve().parents[1] / ".env"


class ConfigError(ValueError):
    """Raised when required database configuration is invalid."""


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)


def _required(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise ConfigError(f"{name} is required")
    return value


def _port(name: str) -> int:
    value = _required(name)
    try:
        parsed = int(value)
    except ValueError as error:
        raise ConfigError(f"{name} must be an integer") from error
    if not 1 <= parsed <= 65535:
        raise ConfigError(f"{name} must be between 1 and 65535")
    return parsed


def load_source_app_config() -> PostgresConfig:
    load_dotenv(ENV_FILE, override=False)
    return PostgresConfig(
        host=_required("SOURCE_POSTGRES_HOST"),
        port=_port("SOURCE_POSTGRES_PORT"),
        database=_required("SOURCE_POSTGRES_DB"),
        user=_required("SOURCE_APP_USER"),
        password=_required("SOURCE_APP_PASSWORD"),
    )


def load_debezium_config() -> PostgresConfig:
    load_dotenv(ENV_FILE, override=False)
    return PostgresConfig(
        host=_required("SOURCE_POSTGRES_INTERNAL_HOST"),
        port=_port("SOURCE_POSTGRES_INTERNAL_PORT"),
        database=_required("SOURCE_POSTGRES_DB"),
        user=_required("DEBEZIUM_USER"),
        password=_required("DEBEZIUM_PASSWORD"),
    )
