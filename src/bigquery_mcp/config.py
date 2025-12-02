"""Application configuration utilities."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

from dotenv import load_dotenv

from .exceptions import ConfigurationError


VALID_LOG_LEVELS: Final[set[str]] = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


@dataclass(frozen=True)
class AppConfig:
    """Configuration values required by the MCP server."""

    aws_region: str
    secret_name: str
    bigquery_project: str | None
    log_level: str
    secrets_endpoint_url: str | None
    query_timeout_seconds: int
    default_max_rows: int
    row_chunk_size: int


def _parse_int(value: str | None, *, default: int, name: str) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError as exc:  # pragma: no cover - defensive branch
        raise ConfigurationError(f"Environment variable {name} must be an integer") from exc
    if parsed <= 0:
        raise ConfigurationError(f"{name} must be greater than zero")
    return parsed


def load_config() -> AppConfig:
    """Load and validate configuration from environment variables."""

    load_dotenv()

    aws_region = os.getenv("AWS_REGION")
    secret_name = os.getenv("SECRET_NAME")
    bigquery_project = os.getenv("BIGQUERY_PROJECT")
    log_level = (os.getenv("LOG_LEVEL") or "INFO").upper()
    secrets_endpoint_url = os.getenv("AWS_SECRETS_ENDPOINT")

    if not aws_region:
        raise ConfigurationError("AWS_REGION is required for Secrets Manager access")
    if not secret_name:
        raise ConfigurationError("SECRET_NAME is required for Secrets Manager access")
    if log_level not in VALID_LOG_LEVELS:
        raise ConfigurationError(
            f"LOG_LEVEL must be one of {sorted(VALID_LOG_LEVELS)}"
        )

    query_timeout_seconds = _parse_int(
        os.getenv("QUERY_TIMEOUT_SECONDS"),
        default=300,
        name="QUERY_TIMEOUT_SECONDS",
    )
    default_max_rows = _parse_int(
        os.getenv("DEFAULT_MAX_ROWS"),
        default=10_000,
        name="DEFAULT_MAX_ROWS",
    )
    row_chunk_size = _parse_int(
        os.getenv("ROW_CHUNK_SIZE"),
        default=500,
        name="ROW_CHUNK_SIZE",
    )

    return AppConfig(
        aws_region=aws_region,
        secret_name=secret_name,
        bigquery_project=bigquery_project,
        log_level=log_level,
        secrets_endpoint_url=secrets_endpoint_url,
        query_timeout_seconds=query_timeout_seconds,
        default_max_rows=default_max_rows,
        row_chunk_size=row_chunk_size,
    )
