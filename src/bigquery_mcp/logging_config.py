"""Structured logging configuration for the MCP server."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    """Serialize log records as JSON for easier ingestion."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401 - inherited docstring
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        # Include any custom, non-standard attributes
        for key, value in record.__dict__.items():
            if key.startswith("_"):
                continue
            if key in payload:
                continue
            if key in {"args", "msg", "levelno", "pathname", "filename", "lineno", "funcName", "created", "msecs", "relativeCreated", "thread", "threadName", "processName", "process"}:
                continue
            payload[key] = value

        return json.dumps(payload, default=str)


def configure_logging(level: str) -> logging.Logger:
    """Configure root logging with the JSON formatter."""

    logger = logging.getLogger("bigquery_mcp")
    logger.setLevel(level)

    # Avoid duplicate handlers when reloading during development
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

    logger.propagate = False
    logger.debug("Logging configured", extra={"log_level": level})
    return logger
