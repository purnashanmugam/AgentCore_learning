"""Shared utility helpers."""

from __future__ import annotations

import uuid
from typing import Any


def generate_request_id() -> str:
    """Return a random request identifier for log correlation."""

    return uuid.uuid4().hex


def success_response(payload: dict[str, Any], *, request_id: str) -> dict[str, Any]:
    """Wrap successful tool responses with metadata."""

    return {"request_id": request_id, "data": payload}


def error_response(
    *,
    request_id: str,
    code: str,
    message: str,
    detail: str | None = None,
) -> dict[str, Any]:
    """Standardized error envelope for MCP tool responses."""

    error_payload: dict[str, Any] = {"code": code, "message": message}
    if detail:
        error_payload["detail"] = detail
    return {"request_id": request_id, "error": error_payload}
