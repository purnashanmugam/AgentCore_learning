"""Input validation utilities."""

from __future__ import annotations

from .exceptions import QueryValidationError


READ_ONLY_PREFIXES = {"select", "with", "explain"}
PROHIBITED_PREFIXES = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "merge",
    "truncate",
    "grant",
    "revoke",
}


def validate_sql_query(sql_query: str) -> str:
    """Ensure the SQL query is non-empty and read-only."""

    if not sql_query or not sql_query.strip():
        raise QueryValidationError("SQL query cannot be empty")

    normalized = sql_query.strip()
    lower = normalized.lower()
    first_token = lower.split(None, 1)[0]

    if first_token in PROHIBITED_PREFIXES:
        raise QueryValidationError(
            "Only read-only queries are allowed (SELECT/WITH/EXPLAIN)."
        )

    if first_token not in READ_ONLY_PREFIXES:
        raise QueryValidationError(
            "SQL must start with SELECT, WITH, or EXPLAIN for safety."
        )

    if ";" in normalized[:-1]:
        raise QueryValidationError("Multiple SQL statements are not supported")

    # Remove a trailing semicolon to avoid accidental batching
    if normalized.endswith(";"):
        normalized = normalized[:-1]

    return normalized.strip()
