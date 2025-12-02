"""Custom exception hierarchy for the BigQuery FastMCP server."""

from __future__ import annotations


class MCPServerError(Exception):
    """Base exception for the BigQuery MCP server."""


class ConfigurationError(MCPServerError):
    """Raised when required configuration is missing or invalid."""


class CredentialRetrievalError(MCPServerError):
    """Raised when service-account credentials cannot be retrieved."""


class CredentialRefreshError(MCPServerError):
    """Raised when refreshing service-account credentials fails."""


class TableNotFoundError(MCPServerError):
    """Raised when the requested BigQuery table cannot be found."""


class QueryValidationError(MCPServerError):
    """Raised when SQL validation fails prior to execution."""


class QueryExecutionError(MCPServerError):
    """Raised when BigQuery returns an error while executing a query."""


class HealthCheckError(MCPServerError):
    """Raised when the health endpoint cannot gather required information."""
