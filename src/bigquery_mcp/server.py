"""FastMCP server exposing Google BigQuery operations."""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Callable

from fastmcp import FastMCP
from fastmcp.tools import ToolResult
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from .aws_secrets import SecretsManagerProvider
from .bigquery_service import BigQueryService, QueryResult
from .config import AppConfig, load_config
from .exceptions import (
    MCPServerError,
    QueryExecutionError,
    QueryValidationError,
    TableNotFoundError,
)
from .logging_config import configure_logging
from .utils import error_response, generate_request_id, success_response
from .validators import validate_sql_query

CONFIG: AppConfig | None = None
LOGGER: logging.Logger | None = None
SECRETS_PROVIDER: SecretsManagerProvider | None = None
BIGQUERY: BigQueryService | None = None
SERVER_START = time.monotonic()


@asynccontextmanager
async def lifespan(_: FastMCP):
    """Initialize configuration, logging, and the BigQuery client."""

    global CONFIG, LOGGER, SECRETS_PROVIDER, BIGQUERY

    CONFIG = load_config()
    LOGGER = configure_logging(CONFIG.log_level)
    LOGGER.info(
        "Starting BigQuery MCP server",
        extra={
            "aws_region": CONFIG.aws_region,
            "bigquery_project": CONFIG.bigquery_project,
        },
    )

    SECRETS_PROVIDER = SecretsManagerProvider(
        region=CONFIG.aws_region,
        endpoint_url=CONFIG.secrets_endpoint_url,
    )
    credential_loader: Callable[[], dict[str, Any]] = lambda: SECRETS_PROVIDER.fetch_service_account(
        CONFIG.secret_name
    )
    BIGQUERY = BigQueryService(
        credential_loader=credential_loader,
        default_project=CONFIG.bigquery_project,
        query_timeout_seconds=CONFIG.query_timeout_seconds,
        row_chunk_size=CONFIG.row_chunk_size,
    )

    try:
        yield
    finally:
        if BIGQUERY:
            BIGQUERY.close()
        if LOGGER:
            LOGGER.info("BigQuery MCP server shut down")


def _require_service() -> BigQueryService:
    if BIGQUERY is None:
        raise QueryExecutionError("BigQuery service has not been initialized")
    return BIGQUERY


def _logger() -> logging.Logger:
    if LOGGER:
        return LOGGER
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger("bigquery_mcp")


def _format_query_result(result: QueryResult) -> dict[str, Any]:
    return {
        "job_id": result.job_id,
        "statement_type": result.statement_type,
        "row_count": result.row_count,
        "chunks": [
            {
                "chunk_index": chunk.chunk_index,
                "row_count": len(chunk.rows),
                "rows": chunk.rows,
            }
            for chunk in result.chunks
        ],
        "cache_hit": result.cache_hit,
        "bytes_processed": result.bytes_processed,
        "slot_millis": result.slot_millis,
        "duration_ms": result.duration_ms,
        "truncated": result.truncated,
    }


server = FastMCP(
    name="bigquery-mcp",
    instructions=(
        "Use the exposed tools to inspect BigQuery table schemas and run read-only queries."
        " Queries are streamed in bounded chunks to keep memory usage low."
    ),
    version="0.1.0",
    lifespan=lifespan,
)


@server.tool(name="get_table_schema")
async def get_table_schema(dataset_id: str, table_name: str) -> ToolResult:
    """Return the schema for a BigQuery table."""

    request_id = generate_request_id()
    log = _logger()
    log.info(
        "get_table_schema invoked",
        extra={
            "request_id": request_id,
            "dataset_id": dataset_id,
            "table_name": table_name,
        },
    )

    try:
        schema = await asyncio.to_thread(_require_service().get_table_schema, dataset_id, table_name)
    except TableNotFoundError as exc:
        return ToolResult(structured_content=error_response(
            request_id=request_id,
            code="TABLE_NOT_FOUND",
            message=str(exc),
        ))
    except MCPServerError as exc:
        log.exception("Schema lookup failed", extra={"request_id": request_id})
        return ToolResult(structured_content=error_response(
            request_id=request_id,
            code="SCHEMA_ERROR",
            message="Unable to load schema",
            detail=str(exc),
        ))

    log.info(
        "Schema retrieved",
        extra={
            "request_id": request_id,
            "dataset_id": dataset_id,
            "table_name": table_name,
            "column_count": len(schema),
        },
    )

    return ToolResult(structured_content=success_response(
        {"schema": schema, "dataset_id": dataset_id, "table_name": table_name},
        request_id=request_id,
    ))


@server.tool(name="execute_query")
async def execute_query(sql_query: str, max_rows: int | None = None) -> ToolResult:
    """Execute a validated SQL query and stream chunks back to the caller."""

    request_id = generate_request_id()
    log = _logger()
    log.info(
        "execute_query invoked",
        extra={
            "request_id": request_id,
            "max_rows": max_rows,
        },
    )

    try:
        validated_sql = validate_sql_query(sql_query)
        default_limit = CONFIG.default_max_rows if CONFIG else 10_000
        rows_limit = max_rows if isinstance(max_rows, int) else default_limit
        if rows_limit is None:
            rows_limit = default_limit
        if rows_limit > default_limit:
            log.warning(
                "Requested max_rows exceeds server limit; clamping",
                extra={
                    "request_id": request_id,
                    "requested": rows_limit,
                    "max_allowed": default_limit,
                },
            )
            rows_limit = default_limit
        if rows_limit <= 0:
            raise QueryValidationError("max_rows must be greater than zero")
    except QueryValidationError as exc:
        return ToolResult(structured_content=error_response(
            request_id=request_id,
            code="INVALID_SQL",
            message=str(exc),
        ))

    try:
        result = await asyncio.to_thread(
            _require_service().execute_query,
            validated_sql,
            max_rows=rows_limit,
        )
    except QueryExecutionError as exc:
        log.exception("Query execution failed", extra={"request_id": request_id})
        return ToolResult(structured_content=error_response(
            request_id=request_id,
            code="QUERY_FAILURE",
            message="BigQuery query failed",
            detail=str(exc),
        ))

    payload = _format_query_result(result)
    payload["sql"] = validated_sql
    payload["max_rows"] = rows_limit

    log.info(
        "Query completed",
        extra={
            "request_id": request_id,
            "row_count": result.row_count,
            "duration_ms": round(result.duration_ms, 2),
            "chunks": len(result.chunks),
        },
    )

    return ToolResult(structured_content=success_response(payload, request_id=request_id))


@server.custom_route("/health", methods=["GET"])
async def health(_: Request) -> Response:
    """Simple readiness probe for container orchestrators."""

    uptime_seconds = time.monotonic() - SERVER_START
    status_code = 200 if BIGQUERY else 503
    body = {
        "status": "ok" if status_code == 200 else "starting",
        "uptime_seconds": round(uptime_seconds, 2),
        "bigquery_ready": BIGQUERY is not None,
        "project": CONFIG.bigquery_project if CONFIG else None,
    }
    return JSONResponse(status_code=status_code, content=body)
