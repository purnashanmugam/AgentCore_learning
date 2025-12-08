"""BigQuery access layer used by the MCP tools."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery
from google.oauth2 import service_account

from .exceptions import (
    ConfigurationError,
    CredentialRefreshError,
    QueryExecutionError,
    TableNotFoundError,
)


@dataclass(slots=True)
class QueryChunk:
    """Represents a streamed subset of query results."""

    chunk_index: int
    rows: list[dict[str, Any]]


@dataclass(slots=True)
class QueryResult:
    """Structured payload returned to the MCP tool."""

    job_id: str
    statement_type: str | None
    row_count: int
    chunks: list[QueryChunk]
    cache_hit: bool | None
    bytes_processed: int | None
    slot_millis: int | None
    duration_ms: float
    truncated: bool


class BigQueryService:
    """Encapsulates BigQuery operations and credential lifecycle."""

    def __init__(
        self,
        *,
        credential_loader: Callable[[], dict[str, Any]],
        default_project: str | None,
        query_timeout_seconds: int,
        row_chunk_size: int,
    ) -> None:
        self._credential_loader = credential_loader
        self._default_project = default_project
        self._query_timeout_seconds = query_timeout_seconds
        self._row_chunk_size = row_chunk_size
        self._client_lock = threading.RLock()
        self._client: bigquery.Client | None = None
        self._credentials_info: dict[str, Any] | None = None
        self.refresh_client()

    def refresh_client(self) -> None:
        """Reload credentials from Secrets Manager and rebuild the client."""

        try:
            credentials_info = self._credential_loader()
        except Exception as exc:  # pragma: no cover - boto3 raises at runtime
            raise CredentialRefreshError("Unable to load credentials from secret") from exc

        project = self._default_project or credentials_info.get("project_id")
        if not project:
            raise ConfigurationError(
                "BIGQUERY_PROJECT is required when the secret is missing project_id"
            )

        credentials = service_account.Credentials.from_service_account_info(
            credentials_info,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )

        client = bigquery.Client(project=project, credentials=credentials)

        with self._client_lock:
            if self._client:
                self._client.close()
            self._client = client
            self._credentials_info = credentials_info

    def close(self) -> None:
        """Close the BigQuery client when shutting down the server."""

        with self._client_lock:
            if self._client:
                self._client.close()
                self._client = None

    def _require_client(self) -> bigquery.Client:
        with self._client_lock:
            if self._client is None:
                raise QueryExecutionError("BigQuery client is not initialized")
            return self._client

    def get_table_schema(self, dataset_id: str, table_name: str) -> list[dict[str, Any]]:
        """Fetch and format the schema metadata for a table."""

        client = self._require_client()
        table_ref = self._build_table_reference(dataset_id, table_name, client.project)

        try:
            table = client.get_table(table_ref)
        except google_exceptions.NotFound as exc:
            raise TableNotFoundError(
                f"Table '{table_ref}' was not found"
            ) from exc
        except google_exceptions.GoogleAPICallError as exc:  # pragma: no cover - network issues
            raise QueryExecutionError("Failed to retrieve table schema") from exc

        return [self._serialize_schema_field(field) for field in table.schema]

    def execute_query(self, sql_query: str, *, max_rows: int) -> QueryResult:
        """Execute a read-only query and stream results in chunks."""

        client = self._require_client()
        start = time.perf_counter()
        try:
            query_job = client.query(sql_query)
            iterator = query_job.result(
                timeout=self._query_timeout_seconds,
                page_size=self._row_chunk_size,
            )
        except google_exceptions.GoogleAPICallError as exc:
            self._maybe_refresh_on_auth_error(exc)
            raise QueryExecutionError(
                f"Failed to execute query: {exc.message if hasattr(exc, 'message') else exc}"
            ) from exc

        row_count = 0
        chunk_index = 0
        chunks: list[QueryChunk] = []
        current_chunk: list[dict[str, Any]] = []

        try:
            for row in iterator:
                row_dict = dict(row.items())
                current_chunk.append(row_dict)
                row_count += 1

                if len(current_chunk) >= self._row_chunk_size:
                    chunks.append(QueryChunk(chunk_index=chunk_index, rows=current_chunk))
                    chunk_index += 1
                    current_chunk = []

                if row_count >= max_rows:
                    break
        except google_exceptions.GoogleAPICallError as exc:  # pragma: no cover - runtime errors
            raise QueryExecutionError("Error streaming query results") from exc

        if current_chunk:
            chunks.append(QueryChunk(chunk_index=chunk_index, rows=current_chunk))

        total_rows = getattr(query_job, "total_rows", None)
        truncated = total_rows is not None and row_count < total_rows
        duration_ms = (time.perf_counter() - start) * 1000

        return QueryResult(
            job_id=query_job.job_id,
            statement_type=getattr(query_job, "statement_type", None),
            row_count=row_count,
            chunks=chunks,
            cache_hit=getattr(query_job, "cache_hit", None),
            bytes_processed=getattr(query_job, "total_bytes_processed", None),
            slot_millis=getattr(query_job, "slot_millis", None),
            duration_ms=duration_ms,
            truncated=truncated,
        )

    @staticmethod
    def _serialize_schema_field(field: bigquery.SchemaField) -> dict[str, Any]:
        return {
            "name": field.name,
            "type": field.field_type,
            "mode": field.mode,
            "description": field.description,
            "fields": [BigQueryService._serialize_schema_field(sub) for sub in field.fields]
            if field.fields
            else [],
        }

    @staticmethod
    def _build_table_reference(dataset_id: str, table_name: str, default_project: str) -> str:
        sanitized_dataset = dataset_id.replace("`", "").strip()
        if "." in sanitized_dataset:
            return f"{sanitized_dataset}.{table_name}"
        return f"{default_project}.{sanitized_dataset}.{table_name}"

    def _maybe_refresh_on_auth_error(self, exc: google_exceptions.GoogleAPICallError) -> None:
        error_code = getattr(exc, "code", None)
        if error_code in {401, 403}:
            try:
                self.refresh_client()
            except CredentialRefreshError:
                pass
