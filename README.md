# FastMCP BigQuery Server

A production-grade [FastMCP](https://gofastmcp.com/) server that exposes Google BigQuery metadata and query execution tools. Credentials are sourced from AWS Secrets Manager, and all responses include request IDs, detailed metrics, and structured logging suited for observability pipelines.

## Features

- 🔐 Service-account credentials loaded from AWS Secrets Manager (with optional LocalStack endpoint for mocks)
- 🧰 Two MCP tools: `get_table_schema` and `execute_query` (streamed/paginated results)
- 📊 Structured JSON logging with request IDs, duration, and row counts
- ⚙️ Configurable via environment variables + `.env` support for local work
- 🩺 `/health` HTTP endpoint for readiness probes and Docker health checks
- 🐳 Dockerfile based on `python:3.11-slim` with baked-in health check
- 🧪 Local test script (`scripts/local_test.py`) to hit the MCP tools via HTTP

## Repository Layout

```
src/bigquery_mcp/
  ├── aws_secrets.py        # Secret retrieval + retry logic
  ├── bigquery_service.py   # BigQuery wrapper (schema + streaming queries)
  ├── config.py             # Environment loading & validation
  ├── exceptions.py         # Custom error hierarchy
  ├── logging_config.py     # JSON logging formatter
  ├── server.py             # FastMCP server + tools + /health route
  ├── utils.py              # Request ID + response helpers
  └── validators.py         # Read-only SQL validation
scripts/
  └── local_test.py         # Async FastMCP client exercising both tools
Dockerfile
requirements.txt
pyproject.toml
.env.example
README.md
```

## Requirements

- Python 3.11+
- Google project with BigQuery enabled
- AWS account (or LocalStack) hosting a Secrets Manager entry that contains a Google service-account JSON blob

## Configuration

All configuration is environment-driven (with optional `.env` loading). Copy `.env.example` and adjust as needed:

```
cp .env.example .env
```

| Variable | Description |
| --- | --- |
| `AWS_REGION` | AWS region containing the secret |
| `SECRET_NAME` | Secrets Manager name/ARN of the BigQuery service-account JSON |
| `BIGQUERY_PROJECT` | Default GCP project for datasets/queries (falls back to the service-account project) |
| `LOG_LEVEL` | Python logging level (`DEBUG/INFO/WARNING/ERROR/CRITICAL`) |
| `AWS_SECRETS_ENDPOINT` | Optional override for the Secrets Manager endpoint (useful for LocalStack) |
| `QUERY_TIMEOUT_SECONDS` | BigQuery query timeout (default 300) |
| `DEFAULT_MAX_ROWS` | Hard cap for returned rows per query (default 10,000) |
| `ROW_CHUNK_SIZE` | Number of rows per streamed chunk (default 500) |

### Creating a mock secret for development

To test locally without touching production AWS, run Secrets Manager inside [LocalStack](https://www.localstack.cloud/):

```bash
# 1. Start LocalStack (Docker)
docker run -d --name localstack -p 4566:4566 localstack/localstack

# 2. Create a fake secret containing your service-account JSON
aws --endpoint-url http://localhost:4566 --region us-east-1 secretsmanager create-secret \
  --name dev/bigquery/service-account \
  --secret-string file://path/to/service-account.json

# 3. Point the server at LocalStack
export AWS_REGION=us-east-1
export SECRET_NAME=dev/bigquery/service-account
export AWS_SECRETS_ENDPOINT=http://localhost:4566
```

> ⚠️ Never commit raw credentials. The server logs mask secrets by design.

## Installation & Local Run

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -e .

# load env vars (if using .env)
export $(grep -v '^#' .env | xargs)

# start the MCP server over HTTP
fastmcp run src/bigquery_mcp/server.py --host 0.0.0.0 --port 8000
```

Once running, test the health endpoint:

```bash
curl -s http://localhost:8000/health | jq
```

## MCP Tools

### `get_table_schema`
- **Params**: `dataset_id`, `table_name`
- **Response**: JSON schema metadata (name, type, mode, description, nested fields)

### `execute_query`
- **Params**: `sql_query`, `max_rows` (optional; capped by `DEFAULT_MAX_ROWS`)
- **Behavior**:
  - Validates SQL (ensures read-only `SELECT/WITH/EXPLAIN`)
  - Streams rows into fixed chunks to avoid large allocations
  - Provides metrics: row count, duration, bytes processed, slot millis, cache hit flag

#### Example HTTP payloads

FastMCP speaks the MCP JSON-RPC protocol over HTTP. You can exercise the tools manually via `curl` by POSTing to `/mcp/` (the default HTTP route):

```bash
# Call get_table_schema
tool_payload='{"jsonrpc":"2.0","id":"1","method":"call_tool","params":{"name":"get_table_schema","arguments":{"dataset_id":"analytics","table_name":"events"}}}'
curl -s http://localhost:8000/mcp/ \
  -H "Content-Type: application/json" \
  -d "$tool_payload"

# Call execute_query
tool_payload='{"jsonrpc":"2.0","id":"2","method":"call_tool","params":{"name":"execute_query","arguments":{"sql_query":"SELECT * FROM `analytics.events` LIMIT 10"}}}'
curl -s http://localhost:8000/mcp/ \
  -H "Content-Type: application/json" \
  -d "$tool_payload"
```

For a friendlier workflow, use the included local test script (next section).

## Local Testing Script

`scripts/local_test.py` connects via FastMCP's `StreamableHttpTransport` and runs whichever tools you configure:

```bash
export MCP_HTTP_URL=http://localhost:8000/mcp/
export BQ_TEST_DATASET=analytics
export BQ_TEST_TABLE=events
export BQ_TEST_QUERY='SELECT COUNT(*) AS total FROM `analytics.events`'
python scripts/local_test.py
```

The script prints the structured JSON responses from each tool. Leave any of the `BQ_TEST_*` variables unset to skip that call.

## Docker

Build and run inside a container:

```bash
docker build -t bigquery-mcp .
docker run --rm -p 8000:8000 --env-file .env bigquery-mcp
```

The Docker image:
- Uses `python:3.11-slim`
- Installs dependencies via `requirements.txt`
- Exposes port `8000`
- Implements a Docker `HEALTHCHECK` hitting `/health`

## Monitoring & Logging

- All log lines are JSON formatted and include request IDs, tool names, duration, and row counts
- Query metrics (`duration_ms`, `row_count`, `bytes_processed`, etc.) are part of every successful tool response
- Errors return a consistent envelope: `{ "request_id": "...", "error": { "code": ..., "message": ..., "detail": ... } }`

## Shutdown & Cleanup

The `BigQueryService` closes its client on FastMCP lifespan shutdown, ensuring sockets are released and credentials are not reused beyond their lifetime.

## Next Steps

- Wire the server into your preferred MCP client (Cursor, Claude, etc.)
- Extend the toolset with additional read-only analytics helpers if needed
- Feed the JSON logs into your observability stack for full traceability
