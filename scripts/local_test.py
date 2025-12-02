#!/usr/bin/env python3
"""Simple helper script to exercise the MCP tools over HTTP."""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport

SERVER_URL = os.getenv("MCP_HTTP_URL", "http://localhost:8000/mcp/")
TEST_DATASET = os.getenv("BQ_TEST_DATASET")
TEST_TABLE = os.getenv("BQ_TEST_TABLE")
TEST_QUERY = os.getenv("BQ_TEST_QUERY")


async def _call_tool(client: Client, name: str, arguments: dict[str, Any]) -> None:
    result = await client.call_tool(
        name,
        arguments=arguments,
        raise_on_error=False,
    )
    print(f"\nTool '{name}' response:")
    print(json.dumps(result.structured_content, indent=2, default=str))


async def main() -> None:
    transport = StreamableHttpTransport(SERVER_URL)
    async with Client(transport) as client:
        print(f"Connected to MCP server at {SERVER_URL}")
        if TEST_DATASET and TEST_TABLE:
            await _call_tool(
                client,
                "get_table_schema",
                {"dataset_id": TEST_DATASET, "table_name": TEST_TABLE},
            )
        else:
            print("Set BQ_TEST_DATASET and BQ_TEST_TABLE to test schema retrieval")

        if TEST_QUERY:
            await _call_tool(
                client,
                "execute_query",
                {"sql_query": TEST_QUERY, "max_rows": 25},
            )
        else:
            print("Set BQ_TEST_QUERY to test query execution")


if __name__ == "__main__":
    asyncio.run(main())
