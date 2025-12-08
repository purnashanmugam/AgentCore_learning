FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pyproject.toml README.md .
COPY .env.example .
COPY src ./src
COPY scripts ./scripts

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD curl -f http://localhost:8000/health || exit 1

# Install the package itself so absolute imports resolve everywhere
RUN pip install --no-cache-dir --no-deps .

CMD ["fastmcp", "run", "src/bigquery_mcp/server.py", "--host", "0.0.0.0", "--port", "8000"]
