FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies first (cache layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy source code
COPY . .

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app

# Secret Manager mounts .env at /secrets/.env — copy it into /app at runtime
# Run alerts daily; add weekly strategy on Sundays (day 0)
CMD ["sh", "-c", "cp /secrets/.env /app/.env 2>/dev/null; python -m ingestion.analysis.run --alerts && if [ $(date -u +%w) -eq 0 ]; then python -m ingestion.analysis.run --weekly; fi"]
