FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies first (cache layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy source code
COPY . .

# Create directories that are gitignored but needed at runtime
RUN mkdir -p /app/reports /app/optimization/proposals /app/seo/drafts /app/content/generator/output

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app

# Secret Manager mounts .env at /secrets/.env — copy it into /app at runtime
# Daily:   alerts + optimization proposals (Slack notified)
# Monday:  + gads-health (30-rule account audit, 8am UTC via separate trigger)
#          + weekly strategy report
CMD ["sh", "-c", "cp /secrets/.env /app/.env 2>/dev/null; python -m ingestion.analysis.run --alerts; python -m ingestion.analysis.run --optimize; if [ $(date -u +%w) -eq 1 ]; then python -m ingestion.analysis.run --gads-health; python -m ingestion.analysis.run --weekly; fi"]
