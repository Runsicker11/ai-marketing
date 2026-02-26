# CLAUDE.md

## Project Overview

AI marketing automation for **Pickleball Effect** — a pickleball paddle review site (pickleballeffect.com) with a Shopify accessory shop (pickleballeffectshop.com). This system replaces a $3K/month agency with automated data pipelines and AI-powered analysis.

## Tech Stack

- **Language:** Python 3.11+
- **Package manager:** uv (NOT pip)
- **BigQuery dataset:** `marketing_data` in GCP project `practical-gecko-373320`
- **GA4 dataset:** `practical-gecko-373320.analytics_456683467` (date-sharded event tables)
- **AI model:** Claude Haiku 4.5 (`claude-haiku-4-5-20251001`) for analysis reports
- **Credentials:** All in `.env` file, loaded via `python-dotenv` with `override=True`

## Key Commands

```bash
# Install dependencies
uv sync

# Full pipeline: ingest Shopify + Meta → BigQuery → deploy views
uv run python -m ingestion.run_all --days-back 3

# Deploy BigQuery views only
uv run python -m ingestion.run_all --views-only

# Run weekly strategy report (primary analysis cadence)
uv run python -m ingestion.analysis.run --weekly --print

# Run daily alert checks
uv run python -m ingestion.analysis.run --alerts

# Full pipeline with analysis
uv run python -m ingestion.run_all --days-back 3 --analyze

# Content machine: audit existing ad copy
uv run python -m content.run --audit --print

# Content machine: generate new ad copy variations
uv run python -m content.run --generate --count 10

# Content machine: generate for a specific product
uv run python -m content.run --generate --product tungsten-tape --count 5

# Content machine: score components after 7+ days of data
uv run python -m content.run --score --print

# Content machine: full cycle (audit -> generate -> score)
uv run python -m content.run --all
```

## Architecture

### Data Flow
Shopify API + Meta Ads API → BigQuery tables (11) → Unified views (10) → Claude analysis → Content library

### BigQuery Tables (ingested)
- `shopify_orders`, `shopify_order_line_items`, `shopify_products`, `shopify_product_variants`, `shopify_customers`
- `meta_campaigns`, `meta_adsets`, `meta_ads`, `meta_daily_insights`, `meta_creatives`
- `content_library`

### BigQuery Views (computed)
- Phase 1: `vw_daily_performance`, `vw_true_roas`, `vw_product_performance`, `vw_trends`
- Phase 2 (GA4): `vw_ga4_attribution`, `vw_enhanced_roas`, `vw_ga4_funnel`, `vw_ga4_product_insights`
- Phase 3 (Content): `vw_creative_performance`, `vw_component_scores`

### Analysis Module
- `analysis/claude_client.py` — thin wrapper, loads brand context from `config/brand.yaml`
- `analysis/alerts.py` — 5 threshold checks (ROAS, CPA, spend, funnel, CTR), thresholds in `config/thresholds.yaml`
- `analysis/weekly_strategy.py` — queries all views, sends to Claude for strategy report
- `analysis/daily_report.py` — available but weekly cadence preferred (low order volume makes daily noisy)

### Content Machine (Phase 3)
- `content/audit.py` — audits existing ad creatives, extracts reusable components, populates library
- `content/generator/generate.py` — AI-powered copy generation inspired by top performers
- `content/scorer/score.py` — matches library components to live ads, scores performance, evolves library
- `content/library/` — CSV files (hooks.csv, bodies.csv, ctas.csv) populated by audit
- `content/generator/output/` — generated copy pending human review

## Key Conventions

- All ingestion modules are under `ingestion/` package
- Use `ingestion.utils.bq_client.run_query(sql)` for BigQuery queries
- Use `ingestion.utils.config` for all config constants (GCP_PROJECT_ID, BQ_DATASET, GA4_DATASET, etc.)
- Use `ingestion.utils.logger.get_logger(__name__)` for logging
- SQL views use `{dataset}` and `{ga4_dataset}` placeholders, replaced at deploy time by `deploy_views.py`
- Reports saved as markdown to `reports/` directory (gitignored)

## Important Gotchas

- **Shopify domain** is `1cee15-1f.myshopify.com`, NOT `pickleballeffectshop.myshopify.com`
- **GA4 transaction_id** = Shopify `order_id` (the long numeric ID), NOT `order_number`
- **Meta timestamps** come as `2025-06-23T09:19:47-0600` — must convert to `YYYY-MM-DD HH:MM:SS` for BigQuery
- **Windows console** needs `sys.stdout.reconfigure(encoding="utf-8")` for Unicode output
- **`.env` override** is enabled (`load_dotenv(override=True)`) so `.env` always wins over system env vars
- **Meta access token** is long-lived (60 days), expires ~Apr 26, 2026 — needs periodic refresh
- **Google Ads** developer token is pending (applied for basic access) — not yet integrated

## Phase Status

- **Phase 1 (Data Pipeline):** COMPLETE — Shopify + Meta → BigQuery → 4 unified views
- **Phase 2 (GA4 + AI Analysis):** COMPLETE — 4 GA4 views + Claude weekly reports + daily alerts
- **Phase 3 (Content Machine):** COMPLETE — creative text ingestion, content audit, AI copy generation, scoring feedback loop
- **Phase 4 (Automated Optimization):** Not started
- **Phase 5 (Autonomous Operation):** Not started

## Analysis Cadence Decision

Weekly reports preferred over daily — at ~10-15 orders/day, daily data is too noisy for meaningful trends. Daily alert checks still run (they're free when nothing triggers). Weekly strategy runs Sunday evening for Monday review.

## Business Context

- **Owner:** Braydon
- **Monthly ad spend:** ~$2K (Google Ads + Meta)
- **Top products:** Tungsten Weighted Tape, Paddle Tuning Tape, Edge Guard Tape, Soft Ultra Tac Overgrip
- **Competitors:** Bodhi Performance, UDrippin, Flick Weight
- **Brand voice:** Direct, authentic, no-BS, data-backed
