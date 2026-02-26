# AI Marketing Automation

AI-powered marketing data pipeline and analysis for [Pickleball Effect](https://pickleballeffectshop.com), replacing manual agency ad management with automated data ingestion, attribution, and Claude-powered strategy reports.

## What It Does

1. **Ingests data** from Shopify (orders, products, customers) and Meta Ads (campaigns, insights) into BigQuery
2. **Joins with GA4** BigQuery export for accurate cross-channel attribution (fills the UTM gap where Shopify misses attribution data)
3. **Generates AI reports** using Claude to analyze performance, flag alerts, and recommend strategy changes

## Architecture

```
Shopify API ──┐
Meta Ads API ─┤──→ BigQuery (marketing_data) ──→ 8 Unified Views ──→ Claude Analysis
GA4 Export ───┘         9 tables                  4 core + 4 GA4        ↓
                                                                   Weekly Strategy
                                                                   Daily Alerts
```

### BigQuery Views

| View | Purpose |
|------|---------|
| `vw_daily_performance` | Meta spend/clicks/conversions by day/campaign/ad |
| `vw_true_roas` | Meta spend vs Shopify revenue (UTM-attributed) |
| `vw_product_performance` | Product sales by channel and source |
| `vw_trends` | 7d/30d rolling averages, DoD/WoW changes |
| `vw_ga4_attribution` | GA4 purchase events joined to Shopify orders via transaction_id |
| `vw_enhanced_roas` | GA4-attributed revenue per channel with ROAS |
| `vw_ga4_funnel` | Full session-to-purchase funnel by traffic source |
| `vw_ga4_product_insights` | Product views/carts/purchases from GA4 events |

### Analysis Cadence

| Frequency | What Runs | Cost |
|-----------|-----------|------|
| **Daily** | Alert threshold checks (ROAS floor, CPA ceiling, spend anomaly, funnel drops, CTR decline) | ~$0/day if no alerts trigger |
| **Weekly** | Full strategy report: channel ROAS, funnel analysis, product insights, ad creative review, budget recommendations | ~$0.01/report |

## Setup

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager
- GCP project with BigQuery enabled
- GA4 BigQuery export configured
- API credentials for Shopify, Meta Ads, and Anthropic (Claude)

### Installation

```bash
git clone <repo-url>
cd ai-marketing
cp .env.template .env
# Fill in credentials in .env (see SETUP_CREDENTIALS.md for details)
uv sync
```

### Credentials

See [SETUP_CREDENTIALS.md](SETUP_CREDENTIALS.md) for step-by-step instructions on obtaining each API credential. All credentials are stored in `.env` (gitignored).

Required environment variables:
- `GCP_PROJECT_ID` - Google Cloud project ID
- `SHOPIFY_SHOP_DOMAIN`, `SHOPIFY_CLIENT_ID`, `SHOPIFY_CLIENT_SECRET`
- `META_ADS_ACCOUNT_ID`, `META_APP_ID`, `META_APP_SECRET`, `META_ACCESS_TOKEN`
- `ANTHROPIC_API_KEY` - Claude API key (from platform.claude.com)

## Usage

### Full Pipeline (ingest + views)

```bash
uv run python -m ingestion.run_all --days-back 3
```

### Ingest + Analyze

```bash
uv run python -m ingestion.run_all --days-back 3 --analyze
```

### Individual Steps

```bash
# Ingest only
uv run python -m ingestion.run_all --shopify-only --days-back 7
uv run python -m ingestion.run_all --meta-only --days-back 7

# Deploy views only
uv run python -m ingestion.run_all --views-only

# Analysis only
uv run python -m ingestion.analysis.run --alerts          # daily alerts
uv run python -m ingestion.analysis.run --weekly          # weekly strategy
uv run python -m ingestion.analysis.run --weekly --print  # print to stdout
uv run python -m ingestion.analysis.run --all             # everything
```

### Recommended Schedule

- **Daily**: `uv run python -m ingestion.run_all --days-back 3 && uv run python -m ingestion.analysis.run --alerts`
- **Weekly (Sunday)**: `uv run python -m ingestion.analysis.run --weekly`

Reports are saved to `reports/` as markdown files.

## Project Structure

```
ai-marketing/
├── config/
│   ├── accounts.yaml         # Account IDs (non-secret)
│   ├── brand.yaml            # Brand voice, ICP, competitors, products
│   └── thresholds.yaml       # Alert thresholds (ROAS floor, CPA ceiling, etc.)
├── ingestion/
│   ├── run_all.py            # Master pipeline orchestrator
│   ├── schemas.py            # BigQuery table schemas
│   ├── setup_bigquery.py     # Table creation
│   ├── analysis/
│   │   ├── claude_client.py  # Claude API wrapper (Haiku 4.5)
│   │   ├── daily_report.py   # Daily performance report generator
│   │   ├── alerts.py         # Threshold checks + Claude analysis
│   │   ├── weekly_strategy.py # Weekly strategy report
│   │   └── run.py            # Analysis CLI orchestrator
│   ├── meta/                 # Meta Ads API ingestion
│   ├── shopify/              # Shopify API ingestion
│   ├── utils/
│   │   ├── bq_client.py      # BigQuery helper functions
│   │   ├── config.py         # Environment config loader
│   │   └── logger.py         # Structured logging
│   └── views/
│       ├── create_views.sql  # All 8 view definitions
│       └── deploy_views.py   # View deployment script
├── reports/                  # Generated reports (gitignored)
├── tests/
├── PLAN.md                   # 5-phase master plan
├── SETUP_CREDENTIALS.md      # Credential setup guide
└── pyproject.toml
```

## Cost

| Item | Monthly Cost |
|------|-------------|
| BigQuery | ~$0 (free tier) |
| Claude API (Haiku, weekly reports + daily alerts) | ~$1-5 |
| **Total infrastructure** | **~$1-5/month** |
