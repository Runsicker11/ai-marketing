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

# Full pipeline: ingest Shopify + Meta + Google Ads + Search Console → BigQuery → deploy views
uv run python -m ingestion.run_all --days-back 3

# Platform-specific ingestion
uv run python -m ingestion.run_all --shopify-only --days-back 3
uv run python -m ingestion.run_all --meta-only --days-back 3
uv run python -m ingestion.run_all --google-only --days-back 3
uv run python -m ingestion.run_all --search-console-only --days-back 7

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

# Content machine: generate new ad copy variations (Meta)
uv run python -m content.run --generate --count 10

# Content machine: generate Google Ads RSA copy
uv run python -m content.run --generate --platform google --count 10

# Content machine: generate for both platforms
uv run python -m content.run --generate --platform both --count 10

# Content machine: generate for a specific product
uv run python -m content.run --generate --product tungsten-tape --count 5

# Content machine: score components after 7+ days of data
uv run python -m content.run --score --print

# Content machine: full cycle (audit -> generate -> score)
uv run python -m content.run --all

# SEO content engine: identify what to write
uv run python -m seo.run --opportunities --print

# SEO content engine: generate article draft
uv run python -m seo.run --generate --type review --keyword "tungsten tape" --print

# SEO content engine: generate landing page for product
uv run python -m seo.run --generate --type landing_page --keyword "paddle tape" --product tungsten-tape

# SEO content engine: sync WordPress content inventory
uv run python -m seo.run --sync-inventory

# SEO content engine: push drafts to WordPress
uv run python -m seo.run --publish-drafts

# SEO content engine: score published content performance
uv run python -m seo.run --score --print

# SEO content engine: full cycle
uv run python -m seo.run --all

# Optimization: review search terms for negative keywords
uv run python -m optimization.run --search-terms --print

# Optimization: budget reallocation recommendations
uv run python -m optimization.run --budget --print

# Optimization: list pending action proposals
uv run python -m optimization.run --list-proposals

# Optimization: execute approved proposals
uv run python -m optimization.run --execute

# Optimization: full analysis cycle
uv run python -m optimization.run --all --print
```

## Architecture

### Data Flow
Shopify API + Meta Ads API + Google Ads API + Search Console API → BigQuery tables (20) → Unified views (18) → Claude analysis → Content library + SEO content + Optimization proposals

### BigQuery Tables (ingested)
- `shopify_orders`, `shopify_order_line_items`, `shopify_products`, `shopify_product_variants`, `shopify_customers`
- `meta_campaigns`, `meta_adsets`, `meta_ads`, `meta_daily_insights`, `meta_creatives`
- `google_ads_campaigns`, `google_ads_ad_groups`, `google_ads_keywords`, `google_ads_daily_insights`, `google_ads_search_terms`
- `content_library`
- `search_console_performance` (Phase 4)
- `content_posts` (Phase 5)
- `google_ads_ad_copy` (Phase 6)
- `optimization_actions` (Phase 6)

### BigQuery Views (computed)
- Phase 1: `vw_daily_performance` (cross-platform), `vw_true_roas` (cross-platform), `vw_product_performance`, `vw_trends`
- Phase 2 (GA4): `vw_ga4_attribution`, `vw_enhanced_roas` (cross-platform), `vw_ga4_funnel`, `vw_ga4_product_insights`
- Phase 3 (Content): `vw_creative_performance`, `vw_component_scores`
- Google Ads: `vw_google_ads_keywords`, `vw_search_terms_waste`
- Phase 4 (SEO): `vw_seo_opportunities`, `vw_seo_content_gaps`, `vw_seo_trends`, `vw_channel_summary`
- Phase 5 (Content Tracking): `vw_content_performance`
- Phase 6 (Google Ads Copy): `vw_google_ads_copy_performance`

### Analysis Module
- `analysis/claude_client.py` — thin wrapper, loads brand context from `config/brand.yaml`
- `analysis/alerts.py` — 8 threshold checks (ROAS, CPA, spend, funnel, CTR, keyword waste, quality score, ranking drop)
- `analysis/weekly_strategy.py` — queries all views including Google Ads keywords, search term waste, SEO opportunities
- `analysis/daily_report.py` — available but weekly cadence preferred (low order volume makes daily noisy)

### Content Machine (Phase 3)
- `content/audit.py` — audits existing ad creatives, extracts reusable components, populates library
- `content/generator/generate.py` — AI-powered copy generation for Meta + Google Ads (RSA headlines/descriptions)
- `content/scorer/score.py` — matches library components to live ads, scores performance, evolves library
- `content/library/` — CSV files (hooks.csv, bodies.csv, ctas.csv) populated by audit
- `content/generator/output/` — generated copy pending human review

### SEO Content Engine (Phase 5)
- `seo/opportunities.py` — identifies content opportunities from Search Console + GA4 data
- `seo/generate.py` — AI-powered article/landing page generation with SEO rules
- `seo/scorer.py` — scores published content against Search Console + GA4 performance
- `seo/wordpress/auth.py` — WordPress REST API authentication (Application Passwords)
- `seo/wordpress/publish.py` — creates draft posts in WordPress for review
- `seo/wordpress/inventory.py` — pulls existing WordPress content inventory
- `seo/shopify/pages.py` — creates landing pages on Shopify shop
- `seo/templates/` — markdown templates for each content type (review, comparison, how_to, landing_page)
- `seo/drafts/` — generated drafts pending human review
- `config/seo_content.yaml` — content generation rules (word counts, required sections, SEO rules)

### Optimization Engine (Phase 6)
- `optimization/search_terms.py` — AI reviews search terms, recommends negatives + keyword expansions
- `optimization/budget.py` — cross-channel budget intelligence and reallocation recommendations
- `optimization/actions.py` — action proposal system with human approval gates
- `optimization/proposals/` — JSON files for pending optimization proposals

### Search Console Integration (Phase 4)
- `ingestion/search_console/auth.py` — OAuth2 for Search Console API
- `ingestion/search_console/pull_performance.py` — pull query+page performance
- `ingestion/search_console/run.py` — orchestrator (same pattern as other ingestion modules)

## Key Conventions

- All ingestion modules are under `ingestion/` package
- Use `ingestion.utils.bq_client.run_query(sql)` for BigQuery queries
- Use `ingestion.utils.config` for all config constants (GCP_PROJECT_ID, BQ_DATASET, GA4_DATASET, etc.)
- Use `ingestion.utils.logger.get_logger(__name__)` for logging
- SQL views use `{dataset}` and `{ga4_dataset}` placeholders, replaced at deploy time by `deploy_views.py`
- Reports saved as markdown to `reports/` directory (gitignored)
- Content always requires human review before publishing (`review_gate: always` in seo_content.yaml)
- Optimization actions require human approval (proposals saved to `optimization/proposals/`)

## Important Gotchas

- **Shopify domain** is `1cee15-1f.myshopify.com`, NOT `pickleballeffectshop.myshopify.com`
- **GA4 transaction_id** = Shopify `order_id` (the long numeric ID), NOT `order_number`
- **Meta timestamps** come as `2025-06-23T09:19:47-0600` — must convert to `YYYY-MM-DD HH:MM:SS` for BigQuery
- **Windows console** needs `sys.stdout.reconfigure(encoding="utf-8")` for Unicode output
- **`.env` override** is enabled (`load_dotenv(override=True)`) so `.env` always wins over system env vars
- **Meta access token** is long-lived (60 days), expires ~Apr 26, 2026 — needs periodic refresh
- **Google Ads** `cost_micros` everywhere — `cost_micros`, `average_cpc`, `cost_per_conversion`, `cpc_bid_micros`, `budget.amount_micros` all need `/ 1_000_000`
- **Google Ads** `ad_group.type_` has trailing underscore (`type` is Python reserved word)
- **Google Ads** conversions are FLOAT64 (data-driven attribution gives fractional values)
- **Google Ads RSA** headlines max 30 chars, descriptions max 90 chars (different from Meta's 40/125)
- **Search Console** data has 2-3 day lag — default to 7 days back, ending 3 days ago
- **WordPress** requires Application Passwords for REST API auth

## .env Variables

```
# Existing
GCP_PROJECT_ID, ANTHROPIC_API_KEY
SHOPIFY_SHOP_DOMAIN, SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET
META_ADS_ACCOUNT_ID, META_APP_ID, META_APP_SECRET, META_ACCESS_TOKEN
GOOGLE_ADS_CUSTOMER_ID, GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_CLIENT_SECRET, GOOGLE_ADS_REFRESH_TOKEN, GOOGLE_ADS_LOGIN_CUSTOMER_ID

# Phase 4 (Search Console)
GOOGLE_SEARCH_CONSOLE_SITE_URL
GOOGLE_SEARCH_CONSOLE_CLIENT_ID
GOOGLE_SEARCH_CONSOLE_CLIENT_SECRET
GOOGLE_SEARCH_CONSOLE_REFRESH_TOKEN

# Phase 5 (WordPress)
WORDPRESS_URL
WORDPRESS_USER
WORDPRESS_APP_PASSWORD
```

## Phase Status

- **Phase 1 (Data Pipeline):** COMPLETE — Shopify + Meta + Google Ads → BigQuery → 12 unified views
- **Phase 2 (GA4 + AI Analysis):** COMPLETE — 4 GA4 views + Claude weekly reports + daily alerts
- **Phase 3 (Content Machine):** COMPLETE — creative text ingestion, content audit, AI copy generation, scoring feedback loop
- **Google Ads Pipeline:** COMPLETE — campaigns, ad groups, keywords, daily insights, search terms + 2 keyword/waste views
- **Phase 4 (Google Intelligence Layer):** COMPLETE — Search Console integration, 3 SEO views, Google Ads analysis in weekly reports, 3 new alerts, channel summary view
- **Phase 5 (Automated Content Engine):** COMPLETE — SEO content config, opportunity identification, AI article generation, WordPress/Shopify integration, content performance tracking, CLI orchestration
- **Phase 6 (Google Ads Copy & Optimization):** COMPLETE — Google Ads RSA copy generation, search term hygiene, budget intelligence, autonomous actions with approval gates

## Analysis Cadence Decision

Weekly reports preferred over daily — at ~10-15 orders/day, daily data is too noisy for meaningful trends. Daily alert checks still run (they're free when nothing triggers). Weekly strategy runs Sunday evening for Monday review.

## Business Context

- **Owner:** Braydon
- **Monthly ad spend:** ~$2K (Google Ads + Meta)
- **Top products:** Tungsten Weighted Tape, Paddle Tuning Tape, Edge Guard Tape, Soft Ultra Tac Overgrip
- **Competitors:** Bodhi Performance, UDrippin, Flick Weight
- **Brand voice:** Direct, authentic, no-BS, data-backed
