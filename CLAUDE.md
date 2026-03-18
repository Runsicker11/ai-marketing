# CLAUDE.md

## Project Overview

AI marketing automation for **Pickleball Effect** — a pickleball paddle review site (pickleballeffect.com) with a Shopify accessory shop (pickleballeffectshop.com). This system replaces a $3K/month agency with automated data pipelines and AI-powered analysis.

## Working Style — Thought Partner Mode

Claude should act as a **strategic thought partner**, not just a code executor. This is especially important during planning and strategy discussions:

### Push Back & Challenge
- **Question assumptions** — if a proposed approach seems suboptimal, say so directly. Don't just build what's asked; challenge whether it's the right thing to build.
- **Suggest alternatives** — when given a task, proactively propose 2-3 approaches with trade-offs before jumping to implementation. Flag the one you'd recommend and why.
- **Call out blind spots** — if a plan ignores something important (attribution gaps, data quality issues, audience segmentation, competitive positioning), raise it unprompted.
- **Say "have you considered..."** — bring up related ideas the user might not be thinking about. If we're discussing Google Ads optimization, ask whether the Search Console data suggests an organic opportunity that might be cheaper.

### Think Holistically About Marketing & Performance
- **Cross-channel thinking** — never optimize one channel in isolation. Always consider how changes to Google Ads affect Meta, SEO, and overall CAC. Ask: "What does this do to blended ROAS?"
- **Full-funnel perspective** — connect top-of-funnel metrics (impressions, clicks) to bottom-of-funnel outcomes (orders, LTV). Push back if a plan optimizes for vanity metrics.
- **Seasonality & context** — consider whether performance changes are structural or seasonal. A dip in March might just be post-holiday normalization, not a campaign problem.
- **Unit economics** — always tie recommendations back to profitability. A 3x ROAS means nothing if COGS eats the margin. Think about contribution margin, not just revenue.
- **Competitive awareness** — when reviewing ad copy, SEO strategy, or positioning, think about what competitors (Bodhi Performance, UDrippin, Flick Weight) are likely doing and how to differentiate.

### AI-Powered Optimization Ideas
- Proactively suggest ways to use AI beyond what's currently built — predictive analytics, customer segmentation, dynamic pricing signals, automated A/B test analysis, churn prediction, cohort analysis.
- When reviewing performance data, don't just report numbers — generate hypotheses about *why* metrics moved and suggest experiments to validate them.
- Think about data the system *doesn't* collect yet that could unlock insights (e.g., weather data for seasonal products, competitor price tracking, review sentiment analysis).

### During Planning Specifically
- Use `EnterPlanMode` for anything non-trivial, and use that time to think broadly before narrowing.
- Before finalizing a plan, explicitly ask: "What are we missing?" and propose at least one thing the user probably hasn't considered.
- If the user's request is too narrow, widen the lens. "You asked about ad copy, but the landing page conversion rate is the bigger lever here."
- Frame trade-offs honestly — speed vs. quality, short-term revenue vs. long-term brand, automation vs. control.

## Recommended MCP Servers

MCPs that would add high-value capabilities to this workflow:

- **Google Sheets MCP** (`@anthropic/google-sheets-mcp`) — read/write campaign planning spreadsheets, content calendars, and performance dashboards directly. Useful for sharing reports with Braydon in a format he can interact with.
- **Slack MCP** (`@anthropic/slack-mcp`) — go beyond webhook notifications. Read channel history for context, post analysis summaries, respond to questions about performance directly in Slack.
- **Puppeteer/Browser MCP** (`@anthropic/puppeteer-mcp`) — audit live landing pages, screenshot competitor ads, verify published content looks right, check page speed and mobile rendering.
- **Sentry MCP** (`@anthropic/sentry-mcp`) — monitor pipeline errors in production. When the daily Cloud Run job fails, get structured error context without digging through logs.
- **GitHub MCP** (`@anthropic/github-mcp`) — manage issues, PRs, and project boards for tracking optimization proposals and content pipeline work.

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

# SEO content engine: identify what to write (all sites)
uv run python -m seo.run --opportunities --print

# SEO content engine: identify for a specific site
uv run python -m seo.run --opportunities --site shop --print

# SEO content engine: generate article draft (default: blog/WordPress)
uv run python -m seo.run --generate --type review --keyword "tungsten tape" --print

# SEO content engine: generate for Shopify shop blog
uv run python -m seo.run --generate --type how_to --keyword "paddle tape" --site shop --print

# SEO content engine: generate landing page for product (Shopify)
uv run python -m seo.run --generate --type landing_page --keyword "paddle tape" --product tungsten-tape --site shop

# SEO content engine: sync WordPress + Shopify blog content inventory
uv run python -m seo.run --sync-inventory

# SEO content engine: push drafts to WordPress/Shopify (auto-routes by site in frontmatter)
uv run python -m seo.run --publish-drafts

# SEO content engine: score published content performance
uv run python -m seo.run --score --print

# SEO content engine: full cycle
uv run python -m seo.run --all

# Optimization: review search terms for negative keywords
uv run python -m optimization.run --search-terms --print

# Optimization: create structured search term proposals (shadow mode)
uv run python -m optimization.run --search-terms --propose --print

# Optimization: budget reallocation recommendations
uv run python -m optimization.run --budget --print

# Optimization: create structured budget shift proposals (shadow mode)
uv run python -m optimization.run --budget --propose --print

# Optimization: shadow mode comparison report (system vs agency)
uv run python -m optimization.run --shadow-report --print

# Optimization: list pending action proposals
uv run python -m optimization.run --list-proposals

# Optimization: execute approved proposals
uv run python -m optimization.run --execute

# Optimization: full analysis cycle
uv run python -m optimization.run --all --print

# Optimization: full proposal cycle (search terms + budget, shadow mode)
uv run python -m optimization.run --all --propose --print
```

## Architecture

### Data Flow
Shopify API + Meta Ads API + Google Ads API + Search Console API (multi-site) → BigQuery tables (20) → Unified views (18) → Claude analysis → Content library + SEO content + Optimization proposals

### BigQuery Tables (ingested)
- `shopify_orders`, `shopify_order_line_items`, `shopify_products`, `shopify_product_variants`, `shopify_customers`
- `meta_campaigns`, `meta_adsets`, `meta_ads`, `meta_daily_insights`, `meta_creatives`
- `google_ads_campaigns`, `google_ads_ad_groups`, `google_ads_keywords`, `google_ads_daily_insights`, `google_ads_search_terms`
- `content_library`
- `search_console_performance` (Phase 4, multi-site with `site` column)
- `content_posts` (Phase 5)
- `google_ads_ad_copy` (Phase 6)
- `optimization_actions` (Phase 6)

### BigQuery Views (computed)
- Phase 1: `vw_daily_performance` (cross-platform), `vw_true_roas` (cross-platform), `vw_product_performance`, `vw_trends`
- Phase 2 (GA4): `vw_ga4_attribution`, `vw_enhanced_roas` (cross-platform), `vw_ga4_funnel`, `vw_ga4_product_insights`
- Phase 3 (Content): `vw_creative_performance`, `vw_component_scores`
- Google Ads: `vw_google_ads_keywords`, `vw_search_terms_waste`
- Phase 4 (SEO): `vw_seo_opportunities` (site-aware), `vw_seo_content_gaps` (site-aware), `vw_seo_trends` (site-aware), `vw_channel_summary`
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
- `seo/opportunities.py` — identifies content opportunities from Search Console + GA4 data (filterable by `--site`)
- `seo/generate.py` — AI-powered article/landing page generation with SEO rules (writes `site` to frontmatter)
- `seo/scorer.py` — scores published content against Search Console + GA4 performance
- `seo/wordpress/auth.py` — WordPress REST API authentication (Application Passwords)
- `seo/wordpress/publish.py` — creates draft posts in WordPress for review
- `seo/wordpress/inventory.py` — pulls existing WordPress content inventory
- `seo/shopify/pages.py` — creates landing pages on Shopify shop
- `seo/shopify/articles.py` — creates blog articles on Shopify shop
- `seo/shopify/inventory.py` — pulls existing Shopify blog article inventory
- `seo/templates/` — markdown templates for each content type (review, comparison, how_to, landing_page)
- `seo/drafts/` — generated drafts pending human review
- `config/seo_content.yaml` — content generation rules (word counts, required sections, SEO rules)

### Optimization Engine (Phase 6 + Shadow Mode)
- `optimization/search_terms.py` — AI reviews search terms, recommends negatives + keyword expansions; `review_and_propose()` creates structured proposals via Sonnet
- `optimization/budget.py` — cross-channel budget intelligence with product margin data; `recommend_and_propose()` creates guardrailed budget shift proposals
- `optimization/shadow_report.py` — compares system proposals vs actual agency changes
- `optimization/actions.py` — action proposal system with human approval gates (supports: `add_negative_keyword`, `add_as_keyword`, `adjust_bid`, `pause_keyword`, `shift_budget`)
- `optimization/proposals/` — JSON files for pending optimization proposals

### Search Console Integration (Phase 4, multi-site)
- `ingestion/search_console/auth.py` — OAuth2 for Search Console API, `get_site_urls()` returns all configured sites
- `ingestion/search_console/pull_performance.py` — pull query+page performance per site (includes `site` column)
- `ingestion/search_console/run.py` — orchestrator, loops over all configured site URLs

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
- **Search Console** now supports multiple sites — `search_console_performance` has a `site` column
- **Shopify blog** articles use `platform: "shopify_blog"` in `content_posts` (vs `"shopify"` for pages)
- **SEO `--site` flag**: `blog` = WordPress (pickleballeffect.com), `shop` = Shopify (pickleballeffectshop.com)
- **WordPress** requires Application Passwords for REST API auth

## .env Variables

```
# Existing
GCP_PROJECT_ID, ANTHROPIC_API_KEY
SHOPIFY_SHOP_DOMAIN, SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET
META_ADS_ACCOUNT_ID, META_APP_ID, META_APP_SECRET, META_ACCESS_TOKEN
GOOGLE_ADS_CUSTOMER_ID, GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_CLIENT_SECRET, GOOGLE_ADS_REFRESH_TOKEN, GOOGLE_ADS_LOGIN_CUSTOMER_ID

# Phase 4 (Search Console — multi-site)
GOOGLE_SEARCH_CONSOLE_SITE_URL           # Review site (pickleballeffect.com)
GOOGLE_SEARCH_CONSOLE_SITE_URL_SHOP      # Shop site (pickleballeffectshop.com)
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
