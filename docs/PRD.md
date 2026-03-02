# Pickleball Effect AI Marketing Platform — Product Requirements Document

> Internal reference. Last updated: March 2026.

---

## 1. Product Summary

An AI-powered marketing automation platform that replaces a $3K/month agency for Pickleball Effect — a pickleball paddle review site (pickleballeffect.com) with a Shopify accessory shop (pickleballeffectshop.com). The system ingests data from 4 ad/commerce platforms into BigQuery, deploys 18 unified views, runs Claude-powered analysis (weekly strategy + 8 real-time alerts), generates ad copy for Meta and Google Ads, produces SEO content for WordPress/Shopify, and proposes optimization actions with human approval gates. Total infrastructure cost: ~$20-50/month vs $3K/month agency — saving ~$2K/month after negotiating the agency down to website + email only.

---

## 2. System Architecture

### Data Flow

```
 Shopify API ─────┐
 Meta Ads API ────┤
 Google Ads API ──┼──► BigQuery (20 tables) ──► 18 Views ──► Claude Analysis
 Search Console ──┘                                              │
                                                    ┌────────────┼────────────┐
                                                    ▼            ▼            ▼
                                              Weekly Report  8 Alerts   Content Gen
                                              (Markdown)     (stdout)   (Meta/Google/SEO)
                                                                              │
                                                                              ▼
                                                                    Optimization Proposals
                                                                    (JSON, human-approved)
```

### Module Map

| Module | Path | Purpose |
|--------|------|---------|
| **Ingestion** | `ingestion/` | Pull data from 4 APIs → BigQuery tables |
| **Views** | `ingestion/views/` | 18 SQL views for unified analytics |
| **Analysis** | `ingestion/analysis/` | Claude-powered weekly strategy + alerts |
| **Content** | `content/` | Ad copy audit, generation (Meta + Google RSA), scoring |
| **SEO** | `seo/` | Content opportunities, article generation, WordPress/Shopify publishing |
| **Optimization** | `optimization/` | Search term hygiene, budget intelligence, action proposals |
| **Config** | `config/` | Brand voice, thresholds, SEO rules, account IDs |

### Tech Stack

- **Language:** Python 3.11+ (managed with `uv`)
- **Data warehouse:** BigQuery (`marketing_data` dataset, GCP project `practical-gecko-373320`)
- **AI model:** Claude Haiku 4.5 (`claude-haiku-4-5-20251001`) — ~$0.01/report, ~$0.05/content cycle
- **GA4 dataset:** `practical-gecko-373320.analytics_456683467` (date-sharded event tables)
- **Credentials:** `.env` file, loaded via `python-dotenv` with `override=True`

---

## 3. Capabilities

### 3.1 Data Pipeline (`ingestion/`)

Pulls data from 4 platform APIs into 20 BigQuery tables.

**Shopify** (5 tables): `shopify_orders`, `shopify_order_line_items`, `shopify_products`, `shopify_product_variants`, `shopify_customers`
- Orders with UTM attribution, line-level revenue, product catalog, variant pricing/inventory, customer LTV profiles
- ~92K customers, ~47 products ingested

**Meta Ads** (5 tables): `meta_campaigns`, `meta_adsets`, `meta_ads`, `meta_daily_insights`, `meta_creatives`
- Campaign/adset/ad configuration, daily performance metrics, creative text extraction from `object_story_spec`
- ~10 campaigns, ~430 creatives (29 linked to active ads)

**Google Ads** (5 tables): `google_ads_campaigns`, `google_ads_ad_groups`, `google_ads_keywords`, `google_ads_daily_insights`, `google_ads_search_terms`
- Campaign/ad group config, keyword targeting with quality scores, daily metrics, search term performance
- All money fields in micros (÷ 1,000,000), conversions as FLOAT64 (data-driven attribution)

**Search Console** (1 table): `search_console_performance`
- Query + page performance data (clicks, impressions, CTR, position)
- 2-3 day data lag — ingests 7 days back, ending 3 days ago

**Content & Optimization** (4 tables): `content_library`, `content_posts`, `google_ads_ad_copy`, `optimization_actions`
- Reusable ad copy components, SEO content tracking, RSA headline/description performance, optimization proposals with approval status

**Commands:**
```bash
uv run python -m ingestion.run_all --days-back 3              # Full pipeline
uv run python -m ingestion.run_all --shopify-only --days-back 3
uv run python -m ingestion.run_all --meta-only --days-back 3
uv run python -m ingestion.run_all --google-only --days-back 3
uv run python -m ingestion.run_all --search-console-only --days-back 7
uv run python -m ingestion.run_all --views-only               # Deploy views only
uv run python -m ingestion.run_all --setup                    # Create tables
uv run python -m ingestion.run_all --days-back 3 --analyze    # Ingest + analyze
uv run python -m ingestion.run_all --analyze-only             # Analysis only
```

### 3.2 Analytics Engine (`ingestion/analysis/` + `ingestion/views/`)

18 BigQuery views provide unified cross-platform analytics. Claude generates weekly strategy reports and checks 8 alert thresholds.

**Views by phase:**

| Phase | View | Purpose |
|-------|------|---------|
| 1 | `vw_daily_performance` | Unified daily metrics across Meta + Google Ads |
| 1 | `vw_true_roas` | Cross-platform ad spend vs Shopify revenue (blended ROAS) |
| 1 | `vw_product_performance` | Product sales by channel with CPA |
| 1 | `vw_trends` | 7-day/30-day rolling averages, WoW/DoD changes |
| 2 | `vw_ga4_attribution` | GA4 purchase events joined to Shopify orders |
| 2 | `vw_enhanced_roas` | GA4-based channel attribution (better than UTMs) |
| 2 | `vw_ga4_funnel` | Full purchase funnel by traffic source |
| 2 | `vw_ga4_product_insights` | Product-level views/carts/purchases from GA4 |
| 3 | `vw_creative_performance` | Creative text with ROAS/CTR/CPA + performance tiers |
| 3 | `vw_component_scores` | Component-level performance after 7+ days |
| GA | `vw_google_ads_keywords` | Keyword performance with quality scores and tiers |
| GA | `vw_search_terms_waste` | Zero-conversion search terms (negative keyword candidates) |
| 4 | `vw_seo_opportunities` | "Striking distance" keywords (position 5-20) |
| 4 | `vw_seo_content_gaps` | High impressions + low CTR pages |
| 4 | `vw_seo_trends` | Week-over-week ranking changes |
| 4 | `vw_channel_summary` | Daily rollup by channel (Meta/Google/Organic/Direct/Referral) |
| 5 | `vw_content_performance` | Published content with Search Console + GA4 metrics |
| 6 | `vw_google_ads_copy_performance` | RSA asset-level performance |

**8 Alerts** (defined in `alerts.py`, thresholds in `config/thresholds.yaml`):
1. ROAS floor breach (< 1.5)
2. CPA ceiling breach (> $50)
3. Spend anomaly (> 30% deviation from average)
4. Funnel conversion drop (> 20% vs 7-day average)
5. CTR decline (7 consecutive days)
6. Search term waste (> $20/week on zero-conversion terms)
7. Keyword quality score drop (below 5)
8. SEO ranking drop (> 5 positions lost)

**Commands:**
```bash
uv run python -m ingestion.analysis.run --weekly --print   # Weekly strategy report
uv run python -m ingestion.analysis.run --daily --print    # Daily report (available, not preferred)
uv run python -m ingestion.analysis.run --alerts           # Alert checks
uv run python -m ingestion.analysis.run --all --print      # All analysis
```

### 3.3 Content Machine (`content/`)

Audits existing ad creatives, generates new copy (Meta + Google Ads RSA), scores performance, and evolves the content library.

**Workflow:** Audit → Generate → Score → Evolve
- **Audit** (`audit.py`): Claude analyzes top/bottom performers, extracts reusable hooks/bodies/CTAs into `content/library/` CSVs + `content_library` BigQuery table
- **Generate** (`generator/generate.py`): AI generates new copy variations inspired by winning components. Meta: headline (40 char), primary text (125 char), CTA. Google RSA: headlines (30 char), descriptions (90 char)
- **Score** (`scorer/score.py`): Matches library components to live ads via `vw_component_scores`, promotes winners, retires losers
- **Output:** Generated copy saved to `content/generator/output/` for human review

**Commands:**
```bash
uv run python -m content.run --audit --print
uv run python -m content.run --generate --count 10
uv run python -m content.run --generate --platform google --count 10
uv run python -m content.run --generate --platform both --count 10
uv run python -m content.run --generate --product tungsten-tape --count 5
uv run python -m content.run --score --print
uv run python -m content.run --all                        # Full cycle
```

### 3.4 SEO Content Engine (`seo/`)

Identifies content opportunities from Search Console + GA4 data, generates articles/landing pages, publishes to WordPress/Shopify, and scores performance.

**Content types** (configured in `config/seo_content.yaml`):
- **Review:** 1,500-2,500 words. Sections: intro, specs, testing, pros/cons, verdict, FAQ
- **Comparison:** 1,200-2,000 words. Sections: intro, side-by-side, testing, verdict, FAQ
- **How-to:** 800-1,500 words. Sections: intro, steps, tips, FAQ
- **Landing page:** 300-800 words. Sections: hero, benefits, social proof, CTA

**SEO rules:** Title max 60 chars, meta description max 155 chars, keyword density 0.5-2%, grade 8 readability, image alt tags required.

**Human review gate:** Always required before publishing (`review_gate: always`).

**Commands:**
```bash
uv run python -m seo.run --opportunities --print
uv run python -m seo.run --generate --type review --keyword "tungsten tape" --print
uv run python -m seo.run --generate --type landing_page --keyword "paddle tape" --product tungsten-tape
uv run python -m seo.run --generate --type comparison --keyword "paddle comparison"
uv run python -m seo.run --generate --type how_to --keyword "how to tune paddle"
uv run python -m seo.run --sync-inventory
uv run python -m seo.run --publish-drafts
uv run python -m seo.run --score --print
uv run python -m seo.run --all                           # Full cycle
```

### 3.5 Optimization Engine (`optimization/`)

Analyzes search terms for waste, recommends budget reallocation, and proposes actions with human approval gates.

- **Search term hygiene** (`search_terms.py`): AI reviews search terms from `vw_search_terms_waste`, recommends negative keywords and keyword expansions
- **Budget intelligence** (`budget.py`): Cross-channel budget analysis using `vw_channel_summary` and `vw_true_roas`, recommends reallocation within guardrails (max 20% shift/day, min $10/campaign/day, min 7 days data)
- **Action proposals** (`actions.py`): Creates JSON proposals in `optimization/proposals/`, tracks approval workflow in `optimization_actions` table

**Autonomy levels** (from `config/thresholds.yaml`):
- **Auto-execute:** Pause ads with ROAS < 0.5 (losing money fast)
- **Requires approval:** Budget reallocation, pause campaign, launch new creative, keyword changes

**Commands:**
```bash
uv run python -m optimization.run --search-terms --print
uv run python -m optimization.run --budget --print
uv run python -m optimization.run --list-proposals
uv run python -m optimization.run --execute
uv run python -m optimization.run --all --print          # Full analysis
```

---

## 4. Operations & Cadence

| Cadence | What | How | Cost |
|---------|------|-----|------|
| **Daily** | Alert threshold checks | `--alerts` (8 checks) | Free when nothing triggers |
| **Weekly** (Sunday PM) | Full ingestion + strategy report | `--days-back 7` then `--weekly` | ~$0.01 (Claude) |
| **On-demand** | Content generation | `content.run --generate` | ~$0.05/cycle |
| **On-demand** | SEO draft generation | `seo.run --generate` | ~$0.02-0.05/article |
| **On-demand** | Optimization analysis | `optimization.run --all` | ~$0.01 |
| **Monthly** | Content scoring + library evolution | `content.run --score` | ~$0.01 |

**Current execution:** All CLI, manual invocation. No scheduler yet.

**Planned cadence:**
- Daily: Automated alert checks via Cloud Scheduler
- Weekly: Sunday evening full pipeline → Monday AM strategy review by Braydon
- On-demand: Content and optimization as needed

**Why weekly, not daily:** At ~10-15 orders/day, daily ROAS swings 50%+ from single orders. Weekly aggregation gives meaningful signal.

---

## 5. Success Metrics & Targets

| Metric | Target | Source | Status |
|--------|--------|--------|--------|
| Monthly infrastructure cost | < $50 | BigQuery + Claude API | On track (~$20-50) |
| Agency cost savings | > $2K/month | vs previous $3K agency | Pending agency negotiation |
| Blended ROAS (Meta + Google) | > 2.0x | `vw_true_roas` | Monitoring |
| Google Ads search term waste | < $20/week | `vw_search_terms_waste` | Alert active |
| CPA | < $50 | `alerts.py` threshold | Alert active |
| ROAS floor | > 1.5x | `alerts.py` threshold | Alert active |
| Weekly strategy delivery | Every Monday AM | `weekly_strategy.py` | Manual (CLI) |
| Content library growth | +10 components/month | `content_library` table | Active |
| SEO content published | 2-4 articles/month | `content_posts` table | Active |
| Optimization proposal turnaround | < 48 hours review | `optimization_actions` table | Manual tracking |
| Alert response time | Same-day for critical | `alerts.py` | Manual (stdout) |
| Keyword quality scores | All > 5 | `google_ads_keywords` | Alert active |
| Creative fatigue detection | Flag before CTR drops >30% | `vw_creative_performance` | 7-day CTR trend |
| Spend anomaly detection | Within 30% of average | `alerts.py` threshold | Alert active |

---

## 6. Data Model Summary

### Tables (20)

**Shopify (5)**
| Table | Key Columns | Notes |
|-------|-------------|-------|
| `shopify_orders` | order_id, order_number, total_price, utm_source/medium/campaign | order_id = GA4 transaction_id |
| `shopify_order_line_items` | order_id, product_id, variant_id, quantity, price | Line-level revenue |
| `shopify_products` | product_id, title, product_type, vendor | 47 products |
| `shopify_product_variants` | variant_id, product_id, price, inventory_quantity | Pricing + inventory |
| `shopify_customers` | customer_id, orders_count, total_spent | 92K customers, LTV data |

**Meta Ads (5)**
| Table | Key Columns | Notes |
|-------|-------------|-------|
| `meta_campaigns` | campaign_id, name, status, objective | Campaign config |
| `meta_adsets` | adset_id, campaign_id, targeting, budget | Ad set config |
| `meta_ads` | ad_id, adset_id, creative_id, status | Ad config |
| `meta_daily_insights` | date, campaign_id, spend, impressions, clicks, conversions | Daily metrics |
| `meta_creatives` | creative_id, headline, primary_text, cta | 430 creatives, 29 active |

**Google Ads (5)**
| Table | Key Columns | Notes |
|-------|-------------|-------|
| `google_ads_campaigns` | campaign_id, name, status, budget_micros | Budget in micros |
| `google_ads_ad_groups` | ad_group_id, campaign_id, type_, status | `type_` (trailing underscore) |
| `google_ads_keywords` | keyword_id, ad_group_id, keyword_text, quality_score | Quality scores tracked |
| `google_ads_daily_insights` | date, campaign_id, cost_micros, impressions, conversions | Conversions are FLOAT64 |
| `google_ads_search_terms` | search_term, campaign_id, impressions, cost_micros, conversions | Negative keyword source |

**Search Console (1)**
| Table | Key Columns | Notes |
|-------|-------------|-------|
| `search_console_performance` | query, page, clicks, impressions, ctr, position, date | 2-3 day data lag |

**Content & Optimization (4)**
| Table | Key Columns | Notes |
|-------|-------------|-------|
| `content_library` | component_type, text, source_ad_id, performance_tier | hooks, bodies, CTAs |
| `content_posts` | post_id, title, url, platform, status | WordPress/Shopify tracking |
| `google_ads_ad_copy` | headline, description, ad_group_id, performance_label | RSA assets |
| `optimization_actions` | action_type, proposal_json, status, approved_at | Approval workflow |

### Views (18)

**Phase 1 — Cross-Platform Performance (4)**
| View | Joins | Purpose |
|------|-------|---------|
| `vw_daily_performance` | meta_daily_insights + google_ads_daily_insights | Unified daily metrics |
| `vw_true_roas` | (meta + google) spend vs shopify_orders revenue | Blended ROAS |
| `vw_product_performance` | shopify_order_line_items + orders + UTM | Product CPA by channel |
| `vw_trends` | vw_daily_performance (self-join) | 7d/30d rolling averages |

**Phase 2 — GA4 Attribution (4)**
| View | Joins | Purpose |
|------|-------|---------|
| `vw_ga4_attribution` | GA4 purchase events + shopify_orders | Transaction matching |
| `vw_enhanced_roas` | GA4 attribution + ad spend | Channel-level true ROAS |
| `vw_ga4_funnel` | GA4 events (page_view → add_to_cart → purchase) | Funnel by source |
| `vw_ga4_product_insights` | GA4 product-scoped events | Product views/carts/purchases |

**Phase 3 — Content (2)**
| View | Joins | Purpose |
|------|-------|---------|
| `vw_creative_performance` | meta_creatives + meta_daily_insights + meta_ads | Creative ROAS/CTR/CPA |
| `vw_component_scores` | content_library + vw_creative_performance | Component scoring |

**Google Ads Intelligence (2)**
| View | Joins | Purpose |
|------|-------|---------|
| `vw_google_ads_keywords` | google_ads_keywords + daily_insights | Keyword tiers + quality |
| `vw_search_terms_waste` | google_ads_search_terms (0 conversions) | Waste identification |

**Phase 4 — SEO (4)**
| View | Joins | Purpose |
|------|-------|---------|
| `vw_seo_opportunities` | search_console_performance | Striking distance keywords |
| `vw_seo_content_gaps` | search_console_performance | High impressions, low CTR |
| `vw_seo_trends` | search_console_performance (self-join) | WoW ranking changes |
| `vw_channel_summary` | meta + google + GA4 + search console | Daily channel rollup |

**Phase 5 — Content Tracking (1)**
| View | Joins | Purpose |
|------|-------|---------|
| `vw_content_performance` | content_posts + search_console + GA4 | Published content ROI |

**Phase 6 — Google Ads Copy (1)**
| View | Joins | Purpose |
|------|-------|---------|
| `vw_google_ads_copy_performance` | google_ads_ad_copy + daily_insights | RSA asset performance |

### Key Relationships

- **GA4 `transaction_id` = Shopify `order_id`** (long numeric ID, NOT `order_number`)
- **UTM attribution:** Shopify orders capture `utm_source`, `utm_medium`, `utm_campaign` at checkout
- **Creative → Ad:** `meta_creatives.creative_id` links to `meta_ads.creative_id`
- **Content library → Ads:** Component text fuzzy-matched against active ad text for scoring
- **Search Console → Content posts:** URL matching for published content performance

---

## 7. Credential & Integration Status

| Integration | Status | Credential Type | Expiry | Notes |
|-------------|--------|-----------------|--------|-------|
| **GCP / BigQuery** | Active | Service account key | None | Project: `practical-gecko-373320` |
| **Google Ads** | Active | OAuth2 + developer token | Refresh token (long-lived) | Basic Access developer token |
| **Meta Ads** | Active | Long-lived access token | ~Apr 26, 2026 | Must refresh via `/oauth/access_token` before expiry |
| **Shopify** | Active | Legacy `shpat_` token | None (non-expiring) | Domain: `1cee15-1f.myshopify.com` |
| **Search Console** | Active | OAuth2 | Refresh token (long-lived) | Shares GCP OAuth client |
| **WordPress** | Active | Application Password | None | REST API auth for pickleballeffect.com |
| **Claude API** | Active | API key | None | platform.claude.com (NOT console.anthropic.com) |

**Action required:** Refresh Meta Ads token before April 26, 2026.

---

## 8. Next Horizon

Ordered by priority. Each item includes the trigger condition for when to implement.

### Immediate

1. **Scheduling & automation** — Cloud Scheduler for daily alert checks + weekly Sunday pipeline. Eliminates manual CLI invocation. _Trigger: Now (system is stable)._

2. **Notification delivery** — Email or Slack delivery for alerts and weekly reports. Currently output goes to stdout/files only. _Trigger: After scheduling is set up._

3. **Meta token auto-refresh** — Automated token refresh before 60-day expiry. _Trigger: Before Apr 26, 2026._

### Near-term

4. **API execution** — Push approved optimization proposals to Google Ads / Meta APIs (currently propose-only). Budget changes, negative keywords, ad pause/unpause. _Trigger: After 4+ weeks of reliable proposals with good approval rate._

5. **Agency transition** — Negotiate agency to $1K/month (website + email only). Run AI-managed ads in parallel for 2-4 weeks, then cut over. _Trigger: After scheduling + notifications + 1 month of autonomous operation._

### When scale warrants

6. **Video creative intelligence** — Full video ad analysis: audio transcription (Whisper), frame extraction (ffmpeg), visual pattern analysis (Claude multimodal). See `docs/creative_intelligence_system.md`. _Trigger: Active video creatives > 100 AND monthly spend > $10K._

7. **Customer LTV optimization** — Use Shopify repeat buyer data to optimize for lifetime value, not just first-purchase ROAS. _Trigger: Sufficient repeat purchase data (6+ months of tracking)._

8. **New platforms** — TikTok Ads, YouTube Ads when relevant to pickleball audience. _Trigger: Platform reaches meaningful pickleball audience._

---

## 9. Known Gaps & Risks

| Gap | Impact | Mitigation |
|-----|--------|------------|
| No automated scheduling | All CLI today; requires manual invocation | Next horizon #1: Cloud Scheduler |
| No notification delivery | Reports go to stdout/files, not email/Slack | Next horizon #2: notification layer |
| Meta token expiry (~Apr 26, 2026) | Meta ingestion stops if not refreshed | Calendar reminder + future auto-refresh |
| Propose-only optimization | Can't execute actions via API yet | Human applies changes manually in ad platforms |
| Minimal test coverage | 3 test files (`test_shopify_auth.py`, `test_meta_auth.py`, `test_utm_parsing.py`) | Add integration tests as system stabilizes |
| No system self-monitoring | Silent failures possible (e.g., ingestion fails, nobody knows) | Add health checks + failure notifications |
| Small data scale | 10-15 orders/day limits statistical confidence | Weekly aggregation, conservative thresholds, grow over time |
| No COGS data | True profit margin unknown (ROAS ≠ profit) | Shopify product costs or manual input needed |

---

## 10. Config Reference

### Environment Variables (23)

```
# Core
GCP_PROJECT_ID                          # practical-gecko-373320
GCP_REGION                              # us-central1
ANTHROPIC_API_KEY                       # Claude API key (platform.claude.com)

# Shopify
SHOPIFY_SHOP_DOMAIN                     # 1cee15-1f.myshopify.com
SHOPIFY_CLIENT_ID
SHOPIFY_CLIENT_SECRET

# Meta Ads
META_ADS_ACCOUNT_ID                     # act_xxxxx format
META_APP_ID
META_APP_SECRET
META_ACCESS_TOKEN                       # Long-lived, expires ~Apr 26, 2026

# Google Ads
GOOGLE_ADS_CUSTOMER_ID                  # 123-456-7890 format
GOOGLE_ADS_DEVELOPER_TOKEN
GOOGLE_ADS_CLIENT_ID
GOOGLE_ADS_CLIENT_SECRET
GOOGLE_ADS_REFRESH_TOKEN
GOOGLE_ADS_LOGIN_CUSTOMER_ID            # MCC account ID (if applicable)

# Search Console
GOOGLE_SEARCH_CONSOLE_SITE_URL
GOOGLE_SEARCH_CONSOLE_CLIENT_ID
GOOGLE_SEARCH_CONSOLE_CLIENT_SECRET
GOOGLE_SEARCH_CONSOLE_REFRESH_TOKEN

# WordPress
WORDPRESS_URL
WORDPRESS_USER
WORDPRESS_APP_PASSWORD
```

### Config Files

| File | Purpose | Key Contents |
|------|---------|--------------|
| `config/brand.yaml` | Brand identity | Voice (direct, authentic, no-BS), ICP (3 segments), 7 products, 3 competitors, tagline |
| `config/thresholds.yaml` | Alert & optimization rules | ROAS floor (1.5), CPA ceiling ($50), spend anomaly (30%), budget guardrails, autonomy levels |
| `config/seo_content.yaml` | SEO generation rules | 4 content types with word counts, required sections, SEO rules (title 60 chars, density 0.5-2%) |
| `config/accounts.yaml` | Account ID template | Points to GCP Secret Manager for sensitive values |

### Output Locations

| Output | Path | Gitignored |
|--------|------|------------|
| Analysis reports | `reports/` | Yes |
| Generated ad copy | `content/generator/output/` | Yes |
| SEO drafts | `seo/drafts/` | Yes |
| Optimization proposals | `optimization/proposals/` | Yes |
| Content library CSVs | `content/library/` | No (tracked) |
