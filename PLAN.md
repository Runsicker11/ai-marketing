# AI Marketing Automation - Master Plan
# Pickleball Effect (pickleballeffectshop.com)

## Current State
- **Business:** Pickleball equipment reviews (pickleballeffect.com) + accessory shop (pickleballeffectshop.com, Shopify)
- **Agency cost:** $3,000/month (email, website, ads management)
- **Ad spend:** ~$2,000/month (Google Ads + Meta)
- **Ad goal:** Drive purchases on Shopify shop
- **Problem:** Not profitable with current agency expense
- **Competitors:** Bodhi Performance, UDrippin, Flick Weight

## Target State
- Agency: $1,000/month (website updates + email only)
- Ad management: AI-powered via GCP (Google Cloud)
- Ad spend: Same ~$2,000/month, managed smarter with full visibility
- Net savings: ~$2,000/month + better performance
- Full data pipeline: Ads → BigQuery ← Shopify (close the loop from click to purchase)

---

## Phase 0: Access & Credentials -- COMPLETE (Feb 2026)

### 0.1 Gather API Access
- [x] **Google Ads:** Applied for developer token (PENDING basic access approval)
- [x] **Meta Ads:** Long-lived token obtained, expires ~Apr 26, 2026
- [x] **Shopify:** Legacy custom app token (shpat_, non-expiring)
- [x] **Claude:** API key from platform.claude.com
- [x] **GCP:** Project practical-gecko-373320, billing enabled

### 0.2 Store Credentials Securely
- [x] Stored in `.env` file (gitignored), loaded via python-dotenv

### 0.3 Brand Guide
- [x] `config/brand.yaml` populated with voice, ICP, competitors, products

---

## Phase 1: Data Pipeline -- COMPLETE (Feb 25, 2026)

- [x] GCP project setup (practical-gecko-373320, BigQuery dataset `marketing_data`)
- [x] Google Ads → BigQuery: BLOCKED (developer token pending)
- [x] Meta Ads → BigQuery: 10 campaigns, daily insights ingested
- [x] Shopify → BigQuery: 92K customers, 47 products, orders with line items
- [x] 4 unified views deployed: `vw_daily_performance`, `vw_true_roas`, `vw_product_performance`, `vw_trends`
- [x] Run with: `uv run python -m ingestion.run_all --days-back 3`

---

## Phase 2: GA4 Integration + AI Analysis -- COMPLETE (Feb 25, 2026)

- [x] 4 GA4 views: `vw_ga4_attribution`, `vw_enhanced_roas`, `vw_ga4_funnel`, `vw_ga4_product_insights`
- [x] GA4 attribution fills UTM gap: 530 purchase events matched, 154 Shopify orders attributed
- [x] Claude analysis module: `ingestion/analysis/` (Haiku 4.5, ~$0.01/report)
- [x] Alert system: 5 threshold checks (ROAS floor, CPA ceiling, spend anomaly, funnel drop, CTR decline)
- [x] Weekly strategy report generator
- [x] Daily report available but **weekly cadence preferred** (10-15 orders/day too noisy for daily)

### Cadence Decision
- **Daily:** Alert threshold checks only (free when nothing triggers)
- **Weekly (Sunday):** Full Claude strategy report for Monday review
- Rationale: At ~10-15 orders/day, daily ROAS swings 50%+ from single orders. Weekly aggregation gives meaningful signal.

---

## Phase 3: Content Machine -- COMPLETE (Feb 25, 2026)

- [x] Pull creative text from Meta API (headline, primary text, CTA from `object_story_spec`)
- [x] `meta_creatives` table: 430 creatives ingested, 29 linked to active ads
- [x] `vw_creative_performance` view: creative text joined with ROAS, CTR, CPA + performance tiers
- [x] Content audit module: Claude analyzes top/bottom performers, extracts reusable components
- [x] Content library: `content/library/` CSVs + `content_library` BigQuery table
- [x] AI copy generator: produces new hooks/bodies inspired by winners, validates char limits
- [x] Scoring feedback loop: matches library components to live ads, promotes winners, retires losers
- [x] `vw_component_scores` view for automated scoring
- [x] CLI: `uv run python -m content.run --audit|--generate|--score|--all`
- [x] Cost: ~$0.05 per full audit+generate cycle

---

## Phase 3.5: Video Creative Intelligence (Planned)

Braydon films all ad videos. Phase 3 only analyzes the **text wrapper** (headline, primary text, CTA). This phase adds analysis of the **video content itself** to understand why certain creatives win.

### 3.5.1 Video Download Pipeline
- [ ] Pull video download URLs from Meta API (`GET /{video_id}?fields=source`)
- [ ] Download and cache MP4 files locally for analysis
- [ ] Track which videos are already downloaded to avoid re-fetching
- [ ] Research: API rate limits, video storage size, retention policy

### 3.5.2 Audio Transcription
- [ ] Extract audio from video files (ffmpeg)
- [ ] Transcribe with Whisper (OpenAI API or local model)
- [ ] Store transcripts alongside creative data in BigQuery
- [ ] Capture: what Braydon says, verbal hooks, tone, pacing

### 3.5.3 Visual Analysis
- [ ] Extract key frames from videos (ffmpeg: thumbnail, 3s mark, midpoint, end)
- [ ] Send frames to Claude (multimodal) for visual pattern analysis
- [ ] Classify: format type (talking head, on-court demo, product close-up, unboxing)
- [ ] Identify: visual hook (first 3 seconds), product presentation style, pacing

### 3.5.4 Creative Pattern Analysis
- [ ] Correlate video content patterns with performance data
- [ ] Build a creative playbook: which formats, openings, demos drive ROAS
- [ ] Example insights:
  - "On-court demos in first 3 seconds → 2.5x higher ROAS than talking-head opens"
  - "Videos under 20 seconds outperform longer ones by 1.8x"
  - "Mentioning a specific stat in the first line boosts CTR 40%"
- [ ] Feed playbook into content generation for more targeted copy suggestions

### 3.5.5 Creative Recommendations
- [ ] Suggest video + copy pairings based on what's worked
- [ ] Recommend new video concepts based on winning patterns
- [ ] Flag creative fatigue: same video running too long with declining CTR

### Cost Estimate
| Component | Per-video cost | 30 videos/cycle |
|-----------|---------------|-----------------|
| Video download | Free | Free |
| Whisper transcription | ~$0.003 | ~$0.09 |
| Frame extraction (ffmpeg) | Free | Free |
| Claude multimodal analysis | ~$0.02-0.05 | ~$0.60-1.50 |
| **Total** | | **~$1-2/cycle** |
| Monthly (weekly cadence) | | **~$4-8/month** |

### Open Questions
- [ ] How much local storage needed for video cache?
- [ ] Use OpenAI Whisper API vs. local whisper model? (cost vs. dependency tradeoff)
- [ ] How far back to analyze? All historical videos or just last 90 days?
- [ ] Can we detect on-screen text/overlays in addition to audio?
- [ ] Integration with Phase 3 content generator: should video insights directly inform copy generation prompts?

### Phase 3.5 Exit Criteria
> You have a creative playbook derived from actual video performance data.
> You know which video formats, openings, and styles drive the best ROAS.
> Content generation is informed by both text AND video performance patterns.

---

## Phase 4: Automated Optimization (Weeks 7-10)

### 4.1 Budget Recommendations
- [ ] Claude analyzes cross-platform + Shopify data
- [ ] Recommends budget shifts based on TRUE ROAS (not platform estimates)
- [ ] Rules: max 20% shift per day, minimum spend per channel, ROAS floor
- [ ] You approve/reject via email

### 4.2 Creative Rotation
- [ ] Detect creative fatigue (CTR declining over 7+ days)
- [ ] Auto-suggest replacement ads from content library
- [ ] You approve, then system pushes to platform via API

### 4.3 Google Ads Automation
- [ ] Negative keyword management (analyze search terms, suggest negatives)
- [ ] Bid adjustment recommendations
- [ ] RSA headline/description optimization
- [ ] Shopping campaign optimization (if applicable)

### 4.4 Meta Ads Automation
- [ ] Audience performance analysis
- [ ] Ad set budget optimization (CBO vs ABO recommendations)
- [ ] Placement performance analysis (Feed vs Stories vs Reels)
- [ ] Lookalike audience suggestions based on Shopify customer data

### 4.5 Product-Level Intelligence
- [ ] Which products are most profitable to advertise? (revenue - COGS - ad cost)
- [ ] Auto-prioritize ad spend toward highest-margin products
- [ ] Seasonal trends: do certain products sell better at certain times?

### Phase 4 Exit Criteria
> AI handles day-to-day optimization recommendations.
> You spend 30 min/day reviewing and approving vs. relying on agency.
> Performance is equal to or better than agency-managed period.

---

## Phase 5: Autonomous Operation (Weeks 11+)

### 5.1 Graduated Autonomy
- [ ] High-confidence actions auto-execute (pause ads with ROAS < 0.5)
- [ ] Medium-confidence actions go to you for approval
- [ ] All actions logged and reversible
- [ ] Dashboard showing what the AI did and why

### 5.2 Full Agency Transition
- [ ] Negotiate agency down to $1,000/month (website + email only)
- [ ] Run parallel: AI-managed + agency-managed for 2-4 weeks
- [ ] Cut over fully when confident

### 5.3 Continuous Improvement
- [ ] Monthly strategy review with Claude (full data analysis + market trends)
- [ ] Quarterly content library refresh
- [ ] Expand to new platforms if relevant (TikTok, YouTube Ads)
- [ ] Customer retention: use Shopify data to identify repeat buyers and optimize for LTV

---

## Data Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Google Ads  │     │   Meta Ads   │     │   Shopify    │
│    API       │     │    API       │     │    API       │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │ (native           │ (Cloud              │ (Cloud
       │  Data Transfer)   │  Function)          │  Function)
       ▼                   ▼                     ▼
┌─────────────────────────────────────────────────────────┐
│                    BigQuery                              │
│                                                         │
│  google_ads.*    meta_ads.*    shopify.*                │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │           Unified Views                          │   │
│  │  vw_daily_performance                           │   │
│  │  vw_true_roas  (ad spend vs shopify revenue)    │   │
│  │  vw_product_performance                         │   │
│  │  vw_trends                                      │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
            ┌──────────────────┐
            │  Cloud Functions │
            │                  │
            │  daily_report    │──→ Email/Slack
            │  alert_checker   │──→ Email/Slack
            │  weekly_summary  │──→ Email/Slack
            │  content_scorer  │
            │  optimizer       │──→ Google/Meta APIs
            └────────┬─────────┘
                     │
                     ▼
              ┌────────────┐
              │ Claude API │
              │ (analysis) │
              └────────────┘
```

## Reference

### API Credentials Needed
| Platform | What You Need | Where to Get It |
|----------|---------------|-----------------|
| Google Ads | Developer token + OAuth credentials | Google Ads API Center |
| Meta Ads | App ID + Access token | Meta Business Suite > Marketing API |
| Shopify | Admin API access token | Shopify Admin > Settings > Apps > Develop apps |
| Claude | API key | console.anthropic.com |
| GCP | Service account key | GCP Console > IAM |

### Key Config Files
- `config/brand.yaml` - Brand voice, competitors, ICP, products
- `config/accounts.yaml` - Account IDs, project IDs
- `config/thresholds.yaml` - ROAS targets, spend limits, alert rules

### Cost Estimate
| Item | Monthly Cost |
|------|-------------|
| BigQuery | ~$0 (first 1TB query free) |
| Cloud Functions | ~$0 (first 2M invocations free) |
| Cloud Scheduler | ~$0 (3 free jobs) |
| Claude API | ~$20-50 |
| **Total** | **~$20-50/month** |
| **Current agency** | **$3,000/month** |
| **Savings** | **~$2,950/month** |
