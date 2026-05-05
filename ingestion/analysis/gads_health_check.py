"""Google Ads account health check — 30 best-practice rules against BigQuery.

Runs Monday 8 AM UTC via a dedicated Cloud Scheduler trigger → daily-analysis job.
Outputs a Markdown report to reports/gads_health_YYYY-MM-DD.md and posts a
Slack summary with ✅/⚠️/🔴 status per category.

Rules are grouped into 6 categories (5 rules each):
  1. Conversion config (account hygiene)
  2. Campaign performance
  3. Search term waste
  4. Keyword quality
  5. Shopping performance
  6. Bidding / budget / structural
"""

from datetime import date, timedelta
from pathlib import Path

import yaml

from ingestion.analysis.claude_client import analyze
from ingestion.utils.bq_client import run_query
from ingestion.utils.config import GCP_PROJECT_ID
from ingestion.utils.logger import get_logger

log = get_logger(__name__)

_REPORTS_DIR = Path(__file__).resolve().parents[2] / "reports"
_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"
_RAW = f"{GCP_PROJECT_ID}.raw_google_ads"

_STATUS_EMOJI = {"ok": "✅", "warn": "⚠️", "crit": "🔴"}
_SEVERITY_MAP = {"ok": "low", "warn": "medium", "crit": "high"}

SYSTEM_PROMPT = """\
You are a Google Ads performance auditor for Pickleball Effect, a DTC pickleball \
accessories brand. The account has two active campaigns: Brand (Search, Maximize \
Conversion Value, $30/day, campaign-specific Purchases-only goal) and Shopping \
(Target ROAS 280%, $100/day). North-star goal: 3.0x blended ROAS on $2,800/month. \
When health-check rules fire, explain the likely root cause and give specific, \
prioritized actions. Be concise. Reference the actual numbers. Flag critical issues \
first. Known context: Page view + Add to cart are still flagged include_in_conversions_metric=true \
at account level — scheduled fix around May 12 after GA4 Purchase is promoted to Primary.\
"""


def _cfg() -> dict:
    path = _CONFIG_DIR / "thresholds.yaml"
    return yaml.safe_load(path.read_text(encoding="utf-8")).get("gads_health", {})


def _finding(
    rule_id: str,
    status: str,
    finding: str,
    dollar_impact: float = 0.0,
    action: str = "",
) -> dict:
    return {
        "rule_id": rule_id,
        "status": status,
        "finding": finding,
        "dollar_impact": dollar_impact,
        "action": action,
        # alerts.py-compatible keys for format_alert_summary
        "type": rule_id,
        "severity": _SEVERITY_MAP[status],
        "message": finding,
    }


# ── CATEGORY 1: Conversion Config ────────────────────────────────────────


def _check_conversion_polluters() -> list[dict]:
    """Non-purchase conversion actions with include_in_conversions_metric=true pollute bidding."""
    sql = f"""
    SELECT conversion_action_name, CAST(conversion_action_id AS STRING) AS cid
    FROM `{_RAW}.conversion_action`
    WHERE status = 'ENABLED'
        AND LOWER(conversion_action_name) NOT LIKE '%purchase%'
        AND include_in_conversions_metric = true
    ORDER BY conversion_action_name
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("CONVERSION_POLLUTERS: query failed", exc_info=True)
        return [_finding("CONVERSION_POLLUTERS", "warn",
                         "Could not query conversion_action table",
                         action="Verify raw_google_ads.conversion_action is populated")]

    if not rows:
        return [_finding("CONVERSION_POLLUTERS", "ok",
                         "No non-purchase actions polluting bidding signals")]

    names = ", ".join(r.conversion_action_name for r in rows)
    return [_finding(
        "CONVERSION_POLLUTERS", "crit",
        f"{len(rows)} non-purchase action(s) with include_in_conversions_metric=true: {names}. "
        "Inflates conversions column for all campaigns on account defaults.",
        action="Scheduled fix ~May 12: after re-promoting GA4 Purchase to Primary, "
               "flip include_in_conversions_metric=false for Page view and Add to cart.",
    )]


def _check_purchase_conv_enabled() -> list[dict]:
    """At least one Purchase conversion action must be ENABLED and in bidding."""
    sql = f"""
    SELECT conversion_action_name, status, include_in_conversions_metric
    FROM `{_RAW}.conversion_action`
    WHERE LOWER(conversion_action_name) LIKE '%purchase%'
        AND status = 'ENABLED'
    ORDER BY conversion_action_name
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("PURCHASE_CONV_ENABLED: query failed", exc_info=True)
        return [_finding("PURCHASE_CONV_ENABLED", "warn",
                         "Could not check purchase conversion actions")]

    if not rows:
        return [_finding(
            "PURCHASE_CONV_ENABLED", "crit",
            "No ENABLED Purchase conversion action found. Campaigns have no purchase signal.",
            action="Re-enable a Purchase conversion action immediately.",
        )]

    in_bidding = [r for r in rows if r.include_in_conversions_metric]
    if not in_bidding:
        names = ", ".join(r.conversion_action_name for r in rows)
        return [_finding(
            "PURCHASE_CONV_ENABLED", "warn",
            f"Purchase action(s) ENABLED but none have include_in_conversions_metric=true: {names}. "
            "Brand campaign override covers it only if campaign-specific goal is set.",
            action="Verify Brand campaign has Purchases-only campaign-specific goal. "
                   "Promote GA4 Purchase to Primary ~May 12.",
        )]

    return [_finding("PURCHASE_CONV_ENABLED", "ok",
                     f"{len(in_bidding)} Purchase action(s) active in bidding")]


def _check_conv_action_count() -> list[dict]:
    """Too many conversion actions in bidding dilutes the Purchase signal."""
    sql = f"""
    SELECT COUNT(*) AS cnt
    FROM `{_RAW}.conversion_action`
    WHERE status = 'ENABLED'
        AND include_in_conversions_metric = true
    """
    try:
        rows = list(run_query(sql))
        cnt = rows[0].cnt if rows else 0
    except Exception:
        log.warning("CONV_ACTION_COUNT: query failed", exc_info=True)
        return [_finding("CONV_ACTION_COUNT", "warn",
                         "Could not count conversion actions in bidding")]

    if cnt > 4:
        return [_finding(
            "CONV_ACTION_COUNT", "warn",
            f"{cnt} conversion actions flagged include_in_conversions_metric=true. "
            "High count dilutes the Purchase signal Smart Bidding optimises toward.",
            action="Audit conversion actions. Only Purchase variants should have include_in_conversions_metric=true.",
        )]
    return [_finding("CONV_ACTION_COUNT", "ok",
                     f"{cnt} conversion actions in bidding — within normal range")]


def _check_default_value_low() -> list[dict]:
    """Purchase action using always_use_default_value with a suspiciously low default."""
    sql = f"""
    SELECT conversion_action_name, default_value
    FROM `{_RAW}.conversion_action`
    WHERE always_use_default_value = true
        AND CAST(default_value AS FLOAT64) < 15
        AND status = 'ENABLED'
        AND LOWER(conversion_action_name) LIKE '%purchase%'
    ORDER BY default_value
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("DEFAULT_VALUE_LOW: query failed", exc_info=True)
        return [_finding("DEFAULT_VALUE_LOW", "warn",
                         "Could not check conversion default values")]

    if not rows:
        return [_finding("DEFAULT_VALUE_LOW", "ok",
                         "No Purchase actions with suspiciously low default conversion values")]

    names = ", ".join(
        f"{r.conversion_action_name} (${r.default_value:.2f})" for r in rows
    )
    return [_finding(
        "DEFAULT_VALUE_LOW", "warn",
        f"Purchase action(s) using fixed default value < $15: {names}. "
        "Underreports revenue to Smart Bidding tROAS.",
        action="Update default_value to match actual avg order value (~$25), "
               "or configure the pixel to send dynamic purchase values.",
    )]


def _check_data_freshness() -> list[dict]:
    """Most recent daily_insights date must be within 2 days."""
    sql = f"SELECT MAX(date_start) AS latest FROM `{_RAW}.daily_insights`"
    try:
        rows = list(run_query(sql))
        latest = rows[0].latest if rows else None
    except Exception:
        log.warning("DATA_FRESHNESS: query failed", exc_info=True)
        return [_finding("DATA_FRESHNESS", "crit",
                         "daily_insights table unreachable — pipeline may be down",
                         action="Check pipeline-google-ads Cloud Run job logs in GCP console")]

    if latest is None:
        return [_finding("DATA_FRESHNESS", "crit",
                         "daily_insights table is empty",
                         action="Run pipeline-google-ads job manually")]

    days_stale = (date.today() - latest).days
    if days_stale > 2:
        return [_finding(
            "DATA_FRESHNESS", "crit",
            f"Most recent data is {latest} — {days_stale} day(s) old.",
            action="Check pipeline-google-ads Cloud Run job logs in GCP console.",
        )]
    return [_finding("DATA_FRESHNESS", "ok",
                     f"Data current through {latest} ({days_stale}d lag)")]


# ── CATEGORY 2: Campaign Performance ─────────────────────────────────────


def _check_roas_below_target(roas_floor: float, min_spend: float) -> list[dict]:
    """Campaign 7-day ROAS below target floor."""
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    sql = f"""
    SELECT campaign_name,
        SAFE_DIVIDE(SUM(conversion_value), SUM(spend)) AS roas,
        SUM(spend) AS total_spend,
        SUM(conversion_value) AS total_rev
    FROM `{_RAW}.daily_insights`
    WHERE date_start >= '{cutoff}'
    GROUP BY campaign_id, campaign_name
    HAVING SUM(spend) >= {min_spend}
        AND (SUM(conversion_value) = 0
             OR SAFE_DIVIDE(SUM(conversion_value), SUM(spend)) < {roas_floor})
    ORDER BY total_spend DESC
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("ROAS_BELOW_TARGET: query failed", exc_info=True)
        return [_finding("ROAS_BELOW_TARGET", "warn", "Could not compute campaign ROAS")]

    findings = []
    for r in rows:
        roas = r.roas or 0.0
        findings.append(_finding(
            "ROAS_BELOW_TARGET",
            "crit" if roas < 1.5 else "warn",
            f"{r.campaign_name}: 7-day ROAS {roas:.2f}x (floor {roas_floor:.1f}x). "
            f"Spend ${r.total_spend:.2f}, Revenue ${r.total_rev:.2f}.",
            dollar_impact=r.total_spend,
            action="Review search terms for waste. Check conversion tracking. "
                   "If Brand: verify learning window — do not change bids for 7+ days post-override.",
        ))

    if not findings:
        return [_finding("ROAS_BELOW_TARGET", "ok",
                         f"All campaigns at or above {roas_floor:.1f}x ROAS floor")]
    return findings


def _check_zero_conv_high_spend(min_spend: float, lookback_days: int) -> list[dict]:
    """Campaign with high spend and zero conversions in lookback window."""
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    sql = f"""
    SELECT campaign_name, SUM(spend) AS total_spend
    FROM `{_RAW}.daily_insights`
    WHERE date_start >= '{cutoff}'
    GROUP BY campaign_id, campaign_name
    HAVING SUM(spend) >= {min_spend} AND SUM(conversions) = 0
    ORDER BY total_spend DESC
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("ZERO_CONV_HIGH_SPEND: query failed", exc_info=True)
        return [_finding("ZERO_CONV_HIGH_SPEND", "warn",
                         "Could not check zero-conversion campaigns")]

    if not rows:
        return [_finding("ZERO_CONV_HIGH_SPEND", "ok",
                         f"No campaigns with ${min_spend}+ spend and 0 conv in {lookback_days}d")]

    findings = []
    for r in rows:
        findings.append(_finding(
            "ZERO_CONV_HIGH_SPEND", "crit",
            f"{r.campaign_name}: ${r.total_spend:.2f} spend in {lookback_days} days, 0 conversions.",
            dollar_impact=r.total_spend,
            action="Check conversion tracking integrity. Verify campaign-specific goal is set. "
                   "Review search terms for irrelevant traffic.",
        ))
    return findings


def _check_spend_anomaly_campaign(pct: float) -> list[dict]:
    """Yesterday's campaign spend deviates ≥ pct% from 7-day average."""
    sql = f"""
    WITH daily AS (
        SELECT campaign_name, date_start, SUM(spend) AS spend
        FROM `{_RAW}.daily_insights`
        WHERE date_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)
        GROUP BY campaign_name, date_start
    ),
    summary AS (
        SELECT campaign_name,
            AVG(CASE WHEN date_start < DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                     THEN spend END) AS avg_spend,
            SUM(CASE WHEN date_start = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                     THEN spend END) AS yesterday
        FROM daily
        GROUP BY campaign_name
    )
    SELECT campaign_name, yesterday, avg_spend,
        SAFE_DIVIDE(ABS(yesterday - avg_spend), avg_spend) AS deviation
    FROM summary
    WHERE avg_spend > 1
        AND yesterday IS NOT NULL AND yesterday > 0
        AND SAFE_DIVIDE(ABS(yesterday - avg_spend), avg_spend) > {pct / 100.0}
    ORDER BY deviation DESC
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("SPEND_ANOMALY: query failed", exc_info=True)
        return [_finding("SPEND_ANOMALY", "warn", "Could not check spend anomalies")]

    findings = []
    for r in rows:
        direction = "above" if r.yesterday > r.avg_spend else "below"
        findings.append(_finding(
            "SPEND_ANOMALY", "warn",
            f"{r.campaign_name}: yesterday ${r.yesterday:.2f} is {r.deviation:.0%} "
            f"{direction} 7-day avg ${r.avg_spend:.2f}.",
            action="Check for budget changes, bid strategy adjustments, or unusual auction activity.",
        ))

    if not findings:
        return [_finding("SPEND_ANOMALY", "ok", "No campaign spend anomalies vs 7-day avg")]
    return findings


def _check_impression_share_brand(is_floor: float) -> list[dict]:
    """Brand campaign search impression share below floor over trailing 7 days."""
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    sql = f"""
    SELECT campaign_name,
        AVG(search_impression_share) AS avg_is,
        MIN(search_impression_share) AS min_is
    FROM `{_RAW}.daily_insights`
    WHERE date_start >= '{cutoff}'
        AND LOWER(campaign_name) LIKE '%brand%'
        AND search_impression_share IS NOT NULL
        AND search_impression_share > 0
    GROUP BY campaign_name
    HAVING AVG(search_impression_share) < {is_floor}
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("IMPRESSION_SHARE_LOW: query failed", exc_info=True)
        return [_finding("IMPRESSION_SHARE_LOW", "warn",
                         "Could not check Brand impression share")]

    if not rows:
        return [_finding("IMPRESSION_SHARE_LOW", "ok",
                         f"Brand impression share at or above {is_floor:.0%} floor")]

    findings = []
    for r in rows:
        findings.append(_finding(
            "IMPRESSION_SHARE_LOW", "warn",
            f"{r.campaign_name}: avg search IS {r.avg_is:.0%}, min {r.min_is:.0%} "
            f"(floor {is_floor:.0%}) over last 7 days.",
            action="Increase Brand budget, or audit keyword QS to lift auction eligibility.",
        ))
    return findings


def _check_active_zero_impressions() -> list[dict]:
    """ENABLED campaign with 0 impressions yesterday (billing, policy, or targeting issue)."""
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    sql = f"""
    SELECT c.campaign_name, c.campaign_type,
        CAST(c.budget_amount AS FLOAT64) AS budget_amount
    FROM `{_RAW}.campaigns` c
    LEFT JOIN (
        SELECT campaign_id, SUM(impressions) AS imps
        FROM `{_RAW}.daily_insights`
        WHERE date_start = '{yesterday}'
        GROUP BY campaign_id
    ) d ON c.campaign_id = d.campaign_id
    WHERE c.status = 'ENABLED'
        AND (d.imps IS NULL OR d.imps = 0)
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("ACTIVE_ZERO_IMPRESSIONS: query failed", exc_info=True)
        return [_finding("ACTIVE_ZERO_IMPRESSIONS", "warn",
                         "Could not check active campaign impression counts")]

    if not rows:
        return [_finding("ACTIVE_ZERO_IMPRESSIONS", "ok",
                         "All ENABLED campaigns served impressions yesterday")]

    findings = []
    for r in rows:
        findings.append(_finding(
            "ACTIVE_ZERO_IMPRESSIONS", "warn",
            f"{r.campaign_name} ({r.campaign_type}, ${r.budget_amount:.2f}/day): "
            "ENABLED but 0 impressions yesterday.",
            action="Check for disapproved ads, policy violations, billing issues, or overly narrow targeting.",
        ))
    return findings


# ── CATEGORY 3: Search Term Waste ─────────────────────────────────────────


def _check_waste_terms(floor: float) -> list[dict]:
    """Search terms with $X+ spend and 0 conversions in trailing 7 days."""
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    sql = f"""
    SELECT search_term, campaign_name,
        SUM(spend) AS total_spend,
        SUM(clicks) AS total_clicks
    FROM `{_RAW}.search_terms`
    WHERE date_start >= '{cutoff}' AND spend > 0
    GROUP BY search_term, campaign_name
    HAVING SUM(conversions) = 0
        AND SUM(spend) >= {floor}
    ORDER BY total_spend DESC
    LIMIT 10
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("WASTE_TERMS: query failed", exc_info=True)
        return [_finding("WASTE_TERMS", "warn", "Could not query search terms table")]

    if not rows:
        return [_finding("WASTE_TERMS", "ok",
                         f"No single search term wasted ≥${floor:.0f} this week")]

    findings = []
    for r in rows:
        findings.append(_finding(
            "WASTE_TERMS", "warn",
            f"'{r.search_term}' [{r.campaign_name}]: ${r.total_spend:.2f} wasted, "
            f"{r.total_clicks} clicks, 0 conversions.",
            dollar_impact=r.total_spend,
            action=f"Add '{r.search_term}' as negative keyword (phrase match) to Master Negatives list.",
        ))
    return findings


def _check_total_waste_pct(pct_warn: float) -> list[dict]:
    """Wasted search-term spend as % of total campaign spend this week."""
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    sql = f"""
    WITH term_totals AS (
        SELECT search_term, campaign_name,
            SUM(spend) AS spend_7d,
            SUM(conversions) AS conv_7d
        FROM `{_RAW}.search_terms`
        WHERE date_start >= '{cutoff}' AND spend > 0
        GROUP BY search_term, campaign_name
    )
    SELECT
        SUM(spend_7d) AS total_spend,
        SUM(CASE WHEN conv_7d = 0 THEN spend_7d ELSE 0 END) AS wasted_spend,
        SAFE_DIVIDE(
            SUM(CASE WHEN conv_7d = 0 THEN spend_7d ELSE 0 END),
            SUM(spend_7d)
        ) AS waste_pct
    FROM term_totals
    """
    try:
        rows = list(run_query(sql))
        r = rows[0] if rows else None
    except Exception:
        log.warning("TOTAL_WASTE_PCT: query failed", exc_info=True)
        return [_finding("TOTAL_WASTE_PCT", "warn", "Could not compute total waste percentage")]

    if r is None or r.total_spend is None or r.total_spend == 0:
        return [_finding("TOTAL_WASTE_PCT", "ok", "No search term spend data this week")]

    pct = (r.waste_pct or 0.0) * 100
    threshold = pct_warn
    if pct >= threshold:
        return [_finding(
            "TOTAL_WASTE_PCT", "warn" if pct < threshold * 1.5 else "crit",
            f"Wasted search-term spend: ${r.wasted_spend:.2f} of ${r.total_spend:.2f} "
            f"({pct:.1f}% — threshold {threshold:.0f}%).",
            dollar_impact=r.wasted_spend,
            action="Run negative keyword review. Add top wasted terms to Master Negatives list.",
        )]
    return [_finding("TOTAL_WASTE_PCT", "ok",
                     f"Wasted search-term spend {pct:.1f}% of total (threshold {threshold:.0f}%)")]


def _check_competitor_bleed() -> list[dict]:
    """Spend on known competitor brand terms that slipped the Master Negatives list."""
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    competitor_terms = [
        "bodhi", "udrippin", "flick weight", "joola", "tourna",
        "j2cr", "selkirk", "paddletek", "engage pickleball",
    ]
    like_clauses = " OR ".join(
        f"LOWER(search_term) LIKE '%{t}%'" for t in competitor_terms
    )
    sql = f"""
    SELECT search_term, campaign_name, SUM(spend) AS total_spend
    FROM `{_RAW}.search_terms`
    WHERE date_start >= '{cutoff}'
        AND spend > 0
        AND ({like_clauses})
    GROUP BY search_term, campaign_name
    HAVING SUM(spend) > 0
    ORDER BY total_spend DESC
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("COMPETITOR_BLEED: query failed", exc_info=True)
        return [_finding("COMPETITOR_BLEED", "warn",
                         "Could not check for competitor term spend")]

    if not rows:
        return [_finding("COMPETITOR_BLEED", "ok",
                         "No spend on competitor brand terms detected this week")]

    total = sum(r.total_spend for r in rows)
    terms_str = ", ".join(
        f"'{r.search_term}' (${r.total_spend:.2f})" for r in rows[:5]
    )
    return [_finding(
        "COMPETITOR_BLEED", "warn",
        f"${total:.2f} spent on competitor-adjacent terms: {terms_str}.",
        dollar_impact=total,
        action="Add terms to Master Negatives list (phrase match). "
               "Note: Flick Weight terms are intentional stop-gap — remove when comparison lander is live.",
    )]


def _check_wrong_sport_bleed() -> list[dict]:
    """Spend on wrong-sport terms that bypassed Master Negatives."""
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    wrong_sport_terms = [
        "tennis", "golf", "ping pong", "table tennis", "badminton",
        "squash", "racquetball", "paddleball",
    ]
    like_clauses = " OR ".join(
        f"LOWER(search_term) LIKE '%{t}%'" for t in wrong_sport_terms
    )
    sql = f"""
    SELECT search_term, campaign_name, SUM(spend) AS total_spend, SUM(clicks) AS clicks
    FROM `{_RAW}.search_terms`
    WHERE date_start >= '{cutoff}'
        AND spend > 0
        AND ({like_clauses})
    GROUP BY search_term, campaign_name
    ORDER BY total_spend DESC
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("WRONG_SPORT_BLEED: query failed", exc_info=True)
        return [_finding("WRONG_SPORT_BLEED", "warn",
                         "Could not check for wrong-sport term spend")]

    if not rows:
        return [_finding("WRONG_SPORT_BLEED", "ok",
                         "No spend on wrong-sport terms detected")]

    total = sum(r.total_spend for r in rows)
    terms_str = ", ".join(f"'{r.search_term}'" for r in rows[:5])
    return [_finding(
        "WRONG_SPORT_BLEED", "crit",
        f"Wrong-sport terms leaked Master Negatives — ${total:.2f} wasted on: {terms_str}.",
        dollar_impact=total,
        action="Add all wrong-sport terms to Master Negatives list immediately (phrase match).",
    )]


def _check_winning_terms_missing_keywords() -> list[dict]:
    """High-converting search terms (2+ conv) not yet added as dedicated keywords."""
    cutoff = (date.today() - timedelta(days=30)).isoformat()
    sql = f"""
    WITH term_perf AS (
        SELECT search_term, campaign_name, keyword_text,
            SUM(conversions) AS conv,
            SUM(spend) AS spend,
            SUM(clicks) AS clicks
        FROM `{_RAW}.search_terms`
        WHERE date_start >= '{cutoff}' AND spend > 0
        GROUP BY search_term, campaign_name, keyword_text
        HAVING SUM(conversions) >= 2
            AND search_term != keyword_text
    )
    SELECT tp.search_term, tp.campaign_name, tp.keyword_text,
        tp.conv, tp.spend
    FROM term_perf tp
    LEFT JOIN `{_RAW}.keywords` k
        ON LOWER(tp.search_term) = LOWER(k.keyword_text)
    WHERE k.keyword_text IS NULL
    ORDER BY tp.conv DESC, tp.spend DESC
    LIMIT 10
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("WINNING_TERMS_NO_KW: query failed", exc_info=True)
        return [_finding("WINNING_TERMS_NO_KW", "warn",
                         "Could not check for winning terms missing as keywords")]

    if not rows:
        return [_finding("WINNING_TERMS_NO_KW", "ok",
                         "All high-converting search terms are already targeted as keywords")]

    findings = []
    for r in rows:
        findings.append(_finding(
            "WINNING_TERMS_NO_KW", "warn",
            f"'{r.search_term}' [{r.campaign_name}]: {r.conv:.0f} conv, ${r.spend:.2f} spend — "
            f"matched via '{r.keyword_text}' but not its own keyword.",
            dollar_impact=0.0,
            action=f"Add '{r.search_term}' as exact or phrase-match keyword in {r.campaign_name}. "
                   "Capturing it directly improves bid control and QS.",
        ))
    return findings


# ── CATEGORY 4: Keyword Quality ────────────────────────────────────────────


def _check_quality_score_low(qs_floor: int, min_spend: float) -> list[dict]:
    """Keywords with QS below floor that have meaningful spend."""
    sql = f"""
    WITH kw_spend AS (
        SELECT keyword_text, ad_group_id, campaign_id,
            SUM(spend) AS total_spend
        FROM `{_RAW}.search_terms`
        WHERE date_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY keyword_text, ad_group_id, campaign_id
    )
    SELECT k.keyword_text, k.quality_score,
        k.expected_ctr, k.ad_relevance, k.landing_page_experience,
        ks.total_spend
    FROM `{_RAW}.keywords` k
    JOIN kw_spend ks ON k.keyword_text = ks.keyword_text
        AND k.ad_group_id = ks.ad_group_id
        AND k.campaign_id = ks.campaign_id
    WHERE k.quality_score IS NOT NULL
        AND k.quality_score > 0
        AND k.quality_score < {qs_floor}
        AND k.status = 'ENABLED'
        AND ks.total_spend >= {min_spend}
    ORDER BY k.quality_score ASC, ks.total_spend DESC
    LIMIT 10
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("QS_LOW: query failed", exc_info=True)
        return [_finding("QS_LOW", "warn", "Could not check keyword quality scores")]

    if not rows:
        return [_finding("QS_LOW", "ok",
                         f"No ENABLED keywords with QS < {qs_floor} and ${min_spend}+ spend")]

    findings = []
    for r in rows:
        findings.append(_finding(
            "QS_LOW", "warn",
            f"'{r.keyword_text}' QS {r.quality_score}/10 (floor {qs_floor}). "
            f"CTR: {r.expected_ctr}, Relevance: {r.ad_relevance}, "
            f"Landing page: {r.landing_page_experience}. ${r.total_spend:.2f} spend (30d).",
            action="Improve ad copy relevance, or create a tighter ad group. "
                   "QS < 5 means you're paying above-benchmark CPCs.",
        ))
    return findings


def _check_landing_page_below_average() -> list[dict]:
    """Keywords flagged landing_page_experience = BELOW_AVERAGE with meaningful spend."""
    sql = f"""
    WITH kw_spend AS (
        SELECT keyword_text, ad_group_id, campaign_id,
            SUM(spend) AS total_spend
        FROM `{_RAW}.search_terms`
        WHERE date_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY keyword_text, ad_group_id, campaign_id
    )
    SELECT k.keyword_text, k.quality_score, ks.total_spend
    FROM `{_RAW}.keywords` k
    JOIN kw_spend ks ON k.keyword_text = ks.keyword_text
        AND k.ad_group_id = ks.ad_group_id
        AND k.campaign_id = ks.campaign_id
    WHERE k.landing_page_experience = 'BELOW_AVERAGE'
        AND k.status = 'ENABLED'
        AND ks.total_spend >= 5
    ORDER BY ks.total_spend DESC
    LIMIT 10
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("LANDING_PAGE_BELOW_AVG: query failed", exc_info=True)
        return [_finding("LANDING_PAGE_BELOW_AVG", "warn",
                         "Could not check landing page experience scores")]

    if not rows:
        return [_finding("LANDING_PAGE_BELOW_AVG", "ok",
                         "No keywords flagged BELOW_AVERAGE landing page experience")]

    total_spend = sum(r.total_spend for r in rows)
    kw_list = ", ".join(f"'{r.keyword_text}'" for r in rows[:5])
    return [_finding(
        "LANDING_PAGE_BELOW_AVG", "warn",
        f"{len(rows)} keyword(s) with BELOW_AVERAGE landing page experience: {kw_list}. "
        f"${total_spend:.2f} total spend (30d).",
        dollar_impact=total_spend,
        action="Audit landing pages for relevance, load speed, and mobile UX. "
               "Match keyword intent to landing page content more closely.",
    )]


def _check_ad_relevance_below_average() -> list[dict]:
    """Keywords flagged ad_relevance = BELOW_AVERAGE with meaningful spend."""
    sql = f"""
    WITH kw_spend AS (
        SELECT keyword_text, ad_group_id, campaign_id,
            SUM(spend) AS total_spend
        FROM `{_RAW}.search_terms`
        WHERE date_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY keyword_text, ad_group_id, campaign_id
    )
    SELECT k.keyword_text, k.quality_score, ks.total_spend
    FROM `{_RAW}.keywords` k
    JOIN kw_spend ks ON k.keyword_text = ks.keyword_text
        AND k.ad_group_id = ks.ad_group_id
        AND k.campaign_id = ks.campaign_id
    WHERE k.ad_relevance = 'BELOW_AVERAGE'
        AND k.status = 'ENABLED'
        AND ks.total_spend >= 5
    ORDER BY ks.total_spend DESC
    LIMIT 10
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("AD_RELEVANCE_BELOW_AVG: query failed", exc_info=True)
        return [_finding("AD_RELEVANCE_BELOW_AVG", "warn",
                         "Could not check ad relevance scores")]

    if not rows:
        return [_finding("AD_RELEVANCE_BELOW_AVG", "ok",
                         "No keywords flagged BELOW_AVERAGE ad relevance")]

    total_spend = sum(r.total_spend for r in rows)
    kw_list = ", ".join(f"'{r.keyword_text}'" for r in rows[:5])
    return [_finding(
        "AD_RELEVANCE_BELOW_AVG", "warn",
        f"{len(rows)} keyword(s) with BELOW_AVERAGE ad relevance: {kw_list}. "
        f"${total_spend:.2f} total spend (30d).",
        dollar_impact=total_spend,
        action="Rewrite ad headlines to include the keyword verbatim. "
               "Create tighter single-keyword ad groups (SKAGs) for top terms.",
    )]


def _check_keyword_wasted_spend(min_spend: float) -> list[dict]:
    """ENABLED keywords with high total spend and zero conversions (all time in data)."""
    sql = f"""
    WITH kw_perf AS (
        SELECT keyword_text, campaign_name, ad_group_id,
            SUM(spend) AS total_spend,
            SUM(conversions) AS total_conv,
            SUM(clicks) AS total_clicks
        FROM `{_RAW}.search_terms`
        WHERE spend > 0
        GROUP BY keyword_text, campaign_name, ad_group_id
    )
    SELECT kp.keyword_text, kp.campaign_name, kp.total_spend, kp.total_clicks,
        k.status
    FROM kw_perf kp
    JOIN `{_RAW}.keywords` k
        ON kp.keyword_text = k.keyword_text
        AND kp.ad_group_id = k.ad_group_id
    WHERE kp.total_conv = 0
        AND kp.total_spend >= {min_spend}
        AND k.status = 'ENABLED'
    ORDER BY kp.total_spend DESC
    LIMIT 10
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("KW_WASTED_SPEND: query failed", exc_info=True)
        return [_finding("KW_WASTED_SPEND", "warn",
                         "Could not check keyword-level wasted spend")]

    if not rows:
        return [_finding("KW_WASTED_SPEND", "ok",
                         f"No ENABLED keywords with ${min_spend}+ spend and 0 conversions")]

    findings = []
    for r in rows:
        findings.append(_finding(
            "KW_WASTED_SPEND", "warn",
            f"'{r.keyword_text}' [{r.campaign_name}]: ${r.total_spend:.2f} all-time spend, "
            f"{r.total_clicks} clicks, 0 conversions.",
            dollar_impact=r.total_spend,
            action=f"Pause '{r.keyword_text}' or restructure its ad group / landing page. "
                   "If it's a brand keyword, check conversion tracking integrity.",
        ))
    return findings


def _check_broad_match_dominance(pct_warn: float) -> list[dict]:
    """Broad-match keywords driving an outsized share of campaign spend."""
    cutoff = (date.today() - timedelta(days=14)).isoformat()
    sql = f"""
    WITH campaign_spend AS (
        SELECT campaign_name,
            SUM(spend) AS total_spend,
            SUM(CASE WHEN match_type = 'BROAD' THEN spend ELSE 0 END) AS broad_spend
        FROM `{_RAW}.search_terms`
        WHERE date_start >= '{cutoff}' AND spend > 0
        GROUP BY campaign_name
    )
    SELECT campaign_name, total_spend, broad_spend,
        SAFE_DIVIDE(broad_spend, total_spend) AS broad_pct
    FROM campaign_spend
    WHERE total_spend > 10
        AND SAFE_DIVIDE(broad_spend, total_spend) > {pct_warn / 100.0}
    ORDER BY broad_pct DESC
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("BROAD_MATCH_DOMINANCE: query failed", exc_info=True)
        return [_finding("BROAD_MATCH_DOMINANCE", "warn",
                         "Could not check broad match dominance")]

    if not rows:
        return [_finding("BROAD_MATCH_DOMINANCE", "ok",
                         f"No campaign has broad match > {pct_warn:.0f}% of spend (14d)")]

    findings = []
    for r in rows:
        findings.append(_finding(
            "BROAD_MATCH_DOMINANCE", "warn",
            f"{r.campaign_name}: {r.broad_pct:.0%} of spend from broad match "
            f"(${r.broad_spend:.2f} of ${r.total_spend:.2f}, 14d).",
            action="Add more phrase/exact match keywords to reduce broad match reliance. "
                   "Review broad search term report for negative keyword candidates.",
        ))
    return findings


# ── CATEGORY 5: Shopping Performance ──────────────────────────────────────


def _check_shopping_roas(roas_floor: float, min_spend: float) -> list[dict]:
    """Shopping campaign 7-day ROAS below floor."""
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    sql = f"""
    SELECT campaign_name,
        SAFE_DIVIDE(SUM(conversion_value), SUM(spend)) AS roas,
        SUM(spend) AS total_spend,
        SUM(conversion_value) AS total_rev
    FROM `{_RAW}.shopping_performance`
    WHERE date_start >= '{cutoff}'
    GROUP BY campaign_id, campaign_name
    HAVING SUM(spend) >= {min_spend}
        AND (SUM(conversion_value) = 0
             OR SAFE_DIVIDE(SUM(conversion_value), SUM(spend)) < {roas_floor})
    ORDER BY total_spend DESC
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("SHOPPING_ROAS: query failed", exc_info=True)
        return [_finding("SHOPPING_ROAS", "warn",
                         "Could not compute Shopping campaign ROAS")]

    if not rows:
        return [_finding("SHOPPING_ROAS", "ok",
                         f"Shopping at or above {roas_floor:.1f}x ROAS floor")]

    findings = []
    for r in rows:
        roas = r.roas or 0.0
        findings.append(_finding(
            "SHOPPING_ROAS", "crit" if roas < 1.5 else "warn",
            f"{r.campaign_name}: 7-day ROAS {roas:.2f}x (floor {roas_floor:.1f}x). "
            f"Spend ${r.total_spend:.2f}, Revenue ${r.total_rev:.2f}.",
            dollar_impact=r.total_spend,
            action="Audit product feed for disapprovals. Check top-product CTR and conv rate. "
                   "Target ROAS may be too aggressive — consider lowering from 280% to 250%.",
        ))
    return findings


def _check_product_wasted_spend(floor: float) -> list[dict]:
    """Individual products with $X+ spend and 0 conversions in trailing 7 days."""
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    sql = f"""
    SELECT product_title, campaign_name,
        SUM(spend) AS total_spend,
        SUM(clicks) AS total_clicks
    FROM `{_RAW}.shopping_performance`
    WHERE date_start >= '{cutoff}' AND spend > 0
    GROUP BY product_title, product_item_id, campaign_name
    HAVING SUM(conversions) = 0
        AND SUM(spend) >= {floor}
    ORDER BY total_spend DESC
    LIMIT 10
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("PRODUCT_WASTED_SPEND: query failed", exc_info=True)
        return [_finding("PRODUCT_WASTED_SPEND", "warn",
                         "Could not check product-level wasted spend")]

    if not rows:
        return [_finding("PRODUCT_WASTED_SPEND", "ok",
                         f"No products with ${floor:.0f}+ spend and 0 conversions this week")]

    findings = []
    for r in rows:
        findings.append(_finding(
            "PRODUCT_WASTED_SPEND", "warn",
            f"'{r.product_title[:60]}' [{r.campaign_name}]: ${r.total_spend:.2f} spend, "
            f"{r.total_clicks} clicks, 0 conv (7d).",
            dollar_impact=r.total_spend,
            action="Review product listing quality, pricing vs competitors, and image quality. "
                   "Exclude product from Shopping if repeatedly zero-conversion.",
        ))
    return findings


def _check_top_sku_zero_impressions() -> list[dict]:
    """Top-performing SKUs (historically) with 0 impressions in trailing 7 days."""
    cutoff_7d = (date.today() - timedelta(days=7)).isoformat()
    cutoff_90d = (date.today() - timedelta(days=90)).isoformat()
    sql = f"""
    WITH historical_top AS (
        SELECT product_item_id, product_title,
            SUM(conversions) AS total_conv
        FROM `{_RAW}.shopping_performance`
        WHERE date_start >= '{cutoff_90d}'
        GROUP BY product_item_id, product_title
        HAVING SUM(conversions) >= 3
    ),
    recent_imps AS (
        SELECT product_item_id, SUM(impressions) AS imps_7d
        FROM `{_RAW}.shopping_performance`
        WHERE date_start >= '{cutoff_7d}'
        GROUP BY product_item_id
    )
    SELECT h.product_title, h.total_conv,
        COALESCE(r.imps_7d, 0) AS imps_7d
    FROM historical_top h
    LEFT JOIN recent_imps r ON h.product_item_id = r.product_item_id
    WHERE COALESCE(r.imps_7d, 0) = 0
    ORDER BY h.total_conv DESC
    LIMIT 5
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("TOP_SKU_ZERO_IMPRESSIONS: query failed", exc_info=True)
        return [_finding("TOP_SKU_ZERO_IMPRESSIONS", "warn",
                         "Could not check top SKU impression coverage")]

    if not rows:
        return [_finding("TOP_SKU_ZERO_IMPRESSIONS", "ok",
                         "All historically converting SKUs have impressions this week")]

    skus = ", ".join(
        f"'{r.product_title[:40]}' ({r.total_conv:.0f} hist. conv)" for r in rows
    )
    return [_finding(
        "TOP_SKU_ZERO_IMPRESSIONS", "crit",
        f"{len(rows)} top-converting SKU(s) have 0 impressions this week: {skus}.",
        action="Check Merchant Center for feed disapprovals or out-of-stock status. "
               "Verify the product isn't excluded from Shopping campaign.",
    )]


def _check_shopping_low_ctr() -> list[dict]:
    """Products with 100+ impressions and CTR < 0.2% this week."""
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    sql = f"""
    SELECT product_title, campaign_name,
        SUM(impressions) AS imps,
        SUM(clicks) AS clicks,
        SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr,
        SUM(spend) AS spend
    FROM `{_RAW}.shopping_performance`
    WHERE date_start >= '{cutoff}'
    GROUP BY product_title, product_item_id, campaign_name
    HAVING SUM(impressions) >= 100
        AND SAFE_DIVIDE(SUM(clicks), SUM(impressions)) < 0.002
    ORDER BY spend DESC
    LIMIT 5
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("SHOPPING_LOW_CTR: query failed", exc_info=True)
        return [_finding("SHOPPING_LOW_CTR", "warn",
                         "Could not check Shopping product CTR")]

    if not rows:
        return [_finding("SHOPPING_LOW_CTR", "ok",
                         "No products with 100+ impressions and CTR < 0.2%")]

    findings = []
    for r in rows:
        findings.append(_finding(
            "SHOPPING_LOW_CTR", "warn",
            f"'{r.product_title[:50]}': CTR {r.ctr:.3%} on {r.imps:,} impressions "
            f"(${r.spend:.2f} spend this week).",
            action="Improve main product image, title clarity, and price competitiveness. "
                   "Low CTR hurts QS and raises effective CPC.",
        ))
    return findings


def _check_shopping_product_count_drop() -> list[dict]:
    """Number of distinct products serving impressions dropped >20% week-over-week."""
    sql = f"""
    WITH weekly AS (
        SELECT
            CASE
                WHEN date_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN 'this_week'
                WHEN date_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) THEN 'last_week'
            END AS week,
            COUNT(DISTINCT product_item_id) AS product_count
        FROM `{_RAW}.shopping_performance`
        WHERE date_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
            AND impressions > 0
        GROUP BY 1
    )
    SELECT
        MAX(CASE WHEN week = 'this_week' THEN product_count END) AS this_week,
        MAX(CASE WHEN week = 'last_week' THEN product_count END) AS last_week,
        SAFE_DIVIDE(
            MAX(CASE WHEN week = 'last_week' THEN product_count END) -
            MAX(CASE WHEN week = 'this_week' THEN product_count END),
            MAX(CASE WHEN week = 'last_week' THEN product_count END)
        ) AS drop_pct
    FROM weekly
    """
    try:
        rows = list(run_query(sql))
        r = rows[0] if rows else None
    except Exception:
        log.warning("PRODUCT_COUNT_DROP: query failed", exc_info=True)
        return [_finding("PRODUCT_COUNT_DROP", "warn",
                         "Could not check week-over-week product count")]

    if r is None or r.last_week is None or r.last_week == 0:
        return [_finding("PRODUCT_COUNT_DROP", "ok",
                         "Insufficient Shopping data for WoW product count comparison")]

    drop_pct = (r.drop_pct or 0.0) * 100
    if drop_pct >= 20:
        return [_finding(
            "PRODUCT_COUNT_DROP", "warn",
            f"Products serving impressions dropped {drop_pct:.0f}% WoW: "
            f"{r.last_week} → {r.this_week}.",
            action="Check Merchant Center for new feed disapprovals or inventory gaps. "
                   "Download disapprovals report from MC > Products > Diagnostics.",
        )]
    return [_finding("PRODUCT_COUNT_DROP", "ok",
                     f"Products serving impressions: {r.this_week} this week, "
                     f"{r.last_week} last week ({drop_pct:.0f}% change)")]


# ── CATEGORY 6: Bidding / Budget / Structural ─────────────────────────────


def _check_budget_too_low(min_budget: float) -> list[dict]:
    """ENABLED campaign with daily budget below minimum."""
    sql = f"""
    SELECT campaign_name, CAST(budget_amount AS FLOAT64) AS budget_amount
    FROM `{_RAW}.campaigns`
    WHERE status = 'ENABLED'
        AND CAST(budget_amount AS FLOAT64) < {min_budget}
    ORDER BY budget_amount ASC
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("BUDGET_TOO_LOW: query failed", exc_info=True)
        return [_finding("BUDGET_TOO_LOW", "warn",
                         "Could not check campaign budgets")]

    if not rows:
        return [_finding("BUDGET_TOO_LOW", "ok",
                         f"All active campaigns above ${min_budget:.0f}/day minimum")]

    findings = []
    for r in rows:
        findings.append(_finding(
            "BUDGET_TOO_LOW", "warn",
            f"{r.campaign_name}: ${r.budget_amount:.2f}/day — below ${min_budget:.0f} minimum.",
            action=f"Increase budget to at least ${min_budget:.0f}/day or pause campaign.",
        ))
    return findings


def _check_cpc_spike(multiplier: float) -> list[dict]:
    """Campaign avg CPC > multiplier × its 14-day avg (competitor bidding pressure)."""
    sql = f"""
    WITH daily_cpc AS (
        SELECT campaign_name, date_start,
            SAFE_DIVIDE(SUM(spend), NULLIF(SUM(clicks), 0)) AS avg_cpc
        FROM `{_RAW}.daily_insights`
        WHERE date_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY)
            AND clicks > 0
        GROUP BY campaign_name, date_start
    ),
    summary AS (
        SELECT campaign_name,
            AVG(CASE WHEN date_start < DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                     THEN avg_cpc END) AS baseline_cpc,
            MAX(CASE WHEN date_start = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                     THEN avg_cpc END) AS yesterday_cpc
        FROM daily_cpc
        GROUP BY campaign_name
    )
    SELECT campaign_name, yesterday_cpc, baseline_cpc,
        SAFE_DIVIDE(yesterday_cpc, baseline_cpc) AS ratio
    FROM summary
    WHERE baseline_cpc > 0.10
        AND yesterday_cpc IS NOT NULL
        AND SAFE_DIVIDE(yesterday_cpc, baseline_cpc) > {multiplier}
    ORDER BY ratio DESC
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("CPC_SPIKE: query failed", exc_info=True)
        return [_finding("CPC_SPIKE", "warn", "Could not check for CPC spikes")]

    if not rows:
        return [_finding("CPC_SPIKE", "ok",
                         f"No CPC spikes > {multiplier:.1f}× 14-day avg detected")]

    findings = []
    for r in rows:
        findings.append(_finding(
            "CPC_SPIKE", "warn",
            f"{r.campaign_name}: yesterday CPC ${r.yesterday_cpc:.2f} is "
            f"{r.ratio:.1f}× the 14-day avg ${r.baseline_cpc:.2f}.",
            action="Check Auction Insights for new competitors. "
                   "Review impression share and top-of-page rate trends.",
        ))
    return findings


def _check_account_cvr_low(cvr_floor: float, min_spend: float) -> list[dict]:
    """Account-level CVR below floor on meaningful spend this week."""
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    sql = f"""
    SELECT
        SUM(spend) AS total_spend,
        SUM(clicks) AS total_clicks,
        SUM(conversions) AS total_conv,
        SAFE_DIVIDE(SUM(conversions), SUM(clicks)) AS cvr
    FROM `{_RAW}.daily_insights`
    WHERE date_start >= '{cutoff}'
    """
    try:
        rows = list(run_query(sql))
        r = rows[0] if rows else None
    except Exception:
        log.warning("ACCOUNT_CVR_LOW: query failed", exc_info=True)
        return [_finding("ACCOUNT_CVR_LOW", "warn",
                         "Could not compute account-level CVR")]

    if r is None or r.total_spend is None or r.total_spend < min_spend:
        return [_finding("ACCOUNT_CVR_LOW", "ok",
                         f"Insufficient spend (${(r.total_spend or 0):.2f}) for CVR check")]

    cvr = r.cvr or 0.0
    if cvr < cvr_floor:
        return [_finding(
            "ACCOUNT_CVR_LOW", "warn",
            f"Account CVR {cvr:.2%} on ${r.total_spend:.2f} spend this week "
            f"(floor {cvr_floor:.2%}, {r.total_conv:.0f} conv / {r.total_clicks} clicks).",
            action="Audit landing page load speed (aim < 2s), mobile UX, and price "
                   "competitiveness. Check for recent site changes or broken checkout.",
        )]
    return [_finding("ACCOUNT_CVR_LOW", "ok",
                     f"Account CVR {cvr:.2%} (floor {cvr_floor:.2%}) — healthy")]


def _check_target_roas_overly_aggressive() -> list[dict]:
    """Shopping campaign tROAS >450% — over-constrained, likely leaving spend on table."""
    sql = f"""
    SELECT campaign_name, bidding_strategy_type,
        CAST(budget_amount AS FLOAT64) AS budget_amount
    FROM `{_RAW}.campaigns`
    WHERE status = 'ENABLED'
        AND LOWER(campaign_type) LIKE '%shopping%'
    """
    # tROAS value isn't in the campaigns table — check via insights ROAS proxy
    # If actual ROAS >> target, bidding is over-constrained
    cutoff = (date.today() - timedelta(days=14)).isoformat()
    sql2 = f"""
    SELECT campaign_name,
        SAFE_DIVIDE(SUM(conversion_value), SUM(spend)) AS actual_roas,
        SUM(spend) AS total_spend,
        SUM(impressions) AS total_imps
    FROM `{_RAW}.daily_insights`
    WHERE date_start >= '{cutoff}'
        AND LOWER(campaign_name) LIKE '%shopping%'
    GROUP BY campaign_name
    HAVING SUM(spend) >= 50
        AND SAFE_DIVIDE(SUM(conversion_value), SUM(spend)) > 4.5
    """
    try:
        rows = list(run_query(sql2))
    except Exception:
        log.warning("TROAS_AGGRESSIVE: query failed", exc_info=True)
        return [_finding("TROAS_AGGRESSIVE", "warn",
                         "Could not check Shopping tROAS constraint")]

    if not rows:
        return [_finding("TROAS_AGGRESSIVE", "ok",
                         "Shopping ROAS does not suggest over-constrained tROAS target")]

    findings = []
    for r in rows:
        findings.append(_finding(
            "TROAS_AGGRESSIVE", "warn",
            f"{r.campaign_name}: actual ROAS {r.actual_roas:.1f}x over 14d — "
            f"if tROAS is set very high, Smart Bidding may be under-serving eligible auctions. "
            f"${r.total_spend:.2f} spend, {r.total_imps:,} impressions.",
            action="If Shopping budget cap is not being reached, consider lowering tROAS "
                   "from current 280% toward 250% to capture more volume at still-profitable ROAS.",
        ))
    return findings


def _check_no_conversions_14d_search() -> list[dict]:
    """Active Search campaign with 0 conversions over 14 days."""
    cutoff = (date.today() - timedelta(days=14)).isoformat()
    sql = f"""
    SELECT di.campaign_name, SUM(di.spend) AS total_spend,
        SUM(di.impressions) AS total_imps
    FROM `{_RAW}.daily_insights` di
    JOIN `{_RAW}.campaigns` c ON di.campaign_id = c.campaign_id
    WHERE di.date_start >= '{cutoff}'
        AND c.status = 'ENABLED'
        AND LOWER(c.campaign_type) LIKE '%search%'
    GROUP BY di.campaign_id, di.campaign_name
    HAVING SUM(di.conversions) = 0
        AND SUM(di.spend) >= 10
    ORDER BY total_spend DESC
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.warning("NO_CONV_14D: query failed", exc_info=True)
        return [_finding("NO_CONV_14D", "warn",
                         "Could not check 14-day conversion data for Search campaigns")]

    if not rows:
        return [_finding("NO_CONV_14D", "ok",
                         "All active Search campaigns recorded conversions in last 14 days")]

    findings = []
    for r in rows:
        findings.append(_finding(
            "NO_CONV_14D", "crit",
            f"{r.campaign_name}: ${r.total_spend:.2f} spend over 14 days, 0 conversions "
            f"({r.total_imps:,} impressions).",
            dollar_impact=r.total_spend,
            action="Check campaign-specific conversion goal. Verify pixel is firing. "
                   "Review search terms for severe relevance mismatch.",
        ))
    return findings


# ── Orchestration ─────────────────────────────────────────────────────────


def _all_rules(cfg: dict) -> list[dict]:
    """Run all 30 rules and return flattened list of findings."""
    results: list[dict] = []

    # Category 1: Conversion config (5 rules)
    results += _check_conversion_polluters()
    results += _check_purchase_conv_enabled()
    results += _check_conv_action_count()
    results += _check_default_value_low()
    results += _check_data_freshness()

    # Category 2: Campaign performance (6 rules)
    results += _check_roas_below_target(
        roas_floor=cfg.get("roas_warn_floor", 2.5),
        min_spend=cfg.get("campaign_min_spend_7d", 20.0),
    )
    results += _check_zero_conv_high_spend(
        min_spend=cfg.get("zero_conv_spend_floor", 50.0),
        lookback_days=14,
    )
    results += _check_spend_anomaly_campaign(pct=cfg.get("spend_anomaly_pct", 25.0))
    results += _check_impression_share_brand(is_floor=cfg.get("impression_share_brand_floor", 0.50))
    results += _check_active_zero_impressions()
    results += _check_no_conversions_14d_search()

    # Category 3: Search term waste (5 rules)
    results += _check_waste_terms(floor=cfg.get("waste_term_floor_7d", 10.0))
    results += _check_total_waste_pct(pct_warn=cfg.get("total_waste_pct_warn", 20.0))
    results += _check_competitor_bleed()
    results += _check_wrong_sport_bleed()
    results += _check_winning_terms_missing_keywords()

    # Category 4: Keyword quality (5 rules)
    results += _check_quality_score_low(
        qs_floor=cfg.get("quality_score_floor", 5),
        min_spend=cfg.get("kw_spend_floor_30d", 5.0),
    )
    results += _check_landing_page_below_average()
    results += _check_keyword_wasted_spend(min_spend=cfg.get("keyword_waste_floor", 30.0))
    results += _check_broad_match_dominance(pct_warn=cfg.get("broad_match_pct_warn", 40.0))
    results += _check_ad_relevance_below_average()

    # Category 5: Shopping (5 rules)
    results += _check_shopping_roas(
        roas_floor=cfg.get("shopping_roas_floor", 2.5),
        min_spend=cfg.get("shopping_min_spend_7d", 30.0),
    )
    results += _check_product_wasted_spend(floor=cfg.get("product_waste_floor_7d", 15.0))
    results += _check_top_sku_zero_impressions()
    results += _check_shopping_low_ctr()
    results += _check_shopping_product_count_drop()

    # Category 6: Bidding / budget / structural (4 rules)
    results += _check_budget_too_low(min_budget=cfg.get("budget_minimum", 10.0))
    results += _check_cpc_spike(multiplier=cfg.get("cpc_spike_multiplier", 2.0))
    results += _check_account_cvr_low(
        cvr_floor=cfg.get("account_cvr_floor", 0.005),
        min_spend=cfg.get("account_cvr_min_spend", 100.0),
    )
    results += _check_target_roas_overly_aggressive()

    return results


def _build_report(findings: list[dict], run_date: date) -> str:
    """Build the full Markdown health-check report."""
    crits = [f for f in findings if f["status"] == "crit"]
    warns = [f for f in findings if f["status"] == "warn"]
    oks = [f for f in findings if f["status"] == "ok"]

    total_dollar_at_risk = sum(f["dollar_impact"] for f in crits + warns)

    lines = [
        f"# Google Ads Health Check — {run_date}",
        "",
        f"**{len(findings)} rules | "
        f"✅ {len(oks)} OK | "
        f"⚠️ {len(warns)} WARN | "
        f"🔴 {len(crits)} CRIT**",
    ]

    if total_dollar_at_risk > 0:
        lines.append(f"**Dollar impact (warn+crit): ${total_dollar_at_risk:.2f}**")

    lines.append("")
    lines.append("---")
    lines.append("")

    for label, group, emoji in [
        ("Critical", crits, "🔴"),
        ("Warnings", warns, "⚠️"),
        ("Passing", oks, "✅"),
    ]:
        if not group:
            continue
        lines.append(f"## {emoji} {label} ({len(group)})")
        lines.append("")
        for f in group:
            lines.append(f"### {f['rule_id']}")
            lines.append(f"**Finding**: {f['finding']}")
            if f.get("dollar_impact", 0) > 0:
                lines.append(f"**Dollar impact**: ${f['dollar_impact']:.2f}")
            if f.get("action"):
                lines.append(f"**Action**: {f['action']}")
            lines.append("")

    return "\n".join(lines)


def run(run_date: date | None = None, to_stdout: bool = False) -> str:
    """Run all health-check rules and return the Markdown report.

    Args:
        run_date: Date context for the report header (default: today).
        to_stdout: If True, print to stdout instead of saving to file.
    """
    if run_date is None:
        run_date = date.today()

    log.info(f"Running Google Ads health check for {run_date}")
    cfg = _cfg()
    findings = _all_rules(cfg)

    crits = [f for f in findings if f["status"] == "crit"]
    warns = [f for f in findings if f["status"] == "warn"]
    log.info(f"Health check complete: {len(crits)} crit, {len(warns)} warn, "
             f"{sum(1 for f in findings if f['status'] == 'ok')} ok")

    report = _build_report(findings, run_date)

    # Claude analysis of non-OK findings
    flagged = crits + warns
    if flagged:
        log.info(f"Getting Claude analysis of {len(flagged)} flagged rules")
        flag_text = "\n".join(
            f"- [{f['status'].upper()}] {f['rule_id']}: {f['finding']}"
            for f in flagged
        )
        question = (
            f"The following Google Ads health-check rules fired for week of {run_date}. "
            "Give a prioritised action plan for this week: what to fix first, what to "
            "monitor, and what can wait. Reference specific numbers."
        )
        try:
            analysis = analyze(SYSTEM_PROMPT, flag_text, question)
            report += "\n---\n\n## AI Analysis\n\n" + analysis
        except Exception:
            log.warning("Claude analysis failed", exc_info=True)

    # Slack notification for any non-OK findings
    if flagged:
        try:
            from ingestion.utils.slack import send_slack, format_alert_summary
            slack_msg = format_alert_summary(
                [f for f in flagged],
                max_per_type=2,
            )
            # Prepend a header line
            header = (
                f":bar_chart: *Google Ads Health Check — {run_date}* | "
                f"{len(crits)} crit · {len(warns)} warn\n\n"
            )
            send_slack(header + slack_msg)
        except Exception:
            log.warning("Slack notification failed", exc_info=True)

    if to_stdout:
        print(report)
    else:
        path = _REPORTS_DIR / f"gads_health_{run_date}.md"
        path.write_text(report, encoding="utf-8")
        log.info(f"Health check report saved to {path}")

    return report
