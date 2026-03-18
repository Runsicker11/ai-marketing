"""Check performance thresholds and generate alerts with Claude analysis."""

from datetime import date, timedelta
from pathlib import Path

import yaml

from ingestion.utils.bq_client import run_query
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.logger import get_logger
from ingestion.analysis.claude_client import analyze

log = get_logger(__name__)

_REPORTS_DIR = Path(__file__).resolve().parents[2] / "reports"
_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"
_DS = f"{GCP_PROJECT_ID}.{BQ_DATASET}"

SYSTEM_PROMPT = """\
You are a marketing performance monitor for a DTC pickleball accessories brand. \
When an alert is triggered, you explain WHY it likely happened and recommend \
specific actions to fix it. Be concise and actionable. Reference the actual \
numbers provided.\
"""


def _load_thresholds() -> dict:
    path = _CONFIG_DIR / "thresholds.yaml"
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    return cfg.get("alerts", {})


def _check_roas_floor(report_date: date, floor: float) -> list[dict]:
    """Check if any channel's ROAS dropped below the floor."""
    sql = f"""
    SELECT channel, enhanced_roas, ga4_attributed_revenue, ad_spend
    FROM `{_DS}.vw_enhanced_roas`
    WHERE report_date = '{report_date}'
        AND ad_spend > 0
        AND enhanced_roas < {floor}
    """
    alerts = []
    for r in run_query(sql):
        alerts.append({
            "type": "ROAS_FLOOR_BREACH",
            "severity": "high",
            "message": (
                f"{r.channel} ROAS is {r.enhanced_roas:.2f} "
                f"(below {floor:.1f}x floor). "
                f"Revenue: ${r.ga4_attributed_revenue:.2f}, "
                f"Spend: ${r.ad_spend:.2f}"
            ),
        })
    return alerts


def _check_cpa_ceiling(report_date: date, ceiling: float) -> list[dict]:
    """Check if CPA exceeds the ceiling for any channel."""
    sql = f"""
    SELECT channel, ad_spend, ga4_attributed_orders,
           SAFE_DIVIDE(ad_spend, ga4_attributed_orders) AS cpa
    FROM `{_DS}.vw_enhanced_roas`
    WHERE report_date = '{report_date}'
        AND ad_spend > 0
        AND ga4_attributed_orders > 0
        AND SAFE_DIVIDE(ad_spend, ga4_attributed_orders) > {ceiling}
    """
    alerts = []
    for r in run_query(sql):
        alerts.append({
            "type": "CPA_CEILING_BREACH",
            "severity": "high",
            "message": (
                f"{r.channel} CPA is ${r.cpa:.2f} "
                f"(above ${ceiling:.2f} ceiling). "
                f"Spend: ${r.ad_spend:.2f}, "
                f"Orders: {r.ga4_attributed_orders}"
            ),
        })
    return alerts


def _check_spend_anomaly(report_date: date, pct: float) -> list[dict]:
    """Check if daily spend deviates from 7-day average by more than pct%."""
    sql = f"""
    SELECT meta_spend, spend_7d_avg,
           SAFE_DIVIDE(ABS(meta_spend - spend_7d_avg), spend_7d_avg) AS deviation
    FROM `{_DS}.vw_trends`
    WHERE report_date = '{report_date}'
        AND spend_7d_avg > 0
    """
    threshold = pct / 100.0 if pct > 1 else pct
    alerts = []
    for r in run_query(sql):
        if r.deviation and r.deviation > threshold:
            direction = "above" if r.meta_spend > r.spend_7d_avg else "below"
            alerts.append({
                "type": "SPEND_ANOMALY",
                "severity": "medium",
                "message": (
                    f"Daily spend ${r.meta_spend:.2f} is "
                    f"{r.deviation:.0%} {direction} "
                    f"7-day avg ${r.spend_7d_avg:.2f}"
                ),
            })
    return alerts


def _check_funnel_drop(report_date: date, pct: float) -> list[dict]:
    """Check if conversion rate dropped vs 7-day average."""
    threshold = pct / 100.0 if pct > 1 else pct
    sql = f"""
    WITH daily_cvr AS (
        -- Aggregate across all sources per day to get a single daily CVR
        SELECT
            report_date,
            SAFE_DIVIDE(SUM(purchases), SUM(sessions)) AS overall_conversion_rate
        FROM `{_DS}.vw_ga4_funnel`
        WHERE report_date BETWEEN '{report_date - timedelta(days=7)}' AND '{report_date}'
            AND sessions > 0
        GROUP BY report_date
    ),
    avg_cvr AS (
        SELECT AVG(overall_conversion_rate) AS avg_cvr
        FROM daily_cvr
        WHERE report_date < '{report_date}'
    )
    SELECT d.overall_conversion_rate, a.avg_cvr,
           SAFE_DIVIDE(a.avg_cvr - d.overall_conversion_rate, a.avg_cvr) AS drop_pct
    FROM daily_cvr d, avg_cvr a
    WHERE d.report_date = '{report_date}'
        AND a.avg_cvr > 0
        AND SAFE_DIVIDE(a.avg_cvr - d.overall_conversion_rate, a.avg_cvr) > {threshold}
    """
    alerts = []
    for r in run_query(sql):
        alerts.append({
            "type": "FUNNEL_DROP",
            "severity": "medium",
            "message": (
                f"Overall conversion rate dropped to "
                f"{r.overall_conversion_rate:.2%} "
                f"({r.drop_pct:.0%} below 7-day avg of {r.avg_cvr:.2%})"
            ),
        })
    return alerts


def _check_ctr_decline(report_date: date, days: int) -> list[dict]:
    """Check if any ad's CTR has declined for N+ consecutive days."""
    sql = f"""
    WITH daily_ctr AS (
        SELECT ad_id, ad_name, report_date, ctr,
               LAG(ctr) OVER (PARTITION BY ad_id ORDER BY report_date) AS prev_ctr
        FROM `{_DS}.vw_daily_performance`
        WHERE report_date BETWEEN '{report_date - timedelta(days=days + 1)}' AND '{report_date}'
            AND impressions >= 100
            AND ad_name IS NOT NULL
    ),
    declining AS (
        SELECT ad_id, ad_name, report_date, ctr,
               CASE WHEN ctr < prev_ctr THEN 1 ELSE 0 END AS is_decline
        FROM daily_ctr
        WHERE prev_ctr IS NOT NULL
    ),
    streaks AS (
        SELECT ad_id, ad_name, SUM(is_decline) AS decline_days,
               MIN(ctr) AS latest_ctr
        FROM declining
        GROUP BY ad_id, ad_name
        HAVING SUM(is_decline) >= {days}
    )
    SELECT * FROM streaks
    """
    alerts = []
    for r in run_query(sql):
        alerts.append({
            "type": "CTR_DECLINE",
            "severity": "medium",
            "message": (
                f"Ad '{r.ad_name}' CTR declining for {r.decline_days} days "
                f"(latest CTR: {r.latest_ctr:.2f}%). Possible creative fatigue."
            ),
        })
    return alerts


def _check_keyword_waste(report_date: date, ceiling: float) -> list[dict]:
    """Check if wasted search term spend exceeds threshold."""
    week_start = report_date - timedelta(days=6)
    sql = f"""
    SELECT search_term, SUM(spend) AS total_spend, SUM(clicks) AS total_clicks
    FROM `{_DS}.google_ads_search_terms`
    WHERE date_start BETWEEN '{week_start}' AND '{report_date}'
        AND conversions = 0
        AND spend > 0
    GROUP BY search_term
    HAVING SUM(spend) >= {ceiling}
    ORDER BY total_spend DESC
    LIMIT 10
    """
    alerts = []
    for r in run_query(sql):
        alerts.append({
            "type": "KEYWORD_WASTE",
            "severity": "medium",
            "message": (
                f"Search term '{r.search_term}' wasted "
                f"${r.total_spend:.2f} this week with {r.total_clicks} clicks "
                f"and zero conversions. Consider adding as negative keyword."
            ),
        })
    return alerts


def _check_quality_score_drop(report_date: date, floor: int) -> list[dict]:
    """Check if any keyword's quality score dropped below floor."""
    # Deduplicate by keyword_text — same keyword can exist in multiple ad groups
    sql = f"""
    SELECT keyword_text,
           MIN(quality_score) AS quality_score,
           ANY_VALUE(expected_ctr) AS expected_ctr,
           ANY_VALUE(ad_relevance) AS ad_relevance,
           ANY_VALUE(landing_page_experience) AS landing_page_experience
    FROM `{_DS}.google_ads_keywords`
    WHERE quality_score IS NOT NULL
        AND quality_score > 0
        AND quality_score < {floor}
        AND status = 'ENABLED'
    GROUP BY keyword_text
    ORDER BY quality_score ASC
    LIMIT 10
    """
    alerts = []
    for r in run_query(sql):
        alerts.append({
            "type": "QUALITY_SCORE_LOW",
            "severity": "medium",
            "message": (
                f"Keyword '{r.keyword_text}' quality score is {r.quality_score} "
                f"(below {floor}). CTR: {r.expected_ctr}, "
                f"Relevance: {r.ad_relevance}, "
                f"Landing page: {r.landing_page_experience}"
            ),
        })
    return alerts


def _check_ranking_drop(report_date: date, threshold: int) -> list[dict]:
    """Check if any key page lost positions week-over-week."""
    sql = f"""
    SELECT query, page, current_week_position, prior_week_position,
           position_change
    FROM `{_DS}.vw_seo_trends`
    WHERE position_change >= {threshold}
        AND current_week_impressions >= 50
    ORDER BY position_change DESC
    LIMIT 5
    """
    alerts = []
    try:
        for r in run_query(sql):
            alerts.append({
                "type": "RANKING_DROP",
                "severity": "medium",
                "message": (
                    f"'{r.query}' dropped {r.position_change:.1f} positions "
                    f"({r.prior_week_position:.1f} -> {r.current_week_position:.1f}) "
                    f"on page: {r.page}"
                ),
            })
    except Exception:
        pass  # SEO views may not exist yet if Search Console not connected
    return alerts


def check(report_date: date | None = None, to_stdout: bool = False) -> str:
    """Run all threshold checks and generate alert report.

    Args:
        report_date: Date to check (default: yesterday).
        to_stdout: If True, print alerts instead of saving to file.

    Returns:
        The formatted alert report text.
    """
    if report_date is None:
        report_date = date.today() - timedelta(days=1)

    log.info(f"Running alert checks for {report_date}")
    cfg = yaml.safe_load((_CONFIG_DIR / "thresholds.yaml").read_text(encoding="utf-8"))
    thresholds = cfg.get("alerts", {})
    google_ads_cfg = cfg.get("google_ads", {})
    seo_cfg = cfg.get("seo", {})

    all_alerts = []
    all_alerts.extend(_check_roas_floor(report_date, thresholds.get("roas_floor", 1.5)))
    all_alerts.extend(_check_cpa_ceiling(report_date, thresholds.get("cpa_ceiling", 50.0)))
    all_alerts.extend(_check_spend_anomaly(report_date, thresholds.get("spend_anomaly_pct", 30)))
    all_alerts.extend(_check_funnel_drop(report_date, thresholds.get("funnel_drop_pct", 20)))
    all_alerts.extend(_check_ctr_decline(report_date, thresholds.get("ctr_decline_days", 7)))
    # Google Ads alerts
    all_alerts.extend(_check_keyword_waste(
        report_date, google_ads_cfg.get("search_term_waste_ceiling", 20.0)))
    all_alerts.extend(_check_quality_score_drop(
        report_date, google_ads_cfg.get("quality_score_floor", 5)))
    # SEO alerts
    all_alerts.extend(_check_ranking_drop(
        report_date, seo_cfg.get("ranking_drop_threshold", 5)))

    if not all_alerts:
        report = f"# Alerts — {report_date}\n\nNo alerts triggered. All metrics within thresholds."
        log.info("No alerts triggered")
    else:
        log.info(f"{len(all_alerts)} alert(s) triggered, getting Claude analysis")

        alert_text = "\n".join(
            f"- [{a['severity'].upper()}] {a['type']}: {a['message']}"
            for a in all_alerts
        )

        question = (
            f"The following alerts were triggered for {report_date}. "
            f"For each alert, explain the likely cause and recommend "
            f"specific actions to take:\n\n{alert_text}"
        )

        analysis = analyze(SYSTEM_PROMPT, alert_text, question)
        report = (
            f"# Alerts — {report_date}\n\n"
            f"## Triggered Alerts ({len(all_alerts)})\n\n"
            f"{alert_text}\n\n"
            f"## Analysis & Recommendations\n\n"
            f"{analysis}"
        )

        # Send alert summary to Slack
        from ingestion.utils.slack import send_slack, format_alert_summary
        slack_msg = format_alert_summary(all_alerts)
        send_slack(slack_msg)

    if to_stdout:
        print(report)
    else:
        path = _REPORTS_DIR / f"alerts_{report_date}.md"
        path.write_text(report, encoding="utf-8")
        log.info(f"Alerts saved to {path}")

    return report
