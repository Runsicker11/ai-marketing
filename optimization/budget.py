"""AI-powered cross-channel budget intelligence and reallocation recommendations."""

from datetime import date, timedelta
from pathlib import Path

import yaml

from ingestion.utils.bq_client import run_query
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.logger import get_logger
from ingestion.analysis.claude_client import analyze

log = get_logger(__name__)

_DS = f"{GCP_PROJECT_ID}.{BQ_DATASET}"
_REPORTS_DIR = Path(__file__).resolve().parents[1] / "reports"
_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"

SYSTEM_PROMPT = """\
You are a media buyer and budget strategist for Pickleball Effect, a DTC \
pickleball accessories brand with ~$2K/month ad spend across Meta and Google Ads.

You analyze cross-channel performance to recommend budget shifts that maximize \
total ROAS. Your recommendations must:
1. Be specific about dollar amounts (e.g., "Shift $5/day from Campaign X to Campaign Y")
2. Respect budget rules (max 20% daily shift, min $10/campaign)
3. Factor in diminishing returns (increasing spend doesn't linearly increase returns)
4. Consider day-of-week patterns
5. Compare Meta vs Google Ads efficiency
6. Flag any campaigns where spend should be paused or significantly increased

Be data-driven and conservative. Small, testable shifts > big risky moves.\
"""


def _load_budget_rules() -> dict:
    """Load budget rules from thresholds.yaml."""
    path = _CONFIG_DIR / "thresholds.yaml"
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    return cfg.get("budget_rules", {})


def _query_channel_performance() -> str:
    """Get 30-day channel performance summary."""
    sql = f"""
    SELECT channel,
           SUM(spend) AS total_spend,
           SUM(revenue) AS total_revenue,
           SUM(orders) AS total_orders,
           SAFE_DIVIDE(SUM(revenue), SUM(spend)) AS roas,
           SAFE_DIVIDE(SUM(spend), SUM(orders)) AS cpa,
           COUNT(DISTINCT report_date) AS days
    FROM `{_DS}.vw_channel_summary`
    WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND spend IS NOT NULL AND spend > 0
    GROUP BY channel
    ORDER BY total_spend DESC
    """
    rows = list(run_query(sql))
    if not rows:
        return "No channel performance data available."
    header = "channel | spend | revenue | orders | roas | cpa | days"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.channel} | ${r.total_spend or 0:.2f} | "
            f"${r.total_revenue or 0:.2f} | {r.total_orders or 0} | "
            f"{r.roas or 0:.2f} | ${r.cpa or 0:.2f} | {r.days}"
        )
    return "\n".join(lines)


def _query_campaign_performance() -> str:
    """Get campaign-level performance for Meta + Google Ads."""
    sql = f"""
    SELECT platform, campaign_name,
           SUM(spend) AS total_spend,
           SUM(conversion_value) AS total_revenue,
           SUM(conversions) AS total_conversions,
           SAFE_DIVIDE(SUM(conversion_value), SUM(spend)) AS roas,
           SAFE_DIVIDE(SUM(spend), SUM(conversions)) AS cpa,
           COUNT(DISTINCT report_date) AS days_active
    FROM `{_DS}.vw_daily_performance`
    WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND spend > 0
    GROUP BY platform, campaign_name
    ORDER BY total_spend DESC
    """
    rows = list(run_query(sql))
    if not rows:
        return "No campaign performance data available."
    header = "platform | campaign | spend | revenue | conv | roas | cpa | days"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.platform} | {r.campaign_name} | "
            f"${r.total_spend or 0:.2f} | ${r.total_revenue or 0:.2f} | "
            f"{r.total_conversions or 0:.1f} | {r.roas or 0:.2f} | "
            f"${r.cpa or 0:.2f} | {r.days_active}"
        )
    return "\n".join(lines)


def _query_day_of_week_patterns() -> str:
    """Get day-of-week performance patterns."""
    sql = f"""
    SELECT
        FORMAT_DATE('%A', report_date) AS day_name,
        EXTRACT(DAYOFWEEK FROM report_date) AS day_num,
        AVG(spend) AS avg_spend,
        AVG(conversion_value) AS avg_revenue,
        AVG(SAFE_DIVIDE(conversion_value, spend)) AS avg_roas,
        SUM(conversions) AS total_conversions
    FROM `{_DS}.vw_daily_performance`
    WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND spend > 0
    GROUP BY day_name, day_num
    ORDER BY day_num
    """
    rows = list(run_query(sql))
    if not rows:
        return "No day-of-week data available."
    header = "day | avg_spend | avg_revenue | avg_roas | total_conv"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.day_name} | ${r.avg_spend or 0:.2f} | "
            f"${r.avg_revenue or 0:.2f} | {r.avg_roas or 0:.2f} | "
            f"{r.total_conversions or 0:.1f}"
        )
    return "\n".join(lines)


def _query_spend_trend() -> str:
    """Get daily spend trend for last 30 days."""
    sql = f"""
    SELECT report_date, channel,
           spend, revenue, roas, orders
    FROM `{_DS}.vw_channel_summary`
    WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND spend IS NOT NULL AND spend > 0
    ORDER BY report_date, channel
    """
    rows = list(run_query(sql))
    if not rows:
        return "No spend trend data available."
    header = "date | channel | spend | revenue | roas | orders"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.report_date} | {r.channel} | "
            f"${r.spend or 0:.2f} | ${r.revenue or 0:.2f} | "
            f"{r.roas or 0:.2f} | {r.orders or 0}"
        )
    return "\n".join(lines)


def recommend(to_stdout: bool = False) -> str:
    """Generate budget reallocation recommendations.

    Returns:
        The budget recommendations report text.
    """
    log.info("Generating budget intelligence report")

    budget_rules = _load_budget_rules()
    channel_data = _query_channel_performance()
    campaign_data = _query_campaign_performance()
    dow_data = _query_day_of_week_patterns()
    trend_data = _query_spend_trend()

    data_context = (
        f"### Channel Performance (Last 30 Days)\n{channel_data}\n\n"
        f"### Campaign Performance (Last 30 Days)\n{campaign_data}\n\n"
        f"### Day-of-Week Patterns\n{dow_data}\n\n"
        f"### Daily Spend Trend\n{trend_data}\n\n"
        f"### Budget Rules\n"
        f"- Max daily shift: {budget_rules.get('max_daily_shift_pct', 20)}%\n"
        f"- Min campaign daily spend: ${budget_rules.get('min_campaign_daily_spend', 10)}\n"
        f"- Min days before decision: {budget_rules.get('min_days_before_decision', 7)}\n"
    )

    question = (
        f"Given 30 days of cross-channel data, recommend specific budget "
        f"allocation changes for this week. Include:\n"
        f"1. Channel-level shifts (Meta vs Google Ads)\n"
        f"2. Campaign-level shifts within each platform\n"
        f"3. Day-of-week optimization (increase/decrease spend by day)\n"
        f"4. Any campaigns to pause or scale\n\n"
        f"Monthly budget is ~$2,000 total. Today is {date.today()}."
    )

    report = analyze(SYSTEM_PROMPT, data_context, question)

    full_report = (
        f"# Budget Intelligence Report\n"
        f"## {date.today()}\n\n"
        f"{report}"
    )

    if to_stdout:
        print(full_report)
    else:
        _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        path = _REPORTS_DIR / f"budget_recommendations_{date.today()}.md"
        path.write_text(full_report, encoding="utf-8")
        log.info(f"Budget recommendations saved to {path}")

    return full_report
