"""Generate weekly strategy report with Claude analysis."""

from datetime import date, timedelta
from pathlib import Path

from ingestion.utils.bq_client import run_query
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.logger import get_logger
from ingestion.analysis.claude_client import analyze

log = get_logger(__name__)

_REPORTS_DIR = Path(__file__).resolve().parents[2] / "reports"
_DS = f"{GCP_PROJECT_ID}.{BQ_DATASET}"

SYSTEM_PROMPT = """\
You are a senior marketing strategist for a DTC pickleball accessories brand. \
You produce weekly strategy reports that guide the next week's ad spend, \
creative direction, and product focus.

Report format:
1. **Week in Review** (key wins, losses, overall trajectory)
2. **Channel Strategy** (which channels to invest more/less in, with data backing)
3. **Budget Recommendations** (specific $ shifts between channels/campaigns)
4. **Product Strategy** (which products to push, which to deprioritize, based on funnel data)
5. **Funnel Optimization** (biggest drop-off points, specific fixes to test)
6. **Creative Direction** (what's working, what's fatigued, what to test next)
7. **Competitive Context** (how our metrics compare to expectations)
8. **Next Week Priorities** (top 3-5 specific actions ranked by expected impact)

Use exact numbers. Be specific about dollar amounts and percentages. \
Recommend bold moves when the data supports them, but flag risks clearly.\
"""


def _query_weekly_roas(week_start: date, week_end: date) -> str:
    sql = f"""
    SELECT report_date, channel, ga4_attributed_revenue, ga4_attributed_orders,
           ad_spend, enhanced_roas
    FROM `{_DS}.vw_enhanced_roas`
    WHERE report_date BETWEEN '{week_start}' AND '{week_end}'
    ORDER BY report_date, channel
    """
    rows = list(run_query(sql))
    if not rows:
        return "No ROAS data for this week."
    header = "date | channel | revenue | orders | spend | roas"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.report_date} | {r.channel} | "
            f"${r.ga4_attributed_revenue or 0:.2f} | "
            f"{r.ga4_attributed_orders or 0} | "
            f"${r.ad_spend or 0:.2f} | "
            f"{r.enhanced_roas or 0:.2f}"
        )
    return "\n".join(lines)


def _query_weekly_funnel(week_start: date, week_end: date) -> str:
    sql = f"""
    SELECT source, medium,
           SUM(sessions) AS sessions,
           SUM(product_views) AS product_views,
           SUM(add_to_carts) AS add_to_carts,
           SUM(checkouts) AS checkouts,
           SUM(purchases) AS purchases,
           SAFE_DIVIDE(SUM(add_to_carts), SUM(product_views)) AS view_to_cart_rate,
           SAFE_DIVIDE(SUM(purchases), SUM(add_to_carts)) AS cart_to_purchase_rate,
           SAFE_DIVIDE(SUM(purchases), SUM(sessions)) AS overall_conversion_rate
    FROM `{_DS}.vw_ga4_funnel`
    WHERE report_date BETWEEN '{week_start}' AND '{week_end}'
    GROUP BY source, medium
    ORDER BY sessions DESC
    LIMIT 10
    """
    rows = list(run_query(sql))
    if not rows:
        return "No funnel data for this week."
    header = "source | medium | sessions | views | carts | checkouts | purchases | v>c | c>p | cvr"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.source} | {r.medium} | {r.sessions} | "
            f"{r.product_views} | {r.add_to_carts} | "
            f"{r.checkouts} | {r.purchases} | "
            f"{r.view_to_cart_rate or 0:.1%} | "
            f"{r.cart_to_purchase_rate or 0:.1%} | "
            f"{r.overall_conversion_rate or 0:.1%}"
        )
    return "\n".join(lines)


def _query_weekly_trends(week_start: date, week_end: date) -> str:
    sql = f"""
    SELECT report_date, meta_spend, shopify_meta_revenue, true_roas,
           spend_7d_avg, revenue_7d_avg, roas_7d_avg
    FROM `{_DS}.vw_trends`
    WHERE report_date BETWEEN '{week_start}' AND '{week_end}'
    ORDER BY report_date
    """
    rows = list(run_query(sql))
    if not rows:
        return "No trend data for this week."
    header = "date | spend | revenue | roas | spend_7d | rev_7d | roas_7d"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.report_date} | ${r.meta_spend or 0:.2f} | "
            f"${r.shopify_meta_revenue or 0:.2f} | "
            f"{r.true_roas or 0:.2f} | "
            f"${r.spend_7d_avg or 0:.2f} | "
            f"${r.revenue_7d_avg or 0:.2f} | "
            f"{r.roas_7d_avg or 0:.2f}"
        )
    return "\n".join(lines)


def _query_weekly_ads(week_start: date, week_end: date) -> str:
    sql = f"""
    SELECT ad_name, campaign_name,
           SUM(spend) AS spend,
           SUM(conversions) AS conversions,
           SUM(conversion_value) AS conversion_value,
           SAFE_DIVIDE(SUM(conversion_value), SUM(spend)) AS ad_roas,
           SUM(impressions) AS impressions,
           SUM(clicks) AS clicks,
           SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100 AS ctr
    FROM `{_DS}.vw_daily_performance`
    WHERE report_date BETWEEN '{week_start}' AND '{week_end}'
        AND spend > 0
    GROUP BY ad_name, campaign_name
    ORDER BY conversion_value DESC
    LIMIT 15
    """
    rows = list(run_query(sql))
    if not rows:
        return "No ad data for this week."
    header = "ad | campaign | spend | conv | value | roas | impr | clicks | ctr"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.ad_name} | {r.campaign_name} | "
            f"${r.spend or 0:.2f} | {r.conversions or 0} | "
            f"${r.conversion_value or 0:.2f} | "
            f"{r.ad_roas or 0:.2f} | "
            f"{r.impressions or 0} | {r.clicks or 0} | "
            f"{r.ctr or 0:.2f}%"
        )
    return "\n".join(lines)


def _query_product_insights() -> str:
    sql = f"""
    SELECT item_name, product_views, add_to_carts, purchases, revenue,
           view_to_cart_rate, cart_to_purchase_rate, avg_price
    FROM `{_DS}.vw_ga4_product_insights`
    ORDER BY product_views DESC
    LIMIT 15
    """
    rows = list(run_query(sql))
    if not rows:
        return "No product insight data available."
    header = "product | views | carts | purchases | revenue | v>c | c>p | avg_price"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.item_name} | {r.product_views} | "
            f"{r.add_to_carts} | {r.purchases} | "
            f"${r.revenue or 0:.2f} | "
            f"{r.view_to_cart_rate or 0:.1%} | "
            f"{r.cart_to_purchase_rate or 0:.1%} | "
            f"${r.avg_price or 0:.2f}"
        )
    return "\n".join(lines)


def generate(week_end: date | None = None, to_stdout: bool = False) -> str:
    """Generate weekly strategy report.

    Args:
        week_end: Last day of the reporting week (default: last Sunday).
        to_stdout: If True, print report instead of saving to file.

    Returns:
        The generated strategy report text.
    """
    if week_end is None:
        today = date.today()
        # Default to last Sunday
        week_end = today - timedelta(days=today.weekday() + 1)

    week_start = week_end - timedelta(days=6)
    log.info(f"Generating weekly strategy for {week_start} to {week_end}")

    roas_data = _query_weekly_roas(week_start, week_end)
    funnel_data = _query_weekly_funnel(week_start, week_end)
    trends_data = _query_weekly_trends(week_start, week_end)
    ads_data = _query_weekly_ads(week_start, week_end)
    product_data = _query_product_insights()

    data_context = (
        f"### Weekly ROAS by Channel ({week_start} to {week_end})\n{roas_data}\n\n"
        f"### Weekly Funnel by Source\n{funnel_data}\n\n"
        f"### Daily Trends\n{trends_data}\n\n"
        f"### Ad Performance (Top 15)\n{ads_data}\n\n"
        f"### Product Insights (Last 90 Days)\n{product_data}"
    )

    question = (
        f"Produce the weekly strategy report for {week_start} through {week_end}. "
        f"Today is {date.today()}. "
        f"Monthly ad budget is ~$2,000 across Meta and Google. "
        f"Analyze all the data and follow the report format exactly. "
        f"Be specific about dollar amounts for budget recommendations."
    )

    report = analyze(SYSTEM_PROMPT, data_context, question)

    full_report = (
        f"# Weekly Strategy Report\n"
        f"## {week_start} — {week_end}\n\n"
        f"{report}"
    )

    if to_stdout:
        print(full_report)
    else:
        path = _REPORTS_DIR / f"weekly_{week_end}.md"
        path.write_text(full_report, encoding="utf-8")
        log.info(f"Weekly report saved to {path}")

    return full_report
