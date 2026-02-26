"""Generate daily marketing performance report using BigQuery + Claude."""

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
You are an expert digital marketing analyst for a DTC ecommerce brand selling \
pickleball accessories. You analyze daily performance data and produce concise, \
actionable reports.

Report format:
1. **Executive Summary** (2-3 sentences: best headline stat, worst, overall trend)
2. **Channel Performance** (ROAS by channel, spend efficiency)
3. **Funnel Analysis** (where are we losing people?)
4. **Product Highlights** (top sellers, underperformers, opportunities)
5. **Ad Creative Performance** (top/bottom ads by ROAS or conversions)
6. **Trends** (DoD and WoW changes, rolling averages)
7. **Recommendations** (3-5 specific, actionable next steps)

Use exact numbers from the data. Be direct and specific — no filler. \
Flag anything unusual or concerning. Compare today vs recent averages.\
"""


def _query_enhanced_roas(report_date: date) -> str:
    sql = f"""
    SELECT report_date, channel, ga4_attributed_revenue, ga4_attributed_orders,
           ad_spend, enhanced_roas
    FROM `{_DS}.vw_enhanced_roas`
    WHERE report_date BETWEEN '{report_date - timedelta(days=7)}' AND '{report_date}'
    ORDER BY report_date DESC, ga4_attributed_revenue DESC
    """
    rows = list(run_query(sql))
    if not rows:
        return "No enhanced ROAS data available."
    header = "report_date | channel | revenue | orders | spend | roas"
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


def _query_funnel(report_date: date) -> str:
    sql = f"""
    SELECT source, medium, sessions, product_views, add_to_carts,
           checkouts, purchases, view_to_cart_rate, cart_to_purchase_rate,
           overall_conversion_rate
    FROM `{_DS}.vw_ga4_funnel`
    WHERE report_date = '{report_date}'
    ORDER BY sessions DESC
    LIMIT 10
    """
    rows = list(run_query(sql))
    if not rows:
        return "No funnel data available for this date."
    header = "source | medium | sessions | views | carts | checkouts | purchases | view>cart | cart>purch | cvr"
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


def _query_trends(report_date: date) -> str:
    sql = f"""
    SELECT report_date, meta_spend, shopify_meta_revenue, true_roas,
           spend_7d_avg, revenue_7d_avg, roas_7d_avg,
           spend_dod_change, revenue_dod_change,
           spend_wow_change, revenue_wow_change
    FROM `{_DS}.vw_trends`
    WHERE report_date BETWEEN '{report_date - timedelta(days=7)}' AND '{report_date}'
    ORDER BY report_date DESC
    """
    rows = list(run_query(sql))
    if not rows:
        return "No trend data available."
    header = "date | spend | revenue | roas | spend_7d | rev_7d | roas_7d | spend_dod | rev_dod"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.report_date} | ${r.meta_spend or 0:.2f} | "
            f"${r.shopify_meta_revenue or 0:.2f} | "
            f"{r.true_roas or 0:.2f} | "
            f"${r.spend_7d_avg or 0:.2f} | "
            f"${r.revenue_7d_avg or 0:.2f} | "
            f"{r.roas_7d_avg or 0:.2f} | "
            f"${r.spend_dod_change or 0:.2f} | "
            f"${r.revenue_dod_change or 0:.2f}"
        )
    return "\n".join(lines)


def _query_top_ads(report_date: date) -> str:
    sql = f"""
    SELECT ad_name, campaign_name, spend, conversions, conversion_value,
           SAFE_DIVIDE(conversion_value, spend) AS ad_roas,
           impressions, clicks, ctr
    FROM `{_DS}.vw_daily_performance`
    WHERE report_date = '{report_date}' AND spend > 0
    ORDER BY conversion_value DESC
    LIMIT 10
    """
    rows = list(run_query(sql))
    if not rows:
        return "No ad performance data for this date."
    header = "ad_name | campaign | spend | conversions | value | roas | impr | clicks | ctr"
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
    header = "product | views | carts | purchases | revenue | v>c rate | c>p rate | avg_price"
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


def generate(report_date: date | None = None, to_stdout: bool = False) -> str:
    """Generate daily marketing report.

    Args:
        report_date: Date to report on (default: yesterday).
        to_stdout: If True, print report instead of saving to file.

    Returns:
        The generated report text.
    """
    if report_date is None:
        report_date = date.today() - timedelta(days=1)

    log.info(f"Generating daily report for {report_date}")

    # Query all data sources
    roas_data = _query_enhanced_roas(report_date)
    funnel_data = _query_funnel(report_date)
    trends_data = _query_trends(report_date)
    ads_data = _query_top_ads(report_date)
    product_data = _query_product_insights()

    data_context = (
        f"### Enhanced ROAS by Channel (last 7 days)\n{roas_data}\n\n"
        f"### Funnel by Source ({report_date})\n{funnel_data}\n\n"
        f"### Trends (last 7 days)\n{trends_data}\n\n"
        f"### Top Ads ({report_date})\n{ads_data}\n\n"
        f"### Product Insights (last 90 days)\n{product_data}"
    )

    question = (
        f"Produce the daily marketing performance report for {report_date}. "
        f"Today is {date.today()}. "
        f"Analyze all the data provided and follow the report format exactly."
    )

    report = analyze(SYSTEM_PROMPT, data_context, question)

    # Add header
    full_report = f"# Daily Marketing Report — {report_date}\n\n{report}"

    if to_stdout:
        print(full_report)
    else:
        path = _REPORTS_DIR / f"daily_{report_date}.md"
        path.write_text(full_report, encoding="utf-8")
        log.info(f"Report saved to {path}")

    return full_report
