"""Generate monthly HTML dashboard report with Highcharts visualizations.

Pure data dashboard — no AI calls. All queries use dbt compat views.
"""

import calendar
import json
from datetime import date, datetime, timedelta
from pathlib import Path

from ingestion.utils.bq_client import run_query
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.logger import get_logger
from ingestion.analysis.dashboard_template import DASHBOARD_HTML

log = get_logger(__name__)

_REPORTS_DIR = Path(__file__).resolve().parents[2] / "reports"
_DS = f"{GCP_PROJECT_ID}.{BQ_DATASET}"

AGENCY_FEE = 3_000
SHIPPING_COST_PER_ORDER = 5.35


# ── Month parsing ────────────────────────────────────────────────────────────

def _parse_month(month_str: str | None) -> tuple[date, date, date, date]:
    """Return (target_start, target_end, prior_start, prior_end).

    If *month_str* is None, defaults to the last fully completed month.
    """
    if month_str:
        year, mon = (int(p) for p in month_str.split("-"))
    else:
        today = date.today()
        first_of_this = today.replace(day=1)
        last_month_end = first_of_this - timedelta(days=1)
        year, mon = last_month_end.year, last_month_end.month

    _, last_day = calendar.monthrange(year, mon)
    target_start = date(year, mon, 1)
    target_end = date(year, mon, last_day)

    # Prior month
    if mon == 1:
        p_year, p_mon = year - 1, 12
    else:
        p_year, p_mon = year, mon - 1
    _, p_last = calendar.monthrange(p_year, p_mon)
    prior_start = date(p_year, p_mon, 1)
    prior_end = date(p_year, p_mon, p_last)

    return target_start, target_end, prior_start, prior_end


def _month_label(d: date) -> str:
    return d.strftime("%B %Y")


# ── Trailing window helpers ──────────────────────────────────────────────────

def _trailing_months_start(n: int = 6) -> date:
    """First day of the month (n-1) months before the current month.

    Combined with GENERATE_DATE_ARRAY(..., CURRENT_DATE(), INTERVAL 1 MONTH)
    this yields n months inclusive ((n-1) prior + current partial).
    """
    today = date.today()
    m, y = today.month - (n - 1), today.year
    while m <= 0:
        m += 12
        y -= 1
    return date(y, m, 1)


def _trailing_13_week_start() -> date:
    """Monday of the week 13 weeks before the current week."""
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    return monday - timedelta(weeks=13)


# ── Query helpers ────────────────────────────────────────────────────────────

def _query_kpis(t_start, t_end, p_start, p_end) -> dict:
    """Aggregate KPIs for target and prior months. Uses CAC (spend/new customers)."""
    sql = f"""
    WITH monthly AS (
        SELECT
            CASE
                WHEN report_date BETWEEN '{t_start}' AND '{t_end}' THEN 'target'
                ELSE 'prior'
            END AS period,
            SUM(spend) AS spend,
            SUM(revenue) AS revenue,
            SUM(orders) AS orders
        FROM `{_DS}.vw_channel_summary`
        WHERE report_date BETWEEN '{p_start}' AND '{t_end}'
        GROUP BY period
    ),
    funnel AS (
        SELECT
            CASE
                WHEN report_date BETWEEN '{t_start}' AND '{t_end}' THEN 'target'
                ELSE 'prior'
            END AS period,
            SUM(sessions) AS sessions,
            SUM(purchases) AS purchases
        FROM `{_DS}.vw_ga4_funnel`
        WHERE report_date BETWEEN '{p_start}' AND '{t_end}'
        GROUP BY period
    ),
    all_cust AS (
        SELECT
            CASE
                WHEN o.order_date BETWEEN '{t_start}' AND '{t_end}' THEN 'target'
                ELSE 'prior'
            END AS period,
            COUNT(DISTINCT o.customer_id) AS total_customers
        FROM `{GCP_PROJECT_ID}.stg_shopify.stg_shopify__orders` o
        WHERE o.customer_id IS NOT NULL
          AND o.order_date BETWEEN '{p_start}' AND '{t_end}'
        GROUP BY period
    ),
    new_cust AS (
        SELECT
            CASE
                WHEN first_order_date BETWEEN '{t_start}' AND '{t_end}' THEN 'target'
                ELSE 'prior'
            END AS period,
            COUNT(DISTINCT customer_id) AS new_customers
        FROM (
            SELECT customer_id, MIN(order_date) AS first_order_date
            FROM `{GCP_PROJECT_ID}.stg_shopify.stg_shopify__orders`
            WHERE customer_id IS NOT NULL
            GROUP BY customer_id
        )
        WHERE first_order_date BETWEEN '{p_start}' AND '{t_end}'
        GROUP BY period
    ),
    cogs_data AS (
        SELECT
            CASE
                WHEN order_date BETWEEN '{t_start}' AND '{t_end}' THEN 'target'
                ELSE 'prior'
            END AS period,
            SUM(net_revenue) AS product_net_revenue,
            SUM(total_cogs) AS cogs
        FROM `{_DS}.vw_product_profitability`
        WHERE order_date BETWEEN '{p_start}' AND '{t_end}'
        GROUP BY period
    ),
    order_counts AS (
        SELECT
            CASE
                WHEN order_date BETWEEN '{t_start}' AND '{t_end}' THEN 'target'
                ELSE 'prior'
            END AS period,
            COUNT(*) AS order_count
        FROM `{GCP_PROJECT_ID}.stg_shopify.stg_shopify__orders`
        WHERE order_date BETWEEN '{p_start}' AND '{t_end}'
        GROUP BY period
    )
    SELECT
        m.period,
        m.spend, m.revenue, m.orders,
        SAFE_DIVIDE(m.revenue, m.spend) AS roas,
        SAFE_DIVIDE(m.spend, nc.new_customers) AS cac,
        SAFE_DIVIDE(f.purchases, f.sessions) AS cvr,
        SAFE_DIVIDE(m.revenue, m.orders) AS aov,
        COALESCE(ac.total_customers, 0) AS total_customers,
        COALESCE(nc.new_customers, 0) AS new_customers,
        COALESCE(ac.total_customers, 0) - COALESCE(nc.new_customers, 0) AS returning_customers,
        SAFE_DIVIDE(m.revenue, m.spend + {AGENCY_FEE}) AS mer,
        SAFE_DIVIDE(
            COALESCE(cd.product_net_revenue, 0) - COALESCE(cd.cogs, 0)
            - (COALESCE(oc.order_count, 0) * {SHIPPING_COST_PER_ORDER})
            - m.spend - {AGENCY_FEE},
            COALESCE(oc.order_count, 0)
        ) AS contrib_margin
    FROM monthly m
    LEFT JOIN funnel f ON m.period = f.period
    LEFT JOIN new_cust nc ON m.period = nc.period
    LEFT JOIN all_cust ac ON m.period = ac.period
    LEFT JOIN cogs_data cd ON m.period = cd.period
    LEFT JOIN order_counts oc ON m.period = oc.period
    """
    rows = {r.period: r for r in run_query(sql)}
    t = rows.get("target")
    p = rows.get("prior")

    def _val(row, col):
        return float(row[col] or 0) if row else 0.0

    def _int_val(row, col):
        return int(row[col] or 0) if row else 0

    result = {}
    for key, fmt, getter in [
        ("revenue", "${:,.0f}", lambda r: _val(r, "revenue")),
        ("aov", "${:,.2f}", lambda r: _val(r, "aov")),
        ("contrib_margin", "${:,.2f}", lambda r: _val(r, "contrib_margin")),
        ("orders", "{:,}", lambda r: _int_val(r, "orders")),
        ("roas", "{:.2f}x", lambda r: _val(r, "roas")),
        ("mer", "{:.2f}x", lambda r: _val(r, "mer")),
        ("cvr", "{:.1%}", lambda r: _val(r, "cvr")),
        ("total_customers", "{:,}", lambda r: _int_val(r, "total_customers")),
        ("new_customers", "{:,}", lambda r: _int_val(r, "new_customers")),
        ("returning_customers", "{:,}", lambda r: _int_val(r, "returning_customers")),
        ("cac", "${:,.2f}", lambda r: _val(r, "cac")),
    ]:
        tv = getter(t)
        pv = getter(p)
        result[f"kpi_{key}"] = fmt.format(tv)

        # MoM delta
        if pv:
            pct = (tv - pv) / abs(pv) * 100
        else:
            pct = 0.0

        # For CAC, lower is better — flip the color logic
        invert = key == "cac"
        if abs(pct) < 0.5:
            cls = "neutral"
        elif (pct > 0) != invert:
            cls = "positive"
        else:
            cls = "negative"

        sign = "+" if pct > 0 else ""
        result[f"delta_{key}"] = f"{sign}{pct:.1f}%"
        result[f"delta_{key}_class"] = cls

    return result


def _query_pnl_trailing() -> dict:
    """Monthly P&L for trailing 6 months.

    Top-line = Shopify gross sales (line item price x qty) + shipping collected.
    This matches the Shopify Analytics "Total Sales" number.
    Partnership revenue (Tuning Clamps 60% share) shown as a deduction.
    Shipping cost = $5.35 x orders. Agency fee = $3,000/mo.
    """
    start = _trailing_months_start(6)
    sql = f"""
    WITH months AS (
        SELECT FORMAT_DATE('%Y-%m', d) AS month_key, d AS month_start,
               LAST_DAY(d) AS month_end
        FROM UNNEST(GENERATE_DATE_ARRAY('{start}', CURRENT_DATE(), INTERVAL 1 MONTH)) d
    ),
    product_monthly AS (
        SELECT
            FORMAT_DATE('%Y-%m', order_date) AS month_key,
            SUM(gross_revenue) AS gross_revenue,
            SUM(total_discounts) AS product_discounts,
            SUM(net_revenue) AS net_revenue,
            SUM(total_cogs) AS cogs,
            -- Partnership slice: gross - net for partnership items only
            SUM(CASE WHEN is_partnership
                THEN gross_revenue - total_discounts - net_revenue
                ELSE 0 END) AS partnership_slice
        FROM `{_DS}.vw_product_profitability`
        WHERE order_date >= '{start}'
        GROUP BY month_key
    ),
    orders_monthly AS (
        SELECT
            FORMAT_DATE('%Y-%m', order_date) AS month_key,
            COUNT(*) AS order_count,
            SUM(total_shipping) AS shipping_collected,
            SUM(total_discounts) AS order_discounts,
            SUM(total_tax) AS tax
        FROM `{GCP_PROJECT_ID}.stg_shopify.stg_shopify__orders`
        WHERE order_date >= '{start}'
        GROUP BY month_key
    ),
    spend_monthly AS (
        SELECT
            FORMAT_DATE('%Y-%m', report_date) AS month_key,
            SUM(COALESCE(spend, 0)) AS ad_spend
        FROM `{_DS}.vw_channel_summary`
        WHERE report_date >= '{start}'
        GROUP BY month_key
    )
    SELECT
        m.month_key,
        COALESCE(p.gross_revenue, 0) AS gross_revenue,
        COALESCE(o.order_discounts, 0) AS discounts,
        COALESCE(o.shipping_collected, 0) AS shipping_collected,
        COALESCE(o.order_count, 0) AS order_count,
        COALESCE(p.partnership_slice, 0) AS partnership_slice,
        COALESCE(p.cogs, 0) AS cogs,
        COALESCE(sp.ad_spend, 0) AS ad_spend
    FROM months m
    LEFT JOIN product_monthly p ON m.month_key = p.month_key
    LEFT JOIN orders_monthly o ON m.month_key = o.month_key
    LEFT JOIN spend_monthly sp ON m.month_key = sp.month_key
    ORDER BY m.month_key
    """
    rows_list = list(run_query(sql))

    months = []
    data_rows = []
    totals = {
        "gross_revenue": 0, "discounts": 0, "shipping_collected": 0,
        "total_revenue": 0, "partnership_slice": 0, "net_revenue": 0,
        "cogs": 0, "shipping_cost": 0,
        "gross_profit": 0, "ad_spend": 0,
        "agency_fee": 0, "bottom_line": 0,
    }

    for r in rows_list:
        mk = r.month_key
        months.append(mk)
        gross_rev = float(r.gross_revenue or 0)
        discounts = float(r.discounts or 0)
        shipping_collected = float(r.shipping_collected or 0)
        total_rev = gross_rev - discounts + shipping_collected
        partnership_slice = float(r.partnership_slice or 0)
        net_rev = total_rev - partnership_slice
        cogs = float(r.cogs or 0)
        order_count = int(r.order_count or 0)
        shipping_cost = round(order_count * SHIPPING_COST_PER_ORDER, 2)
        gross_profit = net_rev - cogs - shipping_cost
        ad_spend = float(r.ad_spend or 0)
        agency_fee = AGENCY_FEE
        bottom_line = gross_profit - ad_spend - agency_fee

        row = {
            "month": mk,
            "gross_revenue": round(gross_rev, 2),
            "discounts": round(discounts, 2),
            "shipping_collected": round(shipping_collected, 2),
            "total_revenue": round(total_rev, 2),
            "partnership_slice": round(partnership_slice, 2),
            "net_revenue": round(net_rev, 2),
            "cogs": round(cogs, 2),
            "shipping_cost": shipping_cost,
            "gross_profit": round(gross_profit, 2),
            "ad_spend": round(ad_spend, 2),
            "agency_fee": agency_fee,
            "bottom_line": round(bottom_line, 2),
        }
        data_rows.append(row)

        for k in ["gross_revenue", "discounts", "shipping_collected",
                   "total_revenue", "partnership_slice", "net_revenue",
                   "cogs", "shipping_cost", "gross_profit", "ad_spend"]:
            totals[k] += row[k]
        totals["agency_fee"] += agency_fee

    totals["bottom_line"] = round(
        totals["gross_profit"] - totals["ad_spend"] - totals["agency_fee"], 2
    )
    for k in totals:
        if k != "bottom_line":
            totals[k] = round(totals[k], 2)

    return {"months": months, "rows": data_rows, "totals": totals}


def _query_avg_cltv() -> float:
    """Average 6-month LTV across all mature cohorts from vw_customer_ltv."""
    sql = f"""
    SELECT
        SAFE_DIVIDE(SUM(total_gross_profit_6mo), SUM(cohort_size)) AS avg_ltv
    FROM `{_DS}.vw_customer_ltv`
    """
    rows = list(run_query(sql))
    if rows and rows[0].avg_ltv:
        return round(float(rows[0].avg_ltv), 2)
    return 0.0


def _query_channel_kpis(t_start, t_end, p_start, p_end) -> dict:
    """Full KPI breakdown by channel segment for both months.

    Channels: Meta, Search Brand, Search Non-Brand, Shopping, Email.
    KPIs: spend, clicks, impressions, CTR, conversions, revenue, ROAS, CPA, AOV.
    """
    sql = f"""
    WITH raw_data AS (
        -- Meta from vw_daily_performance
        SELECT
            report_date,
            'Meta' AS channel,
            spend, impressions, clicks, ctr,
            conversions, conversion_value AS revenue
        FROM `{_DS}.vw_daily_performance`
        WHERE report_date BETWEEN '{p_start}' AND '{t_end}'
          AND platform = 'meta'

        UNION ALL

        -- Google Ads split by campaign
        SELECT
            report_date,
            CASE
                WHEN campaign_name = 'Brand' THEN 'Search Brand'
                WHEN campaign_name LIKE 'Shopping%%' THEN 'Shopping'
                ELSE 'Search Non-Brand'
            END AS channel,
            spend, impressions, clicks, ctr,
            conversions, conversion_value AS revenue
        FROM `{_DS}.vw_daily_performance`
        WHERE report_date BETWEEN '{p_start}' AND '{t_end}'
          AND platform = 'google_ads'
    ),
    email_data AS (
        -- Email from GA4 funnel (Klaviyo / email medium)
        SELECT
            report_date,
            'Email' AS channel,
            SUM(sessions) AS sessions,
            SUM(purchases) AS purchases,
            SUM(add_to_carts) AS add_to_carts
        FROM `{_DS}.vw_ga4_funnel`
        WHERE report_date BETWEEN '{p_start}' AND '{t_end}'
          AND (source = 'klaviyo' OR medium = 'email')
        GROUP BY report_date
    ),
    email_rev AS (
        -- Email revenue from GA4 attribution
        SELECT
            order_date AS report_date,
            'Email' AS channel,
            SUM(shopify_revenue) AS revenue,
            COUNT(*) AS orders
        FROM `{_DS}.vw_ga4_attribution`
        WHERE order_date BETWEEN '{p_start}' AND '{t_end}'
          AND (ga4_source = 'klaviyo' OR ga4_medium = 'email')
        GROUP BY order_date
    ),
    ad_agg AS (
        SELECT
            CASE
                WHEN report_date BETWEEN '{t_start}' AND '{t_end}' THEN 'target'
                ELSE 'prior'
            END AS period,
            channel,
            SUM(spend) AS spend,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(conversions) AS conversions,
            SUM(revenue) AS revenue
        FROM raw_data
        GROUP BY period, channel
    ),
    email_agg AS (
        SELECT
            CASE
                WHEN ed.report_date BETWEEN '{t_start}' AND '{t_end}' THEN 'target'
                ELSE 'prior'
            END AS period,
            'Email' AS channel,
            0 AS spend,
            0 AS impressions,
            SUM(ed.sessions) AS clicks,
            SUM(COALESCE(er.orders, 0)) AS conversions,
            SUM(COALESCE(er.revenue, 0)) AS revenue
        FROM email_data ed
        LEFT JOIN email_rev er ON ed.report_date = er.report_date
        GROUP BY period
    )
    SELECT * FROM ad_agg
    UNION ALL
    SELECT * FROM email_agg
    ORDER BY channel, period
    """
    rows = list(run_query(sql))

    channels = ["Meta", "Search Brand", "Search Non-Brand", "Shopping", "Email"]
    result = {}
    for ch in channels:
        result[ch] = {"target": {}, "prior": {}}

    for r in rows:
        ch = r.channel
        if ch not in result:
            continue
        period = r.period
        spend = float(r.spend or 0)
        impressions = int(r.impressions or 0)
        clicks = int(r.clicks or 0)
        conversions = float(r.conversions or 0)
        revenue = float(r.revenue or 0)

        result[ch][period] = {
            "spend": round(spend, 2),
            "impressions": impressions,
            "clicks": clicks,
            "ctr": round(clicks / impressions * 100, 2) if impressions else 0,
            "conversions": round(conversions, 1),
            "revenue": round(revenue, 2),
            "roas": round(revenue / spend, 2) if spend else 0,
            "cpa": round(spend / conversions, 2) if conversions else 0,
            "aov": round(revenue / conversions, 2) if conversions else 0,
        }

    return {"channels": channels, "data": result}


def _query_new_customers(t_start, t_end, p_start, p_end) -> dict:
    """New customers per channel (first Shopify order in period).

    Uses GA4 attribution to map orders to channels, then checks if the
    customer's first-ever Shopify order falls in the target/prior month.
    """
    sql = f"""
    WITH first_orders AS (
        -- Find each customer's first order date
        SELECT customer_id, MIN(order_date) AS first_order_date
        FROM `{GCP_PROJECT_ID}.stg_shopify.stg_shopify__orders`
        WHERE customer_id IS NOT NULL
        GROUP BY customer_id
    ),
    new_in_period AS (
        -- Orders where the customer's first order falls in target or prior month
        SELECT
            o.order_id,
            o.order_date,
            o.customer_id,
            CASE
                WHEN o.order_date BETWEEN '{t_start}' AND '{t_end}' THEN 'target'
                ELSE 'prior'
            END AS period
        FROM `{GCP_PROJECT_ID}.stg_shopify.stg_shopify__orders` o
        JOIN first_orders fo ON o.customer_id = fo.customer_id
            AND o.order_date = fo.first_order_date
        WHERE o.order_date BETWEEN '{p_start}' AND '{t_end}'
    ),
    attributed AS (
        -- Join with GA4 attribution to get channel
        SELECT
            np.period,
            CASE
                WHEN a.ga4_medium = 'cpc' AND a.ga4_campaign = 'Brand' THEN 'Search Brand'
                WHEN a.ga4_medium = 'cpc' AND a.ga4_campaign LIKE 'Shopping%%' THEN 'Shopping'
                WHEN a.ga4_medium = 'cpc' THEN 'Search Non-Brand'
                WHEN a.ga4_source IN ('facebook', 'ig') OR a.ga4_medium = 'paid_social'
                    OR a.ga4_source LIKE '%%instagram%%' OR a.ga4_source LIKE '%%facebook%%'
                    THEN 'Meta'
                WHEN a.ga4_source = 'klaviyo' OR a.ga4_medium = 'email' THEN 'Email'
                ELSE 'Other'
            END AS channel,
            np.customer_id
        FROM new_in_period np
        LEFT JOIN `{_DS}.vw_ga4_attribution` a
            ON np.order_id = a.order_id
    )
    SELECT period, channel, COUNT(DISTINCT customer_id) AS new_customers
    FROM attributed
    GROUP BY period, channel
    ORDER BY period, channel
    """
    rows = list(run_query(sql))

    channels = ["Meta", "Search Brand", "Search Non-Brand", "Shopping", "Email", "Other"]
    result = {ch: {"target": 0, "prior": 0} for ch in channels}

    for r in rows:
        ch = r.channel
        if ch in result:
            result[ch][r.period] = int(r.new_customers or 0)

    return result


def _query_sessions_cvr(t_start, t_end, p_start, p_end) -> dict:
    """Sessions and CVR by channel from GA4 funnel.

    Breaks Google into CPC (Paid), Product Sync, and Organic sub-channels.
    """
    sql = f"""
    SELECT
        CASE
            WHEN report_date BETWEEN '{t_start}' AND '{t_end}' THEN 'target'
            ELSE 'prior'
        END AS period,
        CASE
            WHEN source = 'google' AND medium = 'cpc' THEN 'Google CPC (Paid)'
            WHEN source = 'google' AND medium = 'product_sync' THEN 'Google Product Sync'
            WHEN source = 'google' AND medium = 'organic' THEN 'Google Organic'
            WHEN source IN ('facebook', 'ig') OR medium = 'paid_social'
                OR source LIKE '%%instagram%%' OR source LIKE '%%facebook%%'
                THEN 'Meta'
            WHEN source = 'klaviyo' OR medium = 'email' THEN 'Email'
            ELSE 'Other'
        END AS channel,
        SUM(sessions) AS sessions,
        SUM(purchases) AS purchases
    FROM `{_DS}.vw_ga4_funnel`
    WHERE report_date BETWEEN '{p_start}' AND '{t_end}'
    GROUP BY period, channel
    ORDER BY period, channel
    """
    rows = list(run_query(sql))

    channels = ["Meta", "Google CPC (Paid)", "Google Product Sync",
                "Google Organic", "Email", "Other"]
    result = {ch: {"target": {}, "prior": {}} for ch in channels}

    for r in rows:
        ch = r.channel
        if ch not in result:
            continue
        sessions = int(r.sessions or 0)
        purchases = int(r.purchases or 0)
        result[ch][r.period] = {
            "sessions": sessions,
            "purchases": purchases,
            "cvr": round(purchases / sessions * 100, 2) if sessions else 0,
        }

    return result


def _query_revenue_by_source() -> dict:
    """Trailing 6 months Shopify revenue split by traffic source type.

    Uses GA4 attribution to classify each order as Paid, Organic Search,
    Review Site (referral from pickleballeffect.com), Email, or Direct/Other.
    Returns data for a stacked column chart.
    """
    start = _trailing_months_start(6)
    sql = f"""
    SELECT
        FORMAT_DATE('%Y-%m', order_date) AS month_key,
        CASE
            WHEN ga4_medium IN ('cpc', 'paid_social')
                OR ga4_source IN ('facebook', 'ig')
                OR ga4_source LIKE '%%instagram%%'
                THEN 'Paid'
            WHEN ga4_medium = 'organic' THEN 'Organic Search'
            WHEN ga4_medium = 'referral'
                AND ga4_source LIKE '%%pickleballeffect.com%%'
                THEN 'Review Site'
            WHEN ga4_source = 'klaviyo' OR ga4_medium = 'email' THEN 'Email'
            ELSE 'Direct / Other'
        END AS source_type,
        SUM(shopify_revenue) AS revenue,
        COUNT(*) AS orders
    FROM `{_DS}.vw_ga4_attribution`
    WHERE order_date >= '{start}'
    GROUP BY month_key, source_type
    ORDER BY month_key, source_type
    """
    rows = list(run_query(sql))

    # Collect months
    month_set = sorted(set(r.month_key for r in rows))
    month_idx = {m: i for i, m in enumerate(month_set)}
    n = len(month_set)

    source_types = ["Paid", "Organic Search", "Review Site", "Email", "Direct / Other"]
    colors = {
        "Paid": "#667eea",
        "Organic Search": "#22c55e",
        "Review Site": "#f59e0b",
        "Email": "#8b5cf6",
        "Direct / Other": "#9ca3af",
    }

    series_data = {s: [0] * n for s in source_types}
    for r in rows:
        st = r.source_type
        i = month_idx.get(r.month_key)
        if i is not None and st in series_data:
            series_data[st][i] = round(float(r.revenue or 0), 2)

    series = [
        {"name": st, "data": series_data[st], "color": colors[st]}
        for st in source_types
        if any(v > 0 for v in series_data[st])  # skip empty series
    ]

    return {"months": month_set, "series": series}


def _query_trailing_revenue_aov() -> dict:
    """Trailing 13 months revenue + AOV from channel_summary."""
    start = _trailing_months_start(6)
    sql = f"""
    SELECT
        FORMAT_DATE('%Y-%m', report_date) AS month_key,
        SUM(revenue) AS revenue,
        SUM(orders) AS orders
    FROM `{_DS}.vw_channel_summary`
    WHERE report_date >= '{start}'
    GROUP BY month_key
    ORDER BY month_key
    """
    rows_list = list(run_query(sql))
    months = []
    revenue = []
    aov = []
    for r in rows_list:
        months.append(r.month_key)
        rev = round(float(r.revenue or 0), 2)
        ords = int(r.orders or 0)
        revenue.append(rev)
        aov.append(round(rev / ords, 2) if ords else 0)

    return {"months": months, "revenue": revenue, "aov": aov}


def _query_weekly_trends_13() -> dict:
    """Weekly spend & revenue for trailing 13 weeks."""
    start = _trailing_13_week_start()
    sql = f"""
    SELECT
        DATE_TRUNC(report_date, WEEK(MONDAY)) AS week_start,
        SUM(COALESCE(spend, 0)) AS spend,
        SUM(revenue) AS revenue
    FROM `{_DS}.vw_channel_summary`
    WHERE report_date >= '{start}'
    GROUP BY week_start
    ORDER BY week_start
    """
    rows_list = list(run_query(sql))
    weeks = []
    spend = []
    revenue = []
    for r in rows_list:
        ws = r.week_start
        label = ws.strftime("%b %d") if hasattr(ws, "strftime") else str(ws)
        weeks.append(label)
        spend.append(round(float(r.spend or 0), 2))
        revenue.append(round(float(r.revenue or 0), 2))

    return {"weeks": weeks, "spend": spend, "revenue": revenue}


def _query_monthly_roas_trends_13() -> dict:
    """Monthly ROAS by channel for trailing 13 months.

    5 series: Meta, Google Brand, Google Non-Brand, Google Shopping, Blended.
    """
    start = _trailing_months_start(6)

    # Meta monthly ROAS from enhanced_roas
    sql_meta = f"""
    SELECT
        FORMAT_DATE('%Y-%m', report_date) AS month_key,
        SAFE_DIVIDE(SUM(ga4_attributed_revenue), SUM(ad_spend)) AS roas
    FROM `{_DS}.vw_enhanced_roas`
    WHERE report_date >= '{start}' AND channel = 'meta' AND ad_spend > 0
    GROUP BY month_key
    ORDER BY month_key
    """

    # Google sub-channels from vw_daily_performance
    sql_google = f"""
    SELECT
        FORMAT_DATE('%Y-%m', report_date) AS month_key,
        CASE
            WHEN campaign_name = 'Brand' THEN 'brand'
            WHEN campaign_name LIKE 'Shopping%%' THEN 'shopping'
            ELSE 'nonbrand'
        END AS subchannel,
        SAFE_DIVIDE(SUM(conversion_value), SUM(spend)) AS roas
    FROM `{_DS}.vw_daily_performance`
    WHERE report_date >= '{start}' AND platform = 'google_ads' AND spend > 0
    GROUP BY month_key, subchannel
    ORDER BY month_key
    """

    # Blended ROAS
    sql_blended = f"""
    SELECT
        FORMAT_DATE('%Y-%m', report_date) AS month_key,
        SAFE_DIVIDE(SUM(ga4_attributed_revenue), SUM(ad_spend)) AS roas
    FROM `{_DS}.vw_enhanced_roas`
    WHERE report_date >= '{start}' AND ad_spend > 0
    GROUP BY month_key
    ORDER BY month_key
    """

    meta_rows = list(run_query(sql_meta))
    google_rows = list(run_query(sql_google))
    blended_rows = list(run_query(sql_blended))

    # Build month list from blended
    months = [r.month_key for r in blended_rows]
    month_idx = {m: i for i, m in enumerate(months)}
    n = len(months)

    blended = [round(float(r.roas or 0), 2) for r in blended_rows]
    meta_vals = [None] * n
    brand_vals = [None] * n
    nonbrand_vals = [None] * n
    shopping_vals = [None] * n

    for r in meta_rows:
        i = month_idx.get(r.month_key)
        if i is not None:
            meta_vals[i] = round(float(r.roas or 0), 2)

    for r in google_rows:
        i = month_idx.get(r.month_key)
        if i is None:
            continue
        val = round(float(r.roas or 0), 2)
        if r.subchannel == "brand":
            brand_vals[i] = val
        elif r.subchannel == "nonbrand":
            nonbrand_vals[i] = val
        elif r.subchannel == "shopping":
            shopping_vals[i] = val

    series = [
        {"name": "Meta", "data": meta_vals, "color": "#667eea", "lineWidth": 2},
        {"name": "Google Brand", "data": brand_vals, "color": "#8b5cf6", "lineWidth": 2},
        {"name": "Google Non-Brand", "data": nonbrand_vals, "color": "#ec4899", "lineWidth": 2},
        {"name": "Google Shopping", "data": shopping_vals, "color": "#f59e0b", "lineWidth": 2},
        {"name": "Blended", "data": blended, "color": "#22c55e", "lineWidth": 2, "dashStyle": "ShortDot"},
    ]

    return {"months": months, "series": series}


def _query_funnel(t_start, t_end) -> list:
    """Aggregate funnel steps for target month."""
    sql = f"""
    SELECT
        SUM(sessions) AS sessions,
        SUM(product_views) AS product_views,
        SUM(add_to_carts) AS add_to_carts,
        SUM(checkouts) AS checkouts,
        SUM(purchases) AS purchases
    FROM `{_DS}.vw_ga4_funnel`
    WHERE report_date BETWEEN '{t_start}' AND '{t_end}'
    """
    rows = list(run_query(sql))
    if not rows:
        return [["Sessions", 0], ["Product Views", 0], ["Add to Cart", 0],
                ["Checkout", 0], ["Purchase", 0]]
    r = rows[0]
    return [
        ["Sessions", int(r.sessions or 0)],
        ["Product Views", int(r.product_views or 0)],
        ["Add to Cart", int(r.add_to_carts or 0)],
        ["Checkout", int(r.checkouts or 0)],
        ["Purchase", int(r.purchases or 0)],
    ]


def _query_google_ads_health() -> list[dict]:
    """Top 10 keywords with quality scores and tiers."""
    sql = f"""
    SELECT keyword_text, campaign_name, total_spend, roas,
           quality_score, performance_tier
    FROM `{_DS}.vw_google_ads_keywords`
    WHERE total_spend > 0
    ORDER BY total_spend DESC
    LIMIT 10
    """
    rows = list(run_query(sql))
    return [
        {
            "keyword": r.keyword_text,
            "campaign": r.campaign_name,
            "spend": round(float(r.total_spend or 0), 2),
            "roas": round(float(r.roas or 0), 2),
            "qs": int(r.quality_score) if r.quality_score else None,
            "tier": r.performance_tier or "",
        }
        for r in rows
    ]


# ── Main orchestrator ────────────────────────────────────────────────────────

def generate(month: str | None = None, to_stdout: bool = False) -> str:
    """Generate monthly HTML dashboard.

    Args:
        month: Target month as 'YYYY-MM' (default: last completed month).
        to_stdout: Print HTML to stdout instead of saving.

    Returns:
        Path to the generated file (or the HTML string if to_stdout).
    """
    t_start, t_end, p_start, p_end = _parse_month(month)
    log.info(f"Generating dashboard for {_month_label(t_start)} "
             f"vs {_month_label(p_start)}")

    # Month-scoped queries
    log.info("Querying KPIs...")
    kpis = _query_kpis(t_start, t_end, p_start, p_end)

    log.info("Querying channel KPIs...")
    channel_kpis = _query_channel_kpis(t_start, t_end, p_start, p_end)

    log.info("Querying new customers...")
    new_customers = _query_new_customers(t_start, t_end, p_start, p_end)

    log.info("Querying sessions & CVR...")
    sessions_cvr = _query_sessions_cvr(t_start, t_end, p_start, p_end)

    log.info("Querying funnel...")
    funnel = _query_funnel(t_start, t_end)

    log.info("Querying Google Ads health...")
    keywords = _query_google_ads_health()

    # Trailing queries (ignore --month, always from today backward)
    log.info("Querying P&L (trailing 6 months)...")
    pnl = _query_pnl_trailing()

    log.info("Querying avg CLTV...")
    avg_cltv = _query_avg_cltv()

    log.info("Querying trailing revenue + AOV...")
    revenue_aov = _query_trailing_revenue_aov()

    log.info("Querying weekly trends (13 weeks)...")
    weekly = _query_weekly_trends_13()

    log.info("Querying monthly ROAS trends (13 months)...")
    monthly_roas = _query_monthly_roas_trends_13()

    log.info("Querying revenue by source (paid vs organic)...")
    revenue_by_source = _query_revenue_by_source()

    # Build template substitutions
    subs = {
        "month_label": _month_label(t_start),
        "prior_month_label": _month_label(p_start),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        # JSON data for charts and tables
        "channel_kpis_json": json.dumps(channel_kpis),
        "new_customers_json": json.dumps(new_customers),
        "sessions_cvr_json": json.dumps(sessions_cvr),
        "funnel_json": json.dumps(funnel),
        "keywords_json": json.dumps(keywords),
        "pnl_json": json.dumps(pnl),
        "kpi_cltv": f"${avg_cltv:,.2f}",
        "revenue_aov_json": json.dumps(revenue_aov),
        "weekly_json": json.dumps(weekly),
        "monthly_roas_json": json.dumps(monthly_roas),
        "revenue_by_source_json": json.dumps(revenue_by_source),
    }
    subs.update(kpis)

    html = DASHBOARD_HTML.safe_substitute(subs)

    if to_stdout:
        print(html)
        return html

    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    month_tag = t_start.strftime("%Y-%m")
    path = _REPORTS_DIR / f"dashboard_{month_tag}.html"
    path.write_text(html, encoding="utf-8")
    log.info(f"Dashboard saved to {path}")
    return str(path)
