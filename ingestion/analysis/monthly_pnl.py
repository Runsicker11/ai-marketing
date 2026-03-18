"""Quick monthly P&L: gross profit minus ad spend minus agency fee."""

import sys

sys.stdout.reconfigure(encoding="utf-8")

from ingestion.utils.bq_client import run_query
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET

_DS = f"{GCP_PROJECT_ID}.{BQ_DATASET}"
AGENCY_FEE = 3_000  # monthly


def run():
    sql = f"""
    WITH monthly_profit AS (
        SELECT
            FORMAT_DATE('%Y-%m', order_date) AS month,
            ROUND(SUM(net_revenue), 2) AS net_revenue,
            ROUND(SUM(total_cogs), 2) AS total_cogs,
            ROUND(SUM(gross_profit), 2) AS gross_profit
        FROM `{_DS}.vw_product_profitability`
        GROUP BY month
    ),
    monthly_spend AS (
        SELECT
            FORMAT_DATE('%Y-%m', report_date) AS month,
            ROUND(SUM(spend), 2) AS ad_spend
        FROM `{_DS}.vw_channel_summary`
        GROUP BY month
    )
    SELECT
        p.month,
        p.net_revenue,
        p.total_cogs,
        p.gross_profit,
        COALESCE(s.ad_spend, 0) AS ad_spend,
        {AGENCY_FEE} AS agency_fee,
        ROUND(p.gross_profit - COALESCE(s.ad_spend, 0) - {AGENCY_FEE}, 2) AS bottom_line
    FROM monthly_profit p
    LEFT JOIN monthly_spend s ON p.month = s.month
    ORDER BY p.month
    """
    rows = list(run_query(sql))

    print("\nMONTHLY P&L")
    print("=" * 105)
    print(
        f"  {'Month':<10} {'Net Revenue':>12} {'COGS':>10} "
        f"{'Gross Profit':>13} {'Ad Spend':>10} {'Agency Fee':>11} {'Bottom Line':>13}"
    )
    print("  " + "-" * 100)

    for r in rows:
        bl = float(r.bottom_line)
        bl_str = f"({abs(bl):,.2f})" if bl < 0 else f"${bl:,.2f}"
        print(
            f"  {r.month:<10} ${float(r.net_revenue):>11,.2f} ${float(r.total_cogs):>9,.2f} "
            f"${float(r.gross_profit):>12,.2f} ${float(r.ad_spend):>9,.2f} "
            f"${float(r.agency_fee):>10,.0f} {bl_str:>13}"
        )

    print("  " + "-" * 100)
    total_rev = sum(float(r.net_revenue) for r in rows)
    total_cogs = sum(float(r.total_cogs) for r in rows)
    total_gp = sum(float(r.gross_profit) for r in rows)
    total_ad = sum(float(r.ad_spend) for r in rows)
    n = len(rows)
    total_agency = AGENCY_FEE * n
    total_bl = total_gp - total_ad - total_agency
    bl_str = f"({abs(total_bl):,.2f})" if total_bl < 0 else f"${total_bl:,.2f}"
    print(
        f"  {'TOTAL':<10} ${total_rev:>11,.2f} ${total_cogs:>9,.2f} "
        f"${total_gp:>12,.2f} ${total_ad:>9,.2f} "
        f"${total_agency:>10,.0f} {bl_str:>13}"
    )
    print()
    print(f"  Gross margin:    {total_gp / total_rev * 100:.1f}%")
    print(f"  After ad spend:  {(total_gp - total_ad) / total_rev * 100:.1f}%")
    print(f"  Bottom line:     {total_bl / total_rev * 100:.1f}%")
    print()


if __name__ == "__main__":
    run()
