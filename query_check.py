"""Phase 1 verification queries — the whole point of the pipeline."""
from ingestion.utils.bq_client import run_query

print("=" * 70)
print("TRUE ROAS: Meta spend vs actual Shopify revenue (last 14 days)")
print("=" * 70)
rows = run_query("""
    SELECT report_date, meta_spend, meta_reported_purchases, meta_reported_revenue,
           shopify_meta_orders, shopify_meta_revenue, true_roas, meta_reported_roas, revenue_gap
    FROM marketing_data.vw_true_roas
    WHERE meta_spend > 0
    ORDER BY report_date DESC LIMIT 14
""")
for r in rows:
    tr = f"{r.true_roas:.2f}x" if r.true_roas else "N/A"
    mr = f"{r.meta_reported_roas:.2f}x" if r.meta_reported_roas else "N/A"
    so = r.shopify_meta_orders or 0
    sr = r.shopify_meta_revenue or 0
    gap = r.revenue_gap or 0
    print(f"  {r.report_date} | spend: ${r.meta_spend:.2f} | meta says: ${r.meta_reported_revenue or 0:.2f} ({mr}) | shopify actual: ${sr:.2f} ({tr}) | gap: ${gap:.2f}")

print()
print("=" * 70)
print("PRODUCT PERFORMANCE by ad source")
print("=" * 70)
rows = run_query("""
    SELECT product_title, source, units_sold, net_revenue, orders
    FROM marketing_data.vw_product_performance
    ORDER BY units_sold DESC LIMIT 15
""")
for r in rows:
    print(f"  {r.product_title[:40]:<40} | {r.source:<10} | {r.units_sold} units | ${r.net_revenue:.2f} | {r.orders} orders")

print()
print("=" * 70)
print("TRENDS: 7-day rolling averages")
print("=" * 70)
rows = run_query("""
    SELECT report_date, meta_spend, shopify_meta_revenue, true_roas,
           spend_7d_avg, revenue_7d_avg, roas_7d_avg
    FROM marketing_data.vw_trends
    WHERE meta_spend > 0
    ORDER BY report_date DESC LIMIT 7
""")
for r in rows:
    tr = f"{r.true_roas:.2f}x" if r.true_roas else "N/A"
    ra = f"{r.roas_7d_avg:.2f}x" if r.roas_7d_avg else "N/A"
    sr = r.shopify_meta_revenue or 0
    s7 = r.spend_7d_avg or 0
    r7 = r.revenue_7d_avg or 0
    print(f"  {r.report_date} | spend: ${r.meta_spend:.2f} (7d avg: ${s7:.2f}) | revenue: ${sr:.2f} (7d avg: ${r7:.2f}) | ROAS: {tr} (7d avg: {ra})")

print()
print("=" * 70)
print("TABLE ROW COUNTS")
print("=" * 70)
for table in ['shopify_orders', 'shopify_order_line_items', 'shopify_products',
              'shopify_product_variants', 'shopify_customers',
              'meta_campaigns', 'meta_adsets', 'meta_ads', 'meta_daily_insights']:
    rows = run_query(f"SELECT COUNT(*) as cnt FROM marketing_data.{table}")
    for r in rows:
        print(f"  {table:<30} {r.cnt:>8} rows")
