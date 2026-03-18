"""Discovery script: explore product dataset in BigQuery for COGS data.

Queries INFORMATION_SCHEMA to discover tables, schemas, and sample data
in the `product` dataset, then tests SKU join coverage against Shopify
order line items.

Usage:
    uv run python -m ingestion.analysis.explore_cogs --print
"""

import argparse
import sys

sys.stdout.reconfigure(encoding="utf-8")

from ingestion.utils.bq_client import run_query
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.logger import get_logger

log = get_logger(__name__)

_PRODUCT_DATASET = f"{GCP_PROJECT_ID}.product"
_MARKETING_DS = f"{GCP_PROJECT_ID}.{BQ_DATASET}"


def _discover_tables() -> list[dict]:
    """List all tables in the product dataset."""
    sql = f"""
    SELECT table_name, table_type
    FROM `{_PRODUCT_DATASET}.INFORMATION_SCHEMA.TABLES`
    ORDER BY table_name
    """
    rows = list(run_query(sql))
    tables = []
    for r in rows:
        # Get row count per table (may fail for Drive-backed tables)
        err_msg = None
        try:
            count_sql = f"SELECT COUNT(*) AS cnt FROM `{_PRODUCT_DATASET}.{r.table_name}`"
            cnt = list(run_query(count_sql))[0].cnt
        except Exception as e:
            log.warning(f"Cannot count {r.table_name}: {e}")
            cnt = -1
            err_msg = str(e)
        tables.append({
            "name": r.table_name,
            "type": r.table_type,
            "rows": int(cnt),
            "accessible": cnt >= 0,
            "error": err_msg,
        })
    return tables


def _get_columns(table_name: str) -> list[dict]:
    """Get column schema for a table in the product dataset."""
    sql = f"""
    SELECT column_name, data_type, is_nullable
    FROM `{_PRODUCT_DATASET}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table_name}'
    ORDER BY ordinal_position
    """
    rows = list(run_query(sql))
    return [
        {
            "name": r.column_name,
            "type": r.data_type,
            "nullable": r.is_nullable,
        }
        for r in rows
    ]


def _sample_rows(table_name: str, limit: int = 10) -> list[dict]:
    """Sample rows from a product dataset table."""
    sql = f"SELECT * FROM `{_PRODUCT_DATASET}.{table_name}` LIMIT {limit}"
    rows = list(run_query(sql))
    return [dict(r.items()) for r in rows]


def _get_shopify_skus() -> list[dict]:
    """Get distinct SKUs from Shopify line items with order counts."""
    sql = f"""
    SELECT
        sku,
        COUNT(DISTINCT order_id) AS order_count,
        SUM(CAST(price AS FLOAT64) * quantity) AS total_revenue,
        SUM(quantity) AS total_units
    FROM `{_MARKETING_DS}.shopify_order_line_items`
    WHERE sku IS NOT NULL AND sku != ''
    GROUP BY sku
    ORDER BY total_revenue DESC
    """
    rows = list(run_query(sql))
    return [
        {
            "sku": r.sku,
            "orders": int(r.order_count),
            "revenue": round(float(r.total_revenue or 0), 2),
            "units": int(r.total_units or 0),
        }
        for r in rows
    ]


def _find_sku_columns(columns: list[dict]) -> list[str]:
    """Identify columns that likely contain SKU data."""
    sku_keywords = ["sku", "item_number", "product_code", "item_code", "upc", "barcode"]
    return [
        c["name"]
        for c in columns
        if any(kw in c["name"].lower() for kw in sku_keywords)
        and c["type"] in ("STRING", "INT64", "FLOAT64")
    ]


def _find_cost_columns(columns: list[dict]) -> list[str]:
    """Identify columns that likely contain COGS/cost data."""
    cost_keywords = ["cost", "cogs", "price", "wholesale", "landed", "freight", "shipping"]
    return [
        c["name"]
        for c in columns
        if any(kw in c["name"].lower() for kw in cost_keywords)
    ]


def _test_sku_join(table_name: str, sku_col: str, shopify_skus: list[dict]) -> dict:
    """Test how well a product table's SKU column joins to Shopify SKUs."""
    sql = f"""
    WITH shopify AS (
        SELECT DISTINCT sku
        FROM `{_MARKETING_DS}.shopify_order_line_items`
        WHERE sku IS NOT NULL AND sku != ''
    ),
    product AS (
        SELECT DISTINCT CAST({sku_col} AS STRING) AS sku
        FROM `{_PRODUCT_DATASET}.{table_name}`
        WHERE {sku_col} IS NOT NULL
    )
    SELECT
        (SELECT COUNT(*) FROM shopify) AS shopify_sku_count,
        (SELECT COUNT(*) FROM product) AS product_sku_count,
        (SELECT COUNT(*) FROM shopify s JOIN product p ON s.sku = p.sku) AS exact_match,
        (SELECT COUNT(*) FROM shopify s JOIN product p ON LOWER(TRIM(s.sku)) = LOWER(TRIM(p.sku))) AS fuzzy_match
    """
    rows = list(run_query(sql))
    r = rows[0]
    return {
        "shopify_skus": int(r.shopify_sku_count),
        "product_skus": int(r.product_sku_count),
        "exact_match": int(r.exact_match),
        "fuzzy_match": int(r.fuzzy_match),
    }


def _test_revenue_coverage(table_name: str, sku_col: str, cost_col: str | None) -> dict:
    """Check what % of Shopify revenue has COGS data from the product table."""
    cost_select = f", AVG(CAST(p.{cost_col} AS FLOAT64)) AS avg_cogs" if cost_col else ""

    sql = f"""
    WITH line_items AS (
        SELECT
            sku,
            SUM(CAST(price AS FLOAT64) * quantity) AS revenue
        FROM `{_MARKETING_DS}.shopify_order_line_items`
        WHERE sku IS NOT NULL AND sku != ''
        GROUP BY sku
    )
    SELECT
        SUM(li.revenue) AS total_revenue,
        SUM(CASE WHEN p.{sku_col} IS NOT NULL THEN li.revenue ELSE 0 END) AS matched_revenue
        {cost_select}
    FROM line_items li
    LEFT JOIN `{_PRODUCT_DATASET}.{table_name}` p
        ON LOWER(TRIM(li.sku)) = LOWER(TRIM(CAST(p.{sku_col} AS STRING)))
    """
    rows = list(run_query(sql))
    r = rows[0]
    total = float(r.total_revenue or 0)
    matched = float(r.matched_revenue or 0)
    result = {
        "total_revenue": round(total, 2),
        "matched_revenue": round(matched, 2),
        "coverage_pct": round(matched / total * 100, 1) if total else 0,
    }
    if cost_col:
        result["avg_cogs"] = round(float(r.avg_cogs or 0), 2)
    return result


def explore() -> str:
    """Run full discovery and return formatted report."""
    lines = []
    lines.append("=" * 70)
    lines.append("COGS DISCOVERY REPORT")
    lines.append(f"Dataset: {_PRODUCT_DATASET}")
    lines.append("=" * 70)

    # 1. Discover tables
    lines.append("\n## 1. Tables in product dataset\n")
    tables = _discover_tables()
    if not tables:
        lines.append("  NO TABLES FOUND — dataset may be empty or inaccessible.")
        return "\n".join(lines)

    for t in tables:
        status = f"rows={t['rows']:,}" if t["accessible"] else "INACCESSIBLE (Drive-backed?)"
        lines.append(f"  {t['name']:40s}  type={t['type']:10s}  {status}")

    # 2. Column schemas + sample data for each table
    all_table_info = {}
    for t in tables:
        if not t["accessible"]:
            continue
        tname = t["name"]
        lines.append(f"\n## 2. Schema: {tname}\n")

        columns = _get_columns(tname)
        all_table_info[tname] = {"columns": columns}

        for c in columns:
            lines.append(f"  {c['name']:40s}  {c['type']:15s}  nullable={c['nullable']}")

        sku_cols = _find_sku_columns(columns)
        cost_cols = _find_cost_columns(columns)
        all_table_info[tname]["sku_cols"] = sku_cols
        all_table_info[tname]["cost_cols"] = cost_cols

        if sku_cols:
            lines.append(f"\n  -> Possible SKU columns: {', '.join(sku_cols)}")
        if cost_cols:
            lines.append(f"  -> Possible COST columns: {', '.join(cost_cols)}")

        # Sample data
        lines.append(f"\n  Sample rows (up to 10):")
        try:
            samples = _sample_rows(tname, 10)
            for i, row in enumerate(samples):
                lines.append(f"    Row {i+1}: {row}")
        except Exception as e:
            lines.append(f"    ERROR sampling: {e}")

    # 3. Shopify SKUs
    lines.append("\n## 3. Shopify SKUs (from order line items)\n")
    shopify_skus = _get_shopify_skus()
    lines.append(f"  Total distinct SKUs: {len(shopify_skus)}")
    lines.append(f"  Top SKUs by revenue:")
    for s in shopify_skus[:15]:
        lines.append(f"    {s['sku']:30s}  orders={s['orders']:4d}  units={s['units']:5d}  revenue=${s['revenue']:,.2f}")

    # 4. SKU join tests
    lines.append("\n## 4. SKU Join Coverage\n")
    join_results = {}
    for tname, info in all_table_info.items():
        for sku_col in info["sku_cols"]:
            key = f"{tname}.{sku_col}"
            lines.append(f"  Testing {key} ...")
            try:
                result = _test_sku_join(tname, sku_col, shopify_skus)
                join_results[key] = result
                lines.append(f"    Shopify SKUs:  {result['shopify_skus']}")
                lines.append(f"    Product SKUs:  {result['product_skus']}")
                lines.append(f"    Exact match:   {result['exact_match']}")
                lines.append(f"    Fuzzy match:   {result['fuzzy_match']}")
                pct = round(result['fuzzy_match'] / result['shopify_skus'] * 100, 1) if result['shopify_skus'] else 0
                lines.append(f"    Coverage:      {pct}%")
            except Exception as e:
                lines.append(f"    ERROR: {e}")

    if not join_results:
        lines.append("  No SKU columns found in any product table.")

    # 5. Revenue coverage
    lines.append("\n## 5. Revenue Coverage\n")
    for tname, info in all_table_info.items():
        for sku_col in info["sku_cols"]:
            cost_col = info["cost_cols"][0] if info["cost_cols"] else None
            key = f"{tname}.{sku_col}"
            lines.append(f"  Testing {key} (cost col: {cost_col or 'NONE'}) ...")
            try:
                result = _test_revenue_coverage(tname, sku_col, cost_col)
                lines.append(f"    Total Shopify revenue:   ${result['total_revenue']:,.2f}")
                lines.append(f"    Matched revenue:         ${result['matched_revenue']:,.2f}")
                lines.append(f"    Coverage:                {result['coverage_pct']}%")
                lines.append(f"    Unmatched (40% fallback): ${result['total_revenue'] - result['matched_revenue']:,.2f}")
                if "avg_cogs" in result:
                    lines.append(f"    Avg COGS (matched):      ${result['avg_cogs']}")
            except Exception as e:
                lines.append(f"    ERROR: {e}")

    # 6. Search for shipping cost columns
    lines.append("\n## 6. Shipping / Freight Columns\n")
    shipping_keywords = ["shipping", "freight", "postage", "delivery", "poland"]
    found_any = False
    for tname, info in all_table_info.items():
        for c in info["columns"]:
            if any(kw in c["name"].lower() for kw in shipping_keywords):
                lines.append(f"  {tname}.{c['name']} ({c['type']})")
                found_any = True
    if not found_any:
        lines.append("  No shipping/freight columns found in product dataset.")

    # 7. Summary
    lines.append("\n" + "=" * 70)
    lines.append("SUMMARY & RECOMMENDATION")
    lines.append("=" * 70)

    best_coverage = 0
    best_key = None
    for key, result in join_results.items():
        pct = round(result['fuzzy_match'] / result['shopify_skus'] * 100, 1) if result['shopify_skus'] else 0
        if pct > best_coverage:
            best_coverage = pct
            best_key = key

    if best_coverage >= 50:
        lines.append(f"\n  GOOD: Best SKU match is {best_key} at {best_coverage}% coverage.")
        lines.append(f"  Recommendation: Build vw_product_profitability using this join.")
        lines.append(f"  Unmatched SKUs will use 40% of net revenue as estimated COGS.")
    elif best_coverage > 0:
        lines.append(f"\n  PARTIAL: Best SKU match is {best_key} at {best_coverage}% coverage.")
        lines.append(f"  Recommendation: Review sample data — SKU format mismatch may be fixable.")
        lines.append(f"  If fixable, build view with transform. Otherwise, discuss alternatives.")
    else:
        lines.append(f"\n  NO MATCH: Product dataset SKUs do not match Shopify SKUs.")
        lines.append(f"  This likely means the product dataset belongs to a different brand.")
        lines.append(f"  Recommendation: Use 40% estimated COGS for all products,")
        lines.append(f"  or manually create a COGS lookup table.")

    lines.append("")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Explore product dataset for COGS data")
    parser.add_argument("--print", action="store_true", dest="to_stdout",
                        help="Print report to stdout")
    args = parser.parse_args()

    report = explore()

    if args.to_stdout:
        print(report)
    else:
        from pathlib import Path
        reports_dir = Path(__file__).resolve().parents[2] / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        path = reports_dir / "cogs_discovery.txt"
        path.write_text(report, encoding="utf-8")
        log.info(f"Discovery report saved to {path}")


if __name__ == "__main__":
    main()
