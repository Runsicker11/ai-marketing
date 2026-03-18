"""Analyze Search Console data for content opportunities."""

import sys
sys.stdout.reconfigure(encoding="utf-8")

from ingestion.utils.bq_client import run_query


def run_analysis():
    results = {}

    # 1. Top pages by clicks
    sql = """
    SELECT
        page,
        SUM(clicks) AS total_clicks,
        SUM(impressions) AS total_impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 1) AS ctr,
        ROUND(AVG(position), 1) AS avg_position,
        COUNT(DISTINCT query) AS query_count
    FROM `marketing_data.search_console_performance`
    WHERE query_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        AND site = 'pickleballeffect.com'
    GROUP BY page
    HAVING total_impressions > 100
    ORDER BY total_clicks DESC
    LIMIT 30
    """
    print("=" * 130)
    print("TOP 30 PAGES BY CLICKS (Last 90 Days - Review Site)")
    print("=" * 130)
    print(f"{'Page':<75} {'Clicks':>7} {'Impr':>8} {'CTR':>6} {'Pos':>5} {'Queries':>7}")
    print("-" * 130)
    top_pages = list(run_query(sql))
    for r in top_pages:
        page = r.page.replace("https://pickleballeffect.com", "") if r.page else ""
        if len(page) > 73:
            page = page[:70] + "..."
        print(f"{page:<75} {r.total_clicks:>7,} {r.total_impressions:>8,} {r.ctr:>5.1f}% {r.avg_position:>5.1f} {r.query_count:>7,}")
    results["top_pages"] = top_pages

    # 2. High impressions, low CTR (title optimization)
    sql2 = """
    SELECT
        page,
        SUM(clicks) AS total_clicks,
        SUM(impressions) AS total_impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 1) AS ctr,
        ROUND(AVG(position), 1) AS avg_position
    FROM `marketing_data.search_console_performance`
    WHERE query_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        AND site = 'pickleballeffect.com'
    GROUP BY page
    HAVING total_impressions > 1000 AND ctr < 3.0
    ORDER BY total_impressions DESC
    LIMIT 20
    """
    print("\n" + "=" * 130)
    print("HIGH IMPRESSIONS / LOW CTR PAGES (<3% CTR, >1000 impressions)")
    print("Pages ranking but not getting clicked -- title/meta optimization wins")
    print("=" * 130)
    print(f"{'Page':<75} {'Clicks':>7} {'Impr':>8} {'CTR':>6} {'Pos':>5}")
    print("-" * 130)
    low_ctr = list(run_query(sql2))
    for r in low_ctr:
        page = r.page.replace("https://pickleballeffect.com", "") if r.page else ""
        if len(page) > 73:
            page = page[:70] + "..."
        print(f"{page:<75} {r.total_clicks:>7,} {r.total_impressions:>8,} {r.ctr:>5.1f}% {r.avg_position:>5.1f}")
    results["low_ctr"] = low_ctr

    # 3. Content gaps - high impression queries ranking poorly
    sql3 = """
    SELECT
        query,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        ROUND(AVG(position), 1) AS avg_position,
        COUNT(DISTINCT page) AS pages_ranking
    FROM `marketing_data.search_console_performance`
    WHERE query_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        AND site = 'pickleballeffect.com'
    GROUP BY query
    HAVING total_impressions > 500 AND avg_position > 10
    ORDER BY total_impressions DESC
    LIMIT 30
    """
    print("\n" + "=" * 130)
    print("CONTENT GAPS: High-impression queries ranking poorly (position >10)")
    print("Queries Google shows you for but you don't have strong content")
    print("=" * 130)
    print(f"{'Query':<65} {'Clicks':>7} {'Impr':>8} {'Pos':>6} {'Pages':>6}")
    print("-" * 130)
    content_gaps = list(run_query(sql3))
    for r in content_gaps:
        q = r.query[:63] if len(r.query) > 63 else r.query
        print(f"{q:<65} {r.total_clicks:>7,} {r.total_impressions:>8,} {r.avg_position:>6.1f} {r.pages_ranking:>6}")
    results["content_gaps"] = content_gaps

    # 4. Accessory/commercial intent queries
    sql4 = """
    SELECT
        query,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        ROUND(AVG(position), 1) AS avg_position,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 1) AS ctr
    FROM `marketing_data.search_console_performance`
    WHERE query_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        AND site = 'pickleballeffect.com'
        AND (
            LOWER(query) LIKE '%tape%'
            OR LOWER(query) LIKE '%overgrip%'
            OR LOWER(query) LIKE '%grip%'
            OR LOWER(query) LIKE '%edge guard%'
            OR LOWER(query) LIKE '%weight%'
            OR LOWER(query) LIKE '%tungsten%'
            OR LOWER(query) LIKE '%lead tape%'
            OR LOWER(query) LIKE '%tuning%'
            OR LOWER(query) LIKE '%customize%'
            OR LOWER(query) LIKE '%accessori%'
        )
    GROUP BY query
    HAVING total_impressions > 20
    ORDER BY total_impressions DESC
    LIMIT 30
    """
    print("\n" + "=" * 130)
    print("ACCESSORY/COMMERCIAL QUERIES (tape, grip, weight, tuning, etc.)")
    print("Directly relate to shop products -- biggest content-to-commerce bridge")
    print("=" * 130)
    print(f"{'Query':<65} {'Clicks':>7} {'Impr':>8} {'CTR':>6} {'Pos':>5}")
    print("-" * 130)
    accessory_queries = list(run_query(sql4))
    for r in accessory_queries:
        q = r.query[:63] if len(r.query) > 63 else r.query
        print(f"{q:<65} {r.total_clicks:>7,} {r.total_impressions:>8,} {r.ctr:>5.1f}% {r.avg_position:>5.1f}")
    results["accessory_queries"] = accessory_queries

    # 5. Comparison queries
    sql5 = """
    SELECT
        query,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        ROUND(AVG(position), 1) AS avg_position,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 1) AS ctr
    FROM `marketing_data.search_console_performance`
    WHERE query_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        AND site = 'pickleballeffect.com'
        AND (
            LOWER(query) LIKE '% vs %'
            OR LOWER(query) LIKE '%versus%'
            OR LOWER(query) LIKE '%compare%'
            OR LOWER(query) LIKE '%comparison%'
            OR LOWER(query) LIKE '%difference between%'
        )
    GROUP BY query
    HAVING total_impressions > 50
    ORDER BY total_impressions DESC
    LIMIT 30
    """
    print("\n" + "=" * 130)
    print("COMPARISON QUERIES (vs, compare, difference)")
    print("High-intent queries -- only 3 comparison posts exist")
    print("=" * 130)
    print(f"{'Query':<65} {'Clicks':>7} {'Impr':>8} {'CTR':>6} {'Pos':>5}")
    print("-" * 130)
    comparison_queries = list(run_query(sql5))
    for r in comparison_queries:
        q = r.query[:63] if len(r.query) > 63 else r.query
        print(f"{q:<65} {r.total_clicks:>7,} {r.total_impressions:>8,} {r.ctr:>5.1f}% {r.avg_position:>5.1f}")
    results["comparison_queries"] = comparison_queries

    # 6. Overall site stats
    sql6 = """
    SELECT
        SUM(clicks) AS total_clicks,
        SUM(impressions) AS total_impressions,
        ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100, 1) AS avg_ctr,
        ROUND(AVG(position), 1) AS avg_position,
        COUNT(DISTINCT query) AS unique_queries,
        COUNT(DISTINCT page) AS unique_pages
    FROM `marketing_data.search_console_performance`
    WHERE query_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        AND site = 'pickleballeffect.com'
    """
    print("\n" + "=" * 130)
    print("OVERALL SITE STATS (Last 90 Days)")
    print("=" * 130)
    for r in run_query(sql6):
        print(f"Total clicks:      {r.total_clicks:>12,}")
        print(f"Total impressions: {r.total_impressions:>12,}")
        print(f"Average CTR:       {r.avg_ctr:>11.1f}%")
        print(f"Average position:  {r.avg_position:>12.1f}")
        print(f"Unique queries:    {r.unique_queries:>12,}")
        print(f"Unique pages:      {r.unique_pages:>12,}")

    return results


if __name__ == "__main__":
    run_analysis()
