"""AI-powered search term hygiene: negative keywords + keyword expansion."""

from datetime import date
from pathlib import Path

from ingestion.utils.bq_client import run_query
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.logger import get_logger
from ingestion.analysis.claude_client import analyze

log = get_logger(__name__)

_DS = f"{GCP_PROJECT_ID}.{BQ_DATASET}"
_REPORTS_DIR = Path(__file__).resolve().parents[1] / "reports"

SYSTEM_PROMPT = """\
You are a Google Ads search term analyst for Pickleball Effect, a DTC pickleball \
accessories brand. You review search term reports to optimize keyword targeting.

Your job is to categorize each search term into one of:
1. **add_negative** — irrelevant terms wasting money (explain why)
2. **add_as_keyword** — high-intent terms that should be added as keywords
3. **monitor** — ambiguous terms that need more data before deciding
4. **ignore** — low-spend terms not worth acting on yet

Be specific about WHY each term should be negative or added. Group negative \
keywords by theme (e.g., "competitor terms", "informational queries", \
"unrelated products").

Also recommend negative keyword match types (exact, phrase, broad) for each.\
"""


def _query_wasted_search_terms() -> str:
    """Get search terms with spend but zero conversions."""
    sql = f"""
    SELECT search_term, total_spend, total_clicks, total_impressions,
           avg_ctr, days_seen
    FROM `{_DS}.vw_search_terms_waste`
    ORDER BY total_spend DESC
    LIMIT 30
    """
    rows = list(run_query(sql))
    if not rows:
        return "No wasted search terms found."
    header = "search_term | spend | clicks | impressions | ctr | days"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.search_term} | ${r.total_spend or 0:.2f} | "
            f"{r.total_clicks or 0} | {r.total_impressions or 0} | "
            f"{r.avg_ctr or 0:.2%} | {r.days_seen or 0}"
        )
    return "\n".join(lines)


def _query_high_converting_terms() -> str:
    """Get search terms with conversions that aren't yet keywords."""
    sql = f"""
    WITH converting_terms AS (
        SELECT
            search_term,
            SUM(spend) AS total_spend,
            SUM(clicks) AS total_clicks,
            SUM(conversions) AS total_conversions,
            SUM(conversion_value) AS total_value,
            SAFE_DIVIDE(SUM(conversion_value), SUM(spend)) AS roas
        FROM `{_DS}.google_ads_search_terms`
        WHERE conversions > 0
        GROUP BY search_term
    ),
    existing_keywords AS (
        SELECT LOWER(keyword_text) AS keyword_text
        FROM `{_DS}.google_ads_keywords`
        WHERE status = 'ENABLED'
    )
    SELECT ct.search_term, ct.total_spend, ct.total_clicks,
           ct.total_conversions, ct.total_value, ct.roas
    FROM converting_terms ct
    LEFT JOIN existing_keywords ek ON LOWER(ct.search_term) = ek.keyword_text
    WHERE ek.keyword_text IS NULL
    ORDER BY ct.total_value DESC
    LIMIT 20
    """
    rows = list(run_query(sql))
    if not rows:
        return "No high-converting search terms found that aren't already keywords."
    header = "search_term | spend | clicks | conversions | value | roas"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.search_term} | ${r.total_spend or 0:.2f} | "
            f"{r.total_clicks or 0} | {r.total_conversions or 0:.1f} | "
            f"${r.total_value or 0:.2f} | {r.roas or 0:.2f}"
        )
    return "\n".join(lines)


def _query_all_recent_terms() -> str:
    """Get all recent search terms for context."""
    sql = f"""
    SELECT search_term,
           SUM(spend) AS total_spend,
           SUM(clicks) AS total_clicks,
           SUM(conversions) AS total_conversions,
           SUM(conversion_value) AS total_value
    FROM `{_DS}.google_ads_search_terms`
    WHERE date_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    GROUP BY search_term
    ORDER BY total_spend DESC
    LIMIT 50
    """
    rows = list(run_query(sql))
    if not rows:
        return "No recent search terms found."
    header = "search_term | spend | clicks | conversions | value"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.search_term} | ${r.total_spend or 0:.2f} | "
            f"{r.total_clicks or 0} | {r.total_conversions or 0:.1f} | "
            f"${r.total_value or 0:.2f}"
        )
    return "\n".join(lines)


def review(to_stdout: bool = False) -> str:
    """Review search terms and generate negative keyword recommendations.

    Returns:
        The search term recommendations report text.
    """
    log.info("Reviewing search terms for keyword hygiene")

    wasted = _query_wasted_search_terms()
    converting = _query_high_converting_terms()
    all_terms = _query_all_recent_terms()

    data_context = (
        f"### Wasted Search Terms (Zero Conversions, $5+ Spend)\n{wasted}\n\n"
        f"### High-Converting Terms NOT Added as Keywords\n{converting}\n\n"
        f"### All Recent Search Terms (Last 14 Days)\n{all_terms}"
    )

    question = (
        f"Review these search terms for Pickleball Effect (pickleball accessories shop). "
        f"Categorize each wasted term as: add_negative, monitor, or ignore. "
        f"Categorize each high-converting term as: add_as_keyword, monitor, or ignore. "
        f"Group negative keywords by theme. "
        f"Specify match type (exact, phrase, broad) for each recommendation. "
        f"Today is {date.today()}."
    )

    report = analyze(SYSTEM_PROMPT, data_context, question)

    full_report = (
        f"# Search Term Recommendations\n"
        f"## {date.today()}\n\n"
        f"{report}"
    )

    if to_stdout:
        print(full_report)
    else:
        _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        path = _REPORTS_DIR / f"search_term_recommendations_{date.today()}.md"
        path.write_text(full_report, encoding="utf-8")
        log.info(f"Search term recommendations saved to {path}")

    return full_report
