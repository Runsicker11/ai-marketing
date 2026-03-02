"""Identify content opportunities from Search Console + GA4 data."""

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
You are an SEO content strategist for Pickleball Effect, a pickleball review and \
accessories site. You analyze Search Console data to recommend what content to \
create or optimize next.

Your recommendations should:
1. Prioritize by potential traffic impact (impressions x position improvement potential)
2. Categorize each recommendation as: new_article | optimize_existing | update_meta
3. Specify the content type: review | comparison | how_to | landing_page
4. Include target keyword and suggested title
5. Note any existing content that overlaps (to avoid cannibalization)

Output a ranked list of the top 5 content recommendations.\
"""


def _site_filter(site: str | None) -> str:
    """Return a SQL WHERE clause fragment for site filtering."""
    if site and site in ("blog", "shop"):
        # Map CLI site names to Search Console domains
        domain_map = {
            "blog": "pickleballeffect.com",
            "shop": "pickleballeffectshop.com",
        }
        domain = domain_map[site]
        return f"AND site = '{domain}'"
    return ""


def _query_opportunities(site: str | None = None) -> str:
    """Get striking-distance keywords."""
    site_clause = _site_filter(site)
    sql = f"""
    SELECT site, query, page, avg_position, impressions_30d, clicks_30d,
           ctr, opportunity_score
    FROM `{_DS}.vw_seo_opportunities`
    WHERE 1=1 {site_clause}
    ORDER BY opportunity_score DESC
    LIMIT 25
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        return "No SEO opportunity data available (Search Console not yet connected)."
    if not rows:
        return "No striking-distance keywords found."
    header = "site | query | page | position | impressions | clicks | ctr | score"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.site} | {r.query} | {r.page} | "
            f"{r.avg_position or 0:.1f} | {r.impressions_30d or 0} | "
            f"{r.clicks_30d or 0} | {r.ctr or 0:.2%} | "
            f"{r.opportunity_score or 0:.1f}"
        )
    return "\n".join(lines)


def _query_content_gaps(site: str | None = None) -> str:
    """Get pages with high impressions but low CTR."""
    site_clause = _site_filter(site)
    sql = f"""
    SELECT site, page, total_impressions, total_clicks, avg_ctr,
           avg_position, suggested_action
    FROM `{_DS}.vw_seo_content_gaps`
    WHERE 1=1 {site_clause}
    ORDER BY total_impressions DESC
    LIMIT 15
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        return "No content gap data available."
    if not rows:
        return "No content gaps found."
    header = "site | page | impressions | clicks | ctr | position | action"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.site} | {r.page} | {r.total_impressions or 0} | "
            f"{r.total_clicks or 0} | {r.avg_ctr or 0:.2%} | "
            f"{r.avg_position or 0:.1f} | {r.suggested_action}"
        )
    return "\n".join(lines)


def _query_existing_content() -> str:
    """Get existing content posts from BigQuery."""
    sql = f"""
    SELECT title, target_keyword, content_type, status, url, platform
    FROM `{_DS}.content_posts`
    ORDER BY created_at DESC
    LIMIT 50
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        return "No existing content inventory available."
    if not rows:
        return "No existing content tracked yet."
    header = "title | keyword | type | status | platform"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.title} | {r.target_keyword} | {r.content_type} | "
            f"{r.status} | {r.platform}"
        )
    return "\n".join(lines)


def identify(site: str | None = None, to_stdout: bool = False) -> str:
    """Identify top content opportunities and output recommendations.

    Args:
        site: Filter to a specific site ("blog" or "shop"). None = all sites.
        to_stdout: Print instead of saving to file.

    Returns:
        The content opportunities report text.
    """
    site_label = f" ({site})" if site else " (all sites)"
    log.info(f"Identifying content opportunities{site_label}")

    opp_data = _query_opportunities(site)
    gap_data = _query_content_gaps(site)
    inventory_data = _query_existing_content()

    data_context = (
        f"### Striking-Distance Keywords (Position 5-20)\n{opp_data}\n\n"
        f"### Content Gaps (High Impressions, Low CTR)\n{gap_data}\n\n"
        f"### Existing Content Inventory\n{inventory_data}"
    )

    question = (
        f"Given these keyword opportunities and existing content, recommend "
        f"the top 5 content pieces to create or optimize. For each, specify:\n"
        f"1. Action: new_article | optimize_existing | update_meta\n"
        f"2. Content type: review | comparison | how_to | landing_page\n"
        f"3. Target keyword\n"
        f"4. Suggested title (under 60 chars)\n"
        f"5. Expected impact (high/medium/low) with reasoning\n"
        f"6. Any existing content overlap to watch for\n\n"
        f"Today is {date.today()}.{' Site: ' + site if site else ''}"
    )

    report = analyze(SYSTEM_PROMPT, data_context, question)

    full_report = (
        f"# Content Opportunities Report{site_label}\n"
        f"## {date.today()}\n\n"
        f"{report}"
    )

    if to_stdout:
        print(full_report)
    else:
        _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        path = _REPORTS_DIR / "content_opportunities.md"
        path.write_text(full_report, encoding="utf-8")
        log.info(f"Content opportunities report saved to {path}")

    return full_report
