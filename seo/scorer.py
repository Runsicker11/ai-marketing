"""Score published content against Search Console + GA4 performance data."""

from datetime import date, datetime, timezone
from pathlib import Path

from ingestion.utils.bq_client import run_query, load_rows
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.logger import get_logger
from ingestion.analysis.claude_client import analyze
from ingestion import schemas

log = get_logger(__name__)

_DS = f"{GCP_PROJECT_ID}.{BQ_DATASET}"
_REPORTS_DIR = Path(__file__).resolve().parents[1] / "reports"

SYSTEM_PROMPT = """\
You are an SEO performance analyst for Pickleball Effect. You evaluate published \
content against Search Console and GA4 data to determine what's working and what \
needs optimization.

For each piece of content, analyze:
1. Ranking performance (current position vs target)
2. Click-through rate (is the title/meta description compelling?)
3. Traffic quality (sessions, conversions, revenue)
4. Time-based trajectory (improving, declining, or stagnant)

Provide specific optimization recommendations:
- Title/meta description rewrites for low-CTR pages
- Content updates for declining rankings
- Internal linking suggestions for underperformers
- Keyword expansion ideas for top performers\
"""


def _query_content_performance() -> list[dict]:
    """Get content performance from the view."""
    sql = f"""
    SELECT post_id, title, target_keyword, content_type, platform,
           status, url, word_count, publish_date, days_since_publish,
           current_position, impressions_7d, clicks_7d, ctr,
           ga4_sessions, ga4_conversions, ga4_revenue, performance_tier
    FROM `{_DS}.vw_content_performance`
    WHERE days_since_publish >= 14
    ORDER BY impressions_7d DESC
    """
    try:
        rows = list(run_query(sql))
        return [dict(r) for r in rows]
    except Exception as e:
        log.warning(f"Could not query vw_content_performance: {e}")
        return []


def _format_performance_for_claude(content: list[dict]) -> str:
    """Format content performance as a table for Claude."""
    if not content:
        return "No content with 14+ days of data found."

    header = ("title | keyword | position | impr_7d | clicks_7d | ctr | "
              "sessions | conv | revenue | days | tier")
    lines = [header, "-" * len(header)]
    for c in content:
        lines.append(
            f"{(c.get('title') or '')[:40]} | "
            f"{c.get('target_keyword', '')} | "
            f"{c.get('current_position') or '-'} | "
            f"{c.get('impressions_7d') or 0} | "
            f"{c.get('clicks_7d') or 0} | "
            f"{c.get('ctr') or 0:.2%} | "
            f"{c.get('ga4_sessions') or 0} | "
            f"{c.get('ga4_conversions') or 0} | "
            f"${c.get('ga4_revenue') or 0:.2f} | "
            f"{c.get('days_since_publish') or 0} | "
            f"{c.get('performance_tier', '')}"
        )
    return "\n".join(lines)


def _update_content_statuses(content: list[dict]):
    """Update content_posts statuses based on performance."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    for c in content:
        tier = c.get("performance_tier", "")
        current_status = c.get("status", "")

        if current_status in ("draft", "retired"):
            continue

        new_status = current_status
        if tier == "top_performer":
            new_status = "performing"
        elif tier == "underperforming" and c.get("days_since_publish", 0) > 30:
            new_status = "underperforming"

        if new_status != current_status:
            post_id = c.get("post_id", "")
            sql = f"""
            UPDATE `{_DS}.content_posts`
            SET status = '{new_status}', updated_at = '{now_str}'
            WHERE post_id = '{post_id}'
            """
            try:
                run_query(sql)
                log.info(f"Updated {post_id} status: {current_status} -> {new_status}")
            except Exception as e:
                log.warning(f"Could not update {post_id}: {e}")


def run_scoring(to_stdout: bool = False) -> str:
    """Score published content and generate optimization report.

    Returns:
        The content performance report text.
    """
    log.info("Starting content performance scoring")

    content = _query_content_performance()
    if not content:
        msg = "No published content with 14+ days of data found."
        log.warning(msg)
        if to_stdout:
            print(msg)
        return msg

    log.info(f"Scoring {len(content)} published content pieces")

    # Update statuses in BigQuery
    _update_content_statuses(content)

    # Send to Claude for analysis
    data_context = _format_performance_for_claude(content)
    question = (
        f"Analyze these {len(content)} published content pieces. For each, "
        f"evaluate performance and recommend specific optimizations. "
        f"Group recommendations by priority (high/medium/low impact). "
        f"For underperformers, suggest specific title/meta rewrites. "
        f"For top performers, suggest keyword expansion opportunities."
    )

    report = analyze(SYSTEM_PROMPT, data_context, question)

    today = date.today().isoformat()
    full_report = (
        f"# Content Performance Report\n"
        f"## {today}\n\n"
        f"Analyzed: {len(content)} published content pieces\n\n"
        f"{report}"
    )

    if to_stdout:
        print(full_report)
    else:
        _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        path = _REPORTS_DIR / f"content_performance_{today}.md"
        path.write_text(full_report, encoding="utf-8")
        log.info(f"Content performance report saved to {path}")

    return full_report
