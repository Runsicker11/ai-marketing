"""Score content library components against live ad performance and evolve the library."""

from datetime import date, datetime, timezone
from pathlib import Path

from ingestion.utils.bq_client import run_query, full_replace
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.logger import get_logger
from ingestion.analysis.claude_client import analyze
from ingestion import schemas

log = get_logger(__name__)

_DS = f"{GCP_PROJECT_ID}.{BQ_DATASET}"
_REPORTS_DIR = Path(__file__).resolve().parents[2] / "reports"

SYSTEM_PROMPT = """\
You are a performance analyst for a DTC pickleball accessories brand's ad copy. \
You evaluate which ad copy components are working and recommend next actions.

Given component performance scores, you will:
1. Identify the top 5 winners and explain WHY they work
2. Identify underperformers and recommend retirement
3. For each winner, suggest 3-5 new variations to test (brief, one line each)
4. Recommend overall library adjustments

Be specific and data-driven. Reference exact component IDs and metrics.\
"""


def _query_component_scores() -> list[dict]:
    """Get scored components from the vw_component_scores view."""
    sql = f"""
    SELECT component_id, component_type, text,
           ads_using, total_spend, total_purchases,
           weighted_roas, avg_ctr, avg_cpa, max_days_active
    FROM `{_DS}.vw_component_scores`
    ORDER BY weighted_roas DESC
    """
    try:
        rows = list(run_query(sql))
        return [dict(r) for r in rows]
    except Exception as e:
        log.warning(f"Could not query vw_component_scores: {e}")
        return []


def _query_all_library() -> list[dict]:
    """Get all content library entries."""
    sql = f"""
    SELECT component_id, component_type, text, score, status,
           source, source_ad_name, product_focus
    FROM `{_DS}.content_library`
    ORDER BY component_type, component_id
    """
    rows = list(run_query(sql))
    return [dict(r) for r in rows]


def _format_scores_for_claude(scored: list[dict], library: list[dict]) -> str:
    """Format component scores as a table for Claude."""
    lines = ["### Scored Components (matched to live ads, 7+ days data)"]
    if scored:
        header = "id | type | text | ads | spend | purchases | roas | ctr | cpa | days"
        lines.extend([header, "-" * len(header)])
        for s in scored:
            lines.append(
                f"{s.get('component_id', '')} | "
                f"{s.get('component_type', '')} | "
                f"{(s.get('text', '') or '')[:50]} | "
                f"{s.get('ads_using', 0)} | "
                f"${s.get('total_spend') or 0:.2f} | "
                f"{s.get('total_purchases') or 0} | "
                f"{s.get('weighted_roas') or 0:.2f} | "
                f"{s.get('avg_ctr') or 0:.2f}% | "
                f"${s.get('avg_cpa') or 0:.2f} | "
                f"{s.get('max_days_active') or 0}"
            )
    else:
        lines.append("No components have matched to live ads with 7+ days data yet.")

    lines.append(f"\n### Full Library ({len(library)} components)")
    header = "id | type | text | score | status"
    lines.extend([header, "-" * len(header)])
    for item in library:
        lines.append(
            f"{item.get('component_id', '')} | "
            f"{item.get('component_type', '')} | "
            f"{(item.get('text', '') or '')[:50]} | "
            f"{item.get('score') or 0:.1f} | "
            f"{item.get('status', '')}"
        )

    return "\n".join(lines)


def _update_library_scores(scored: list[dict], library: list[dict]) -> list[dict]:
    """Update library entries with live performance scores and status changes."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Build lookup from scored components
    score_lookup = {}
    for s in scored:
        cid = s.get("component_id")
        if cid:
            roas = s.get("weighted_roas") or 0
            # Convert ROAS to 1-10 score: 3.0+ ROAS = 10, 0.0 ROAS = 1
            new_score = min(10, max(1, round(roas * 3.33)))
            score_lookup[cid] = {
                "score": new_score,
                "roas": roas,
            }

    updated = []
    for item in library:
        cid = item["component_id"]
        row = {
            "component_id": cid,
            "component_type": item["component_type"],
            "text": item["text"],
            "source": item.get("source", "audit"),
            "source_ad_id": item.get("source_ad_id"),
            "source_ad_name": item.get("source_ad_name"),
            "score": item.get("score") or 5.0,
            "status": item.get("status", "active"),
            "product_focus": item.get("product_focus"),
            "created_at": item.get("created_at", now_str),
            "updated_at": now_str,
        }

        if cid in score_lookup:
            row["score"] = score_lookup[cid]["score"]
            roas = score_lookup[cid]["roas"]
            # Promote winners, retire losers
            if roas >= 2.5:
                row["status"] = "proven"
            elif roas < 0.5 and item.get("status") != "proven":
                row["status"] = "retired"

        updated.append(row)

    return updated


def run_scoring(to_stdout: bool = False) -> str:
    """Score library components against live performance and update statuses.

    Returns:
        The scoring report text.
    """
    log.info("Starting content scoring")

    # 1. Get scored components from the view
    scored = _query_component_scores()

    # 2. Get full library
    library = _query_all_library()
    if not library:
        msg = "Content library is empty. Run --audit first."
        log.warning(msg)
        return msg

    log.info(f"Library has {len(library)} components, {len(scored)} matched to live ads")

    # 3. Update scores in BigQuery
    if scored:
        updated_rows = _update_library_scores(scored, library)
        full_replace("content_library", updated_rows, schemas.CONTENT_LIBRARY)
        log.info("Updated content_library with live performance scores")

    # 4. Send to Claude for analysis
    data_context = _format_scores_for_claude(scored, library)
    question = (
        f"Analyze these {len(scored)} scored components and {len(library)} total library entries. "
        "Identify winners to double down on, losers to retire, and suggest new variations "
        "for the top performers. Be specific about component IDs."
    )

    report = analyze(SYSTEM_PROMPT, data_context, question)

    # 5. Save report
    today = date.today().isoformat()
    full_report = (
        f"# Content Performance Report\n"
        f"## {today}\n\n"
        f"Scored: {len(scored)} components matched to live ads\n"
        f"Library: {len(library)} total components\n\n"
        f"{report}"
    )

    if to_stdout:
        print(full_report)
    else:
        _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        path = _REPORTS_DIR / f"content_performance_{today}.md"
        path.write_text(full_report, encoding="utf-8")
        log.info(f"Scoring report saved to {path}")

    return full_report
