"""Audit existing ad creatives: analyze patterns, populate content library."""

import csv
from datetime import datetime, timezone
from pathlib import Path

from ingestion.utils.bq_client import run_query, full_replace
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.logger import get_logger
from ingestion.analysis.claude_client import analyze
from ingestion import schemas

log = get_logger(__name__)

_DS = f"{GCP_PROJECT_ID}.{BQ_DATASET}"
_LIBRARY_DIR = Path(__file__).resolve().parent / "library"
_REPORTS_DIR = Path(__file__).resolve().parents[1] / "reports"

SYSTEM_PROMPT = """\
You are a direct-response ad copy analyst for a DTC pickleball accessories brand. \
You analyze ad creative text to find patterns in what makes winners win and losers lose.

You will receive a list of ad creatives with their performance metrics. \
Your job is to:
1. Identify patterns in top-performing creatives (hooks, emotional triggers, value props)
2. Identify patterns in underperforming creatives (what to avoid)
3. Extract reusable components: hooks (headlines), bodies (primary text), CTAs
4. Score each extracted component 1-10 based on performance of the ads using it

Output your analysis in two sections:

## Analysis
Narrative analysis of what's working and what isn't. Be specific about patterns.

## Components
Output each component in this exact CSV format (one per line):

TYPE,TEXT,SCORE,SOURCE_AD_NAME
hook,"The exact headline text",8,Ad Name Here
body,"The exact primary text",7,Ad Name Here
cta,SHOP_NOW,6,Ad Name Here

Rules for extraction:
- hooks: Extract from headline/title field. If empty, skip.
- bodies: Extract from primary_text field. If empty, skip.
- ctas: Extract the CTA type (e.g., SHOP_NOW, LEARN_MORE). If empty, skip.
- Score 1-10 based on the ad's ROAS and CTR relative to others in the set.
- Only extract components that have actual text (skip nulls/empties).\
"""


def _query_creative_performance() -> list[dict]:
    """Get all creatives with performance data."""
    sql = f"""
    SELECT creative_id, ad_id, ad_name, headline, primary_text, cta_type,
           object_type, lifetime_spend, lifetime_roas, lifetime_ctr,
           lifetime_cpa, lifetime_purchases, days_active,
           performance_tier, health_status
    FROM `{_DS}.vw_creative_performance`
    WHERE lifetime_spend > 0
    ORDER BY lifetime_roas DESC
    """
    rows = list(run_query(sql))
    return [dict(r) for r in rows]


def _format_creatives_for_claude(creatives: list[dict]) -> str:
    """Format creative data as a table for Claude."""
    if not creatives:
        return "No creative performance data available."

    header = "ad_name | headline | primary_text | cta | roas | ctr | cpa | spend | purchases | tier"
    lines = [header, "-" * len(header)]
    for c in creatives:
        lines.append(
            f"{c.get('ad_name', '')} | "
            f"{c.get('headline', '') or '(none)'} | "
            f"{(c.get('primary_text', '') or '(none)')[:80]} | "
            f"{c.get('cta_type', '') or '(none)'} | "
            f"{c.get('lifetime_roas') or 0:.2f} | "
            f"{c.get('lifetime_ctr') or 0:.2f}% | "
            f"${c.get('lifetime_cpa') or 0:.2f} | "
            f"${c.get('lifetime_spend') or 0:.2f} | "
            f"{c.get('lifetime_purchases') or 0} | "
            f"{c.get('performance_tier', '')}"
        )
    return "\n".join(lines)


def _parse_components(claude_response: str) -> list[dict]:
    """Parse component CSV lines from Claude's response."""
    components = []
    in_components = False

    for line in claude_response.split("\n"):
        stripped = line.strip()
        if stripped.startswith("## Components"):
            in_components = True
            continue
        if in_components and stripped.startswith("## "):
            break
        if not in_components:
            continue

        # Skip empty lines, headers, markdown formatting
        if not stripped or stripped.startswith("TYPE,") or stripped.startswith("```"):
            continue

        # Parse CSV-like component lines
        parts = list(csv.reader([stripped]))
        if not parts or not parts[0]:
            continue
        row = parts[0]
        if len(row) < 3:
            continue

        comp_type = row[0].strip().lower()
        if comp_type not in ("hook", "body", "cta"):
            continue

        text = row[1].strip()
        try:
            score = float(row[2].strip())
        except (ValueError, IndexError):
            score = 5.0

        source_ad = row[3].strip() if len(row) > 3 else ""

        components.append({
            "component_type": comp_type,
            "text": text,
            "score": score,
            "source_ad_name": source_ad,
        })

    return components


def _assign_ids(components: list[dict]) -> list[dict]:
    """Assign component IDs like H001, B001, C001."""
    counters = {"hook": 0, "body": 0, "cta": 0}
    prefixes = {"hook": "H", "body": "B", "cta": "C"}

    for comp in components:
        ct = comp["component_type"]
        counters[ct] += 1
        comp["component_id"] = f"{prefixes[ct]}{counters[ct]:03d}"

    return components


def _save_library_csvs(components: list[dict]):
    """Save components to CSV files in content/library/."""
    _LIBRARY_DIR.mkdir(parents=True, exist_ok=True)

    for comp_type in ("hook", "body", "cta"):
        items = [c for c in components if c["component_type"] == comp_type]
        path = _LIBRARY_DIR / f"{comp_type}s.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["component_id", "text", "score", "source_ad_name", "status"])
            for item in items:
                writer.writerow([
                    item["component_id"],
                    item["text"],
                    item["score"],
                    item.get("source_ad_name", ""),
                    "active",
                ])
        log.info(f"Saved {len(items)} {comp_type}s to {path}")


def _build_bq_rows(components: list[dict]) -> list[dict]:
    """Convert components to BigQuery content_library rows."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for c in components:
        rows.append({
            "component_id": c["component_id"],
            "component_type": c["component_type"],
            "text": c["text"],
            "source": "audit",
            "source_ad_id": None,
            "source_ad_name": c.get("source_ad_name"),
            "score": c["score"],
            "status": "active",
            "product_focus": None,
            "created_at": now_str,
            "updated_at": now_str,
        })
    return rows


def run_audit(to_stdout: bool = False) -> str:
    """Run full creative audit: query data, analyze with Claude, populate library.

    Returns:
        The audit report text.
    """
    log.info("Starting content audit")

    # 1. Query creative performance
    creatives = _query_creative_performance()
    if not creatives:
        msg = "No creative performance data found. Run Meta ingestion first."
        log.warning(msg)
        return msg

    log.info(f"Found {len(creatives)} creatives with performance data")

    # 2. Send to Claude for analysis
    data_context = _format_creatives_for_claude(creatives)
    question = (
        f"Analyze these {len(creatives)} ad creatives for Pickleball Effect. "
        "Identify the patterns that separate winners from losers, then extract "
        "all reusable components (hooks, bodies, CTAs) with scores. "
        "Focus on what makes the top-performing ads succeed."
    )

    report = analyze(SYSTEM_PROMPT, data_context, question)

    # 3. Parse components from Claude's response
    components = _parse_components(report)
    if components:
        components = _assign_ids(components)
        log.info(f"Extracted {len(components)} components from audit")

        # 4. Save to CSVs
        _save_library_csvs(components)

        # 5. Load into BigQuery
        bq_rows = _build_bq_rows(components)
        full_replace("content_library", bq_rows, schemas.CONTENT_LIBRARY)
    else:
        log.warning("No components extracted from Claude's response")

    # 6. Save report
    full_report = f"# Content Audit Report\n\n{report}"

    if to_stdout:
        print(full_report)
    else:
        _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        path = _REPORTS_DIR / "content_audit.md"
        path.write_text(full_report, encoding="utf-8")
        log.info(f"Audit report saved to {path}")

    return full_report
