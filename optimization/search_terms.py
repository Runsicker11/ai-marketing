"""AI-powered search term hygiene: negative keywords + keyword expansion."""

import json
from datetime import date
from pathlib import Path

import yaml

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


# ─── Structured Proposal Generation ─────────────────────────

_SONNET_MODEL = "claude-sonnet-4-5-20250929"
_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"

PROPOSE_SYSTEM_PROMPT = """\
You are a Google Ads search term analyst for Pickleball Effect, a DTC pickleball \
accessories brand. You review search term reports and output STRUCTURED JSON proposals.

You must respond with ONLY a JSON array. No markdown, no explanation, no code fences.

Each element must be an object with exactly these fields:
- "action_type": one of "add_negative_keyword", "add_as_keyword", "monitor"
- "search_term": the actual search term text
- "campaign_id": the numeric campaign ID as a string (e.g. "12345678")
- "campaign_name": the campaign name
- "match_type": one of "exact", "phrase", "broad"
- "rationale": 1-2 sentence explanation
- "expected_impact": estimated weekly savings or value (e.g. "Save ~$5/week")
- "risk_level": one of "low", "medium", "high"

Rules:
- Only include "add_negative_keyword" or "add_as_keyword" actions, not "monitor"
- For negative keywords, prefer "phrase" match unless the term is very specific (use "exact")
- For add_as_keyword, recommend "exact" match for proven converters
- Be conservative: only propose negatives for clearly irrelevant terms
- Use the campaign_id from the data — never invent an ID\
"""


def _query_terms_with_campaign_ids() -> str:
    """Get search terms with campaign IDs from the raw table."""
    sql = f"""
    SELECT
        search_term,
        CAST(campaign_id AS STRING) AS campaign_id,
        campaign_name,
        SUM(spend) AS total_spend,
        SUM(clicks) AS total_clicks,
        SUM(conversions) AS total_conversions,
        SUM(conversion_value) AS total_value,
        SAFE_DIVIDE(SUM(conversion_value), SUM(spend)) AS roas,
        COUNT(DISTINCT date_start) AS days_seen
    FROM `{_DS}.google_ads_search_terms`
    WHERE date_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    GROUP BY search_term, campaign_id, campaign_name
    ORDER BY total_spend DESC
    LIMIT 50
    """
    rows = list(run_query(sql))
    if not rows:
        return "No recent search terms found."
    header = ("search_term | campaign_id | campaign_name | spend | clicks "
              "| conversions | value | roas | days")
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.search_term} | {r.campaign_id} | {r.campaign_name} | "
            f"${r.total_spend or 0:.2f} | {r.total_clicks or 0} | "
            f"{r.total_conversions or 0:.1f} | ${r.total_value or 0:.2f} | "
            f"{r.roas or 0:.2f} | {r.days_seen or 0}"
        )
    return "\n".join(lines)


def _load_autonomy_config() -> dict:
    """Load autonomy config from thresholds.yaml."""
    path = _CONFIG_DIR / "thresholds.yaml"
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    return cfg.get("autonomy", {})


def _parse_proposals(claude_response: str) -> list[dict]:
    """Parse JSON proposals from Claude's response.

    Follows the _parse_generated() pattern: lenient parsing, log warnings,
    don't fail on individual bad items.
    """
    # Strip markdown code fences if present
    text = claude_response.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first and last lines (code fences)
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)

    try:
        items = json.loads(text)
    except json.JSONDecodeError as e:
        log.warning(f"Failed to parse JSON response: {e}")
        log.warning(f"Raw response (first 500 chars): {text[:500]}")
        return []

    if not isinstance(items, list):
        log.warning(f"Expected JSON array, got {type(items).__name__}")
        return []

    required_fields = {
        "action_type", "search_term", "campaign_id", "campaign_name",
        "match_type", "rationale", "expected_impact", "risk_level",
    }
    valid_action_types = {"add_negative_keyword", "add_as_keyword", "monitor"}
    valid_match_types = {"exact", "phrase", "broad"}
    valid_risk_levels = {"low", "medium", "high"}

    proposals = []
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            log.warning(f"Proposal {i}: expected dict, got {type(item).__name__}, skipping")
            continue

        missing = required_fields - set(item.keys())
        if missing:
            log.warning(f"Proposal {i}: missing fields {missing}, skipping")
            continue

        if item["action_type"] not in valid_action_types:
            log.warning(f"Proposal {i}: invalid action_type '{item['action_type']}', skipping")
            continue

        if item["match_type"] not in valid_match_types:
            log.warning(f"Proposal {i}: invalid match_type '{item['match_type']}', "
                        f"defaulting to 'phrase'")
            item["match_type"] = "phrase"

        if item["risk_level"] not in valid_risk_levels:
            log.warning(f"Proposal {i}: invalid risk_level '{item['risk_level']}', "
                        f"defaulting to 'medium'")
            item["risk_level"] = "medium"

        # Skip monitor-only items — they don't become proposals
        if item["action_type"] == "monitor":
            continue

        proposals.append(item)

    return proposals


def review_and_propose(to_stdout: bool = False) -> int:
    """Review search terms and create structured optimization proposals.

    Returns:
        Number of proposals created.
    """
    from optimization.actions import create_proposal

    log.info("Reviewing search terms for structured proposals")

    autonomy_cfg = _load_autonomy_config()
    max_proposals = autonomy_cfg.get("max_proposals_per_run", 10)

    # Gather data — reuse existing queries + add campaign ID query
    wasted = _query_wasted_search_terms()
    converting = _query_high_converting_terms()
    terms_with_ids = _query_terms_with_campaign_ids()

    data_context = (
        f"### Wasted Search Terms (Zero Conversions)\n{wasted}\n\n"
        f"### High-Converting Terms NOT Added as Keywords\n{converting}\n\n"
        f"### All Recent Terms with Campaign IDs\n{terms_with_ids}"
    )

    question = (
        f"Analyze these search terms for Pickleball Effect and return a JSON array "
        f"of optimization proposals. Focus on:\n"
        f"1. Wasted terms that should be added as negative keywords\n"
        f"2. High-converting terms that should be added as exact-match keywords\n"
        f"Only include actionable items (add_negative_keyword or add_as_keyword). "
        f"Use campaign_id values from the data — never invent IDs.\n"
        f"Return at most {max_proposals} proposals, prioritized by expected impact.\n"
        f"Today is {date.today()}."
    )

    response = analyze(PROPOSE_SYSTEM_PROMPT, data_context, question,
                       model=_SONNET_MODEL)

    proposals = _parse_proposals(response)
    if not proposals:
        log.warning("No valid proposals parsed from Claude's response")
        if to_stdout:
            print("No actionable proposals generated.")
            print(f"\nRaw response:\n{response[:1000]}")
        return 0

    # Cap at max_proposals
    proposals = proposals[:max_proposals]

    created = 0
    for p in proposals:
        action_type = p["action_type"]

        if action_type == "add_negative_keyword":
            current_val = f"search_term: {p['search_term']}"
            proposed_val = p["search_term"]
            risk = p.get("risk_level", "low")
        elif action_type == "add_as_keyword":
            current_val = f"not a keyword yet"
            proposed_val = f"{p['search_term']} [{p['match_type']}]"
            risk = p.get("risk_level", "medium")
        else:
            continue

        try:
            create_proposal(
                action_type=action_type,
                platform="google_ads",
                entity_id=str(p["campaign_id"]),
                entity_name=p["campaign_name"],
                current_value=current_val,
                proposed_value=proposed_val,
                rationale=p["rationale"],
                expected_impact=p["expected_impact"],
                risk_level=p.get("risk_level", risk),
            )
            created += 1
        except Exception as e:
            log.warning(f"Failed to create proposal for '{p['search_term']}': {e}")

    log.info(f"Created {created} search term proposals")

    if to_stdout:
        print(f"\n{'='*60}")
        print(f"Search Term Proposals Created: {created}")
        print(f"{'='*60}")
        for p in proposals[:created]:
            print(f"\n  [{p['action_type']}] {p['search_term']}")
            print(f"    Campaign: {p['campaign_name']} ({p['campaign_id']})")
            print(f"    Match: {p['match_type']} | Risk: {p['risk_level']}")
            print(f"    Rationale: {p['rationale']}")
            print(f"    Impact: {p['expected_impact']}")

    return created
