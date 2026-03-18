"""Shadow comparison: system proposals vs actual account changes.

Compares what the autonomous system proposed against what the agency
actually changed, identifying matches, misses, and unique catches.
"""

from datetime import date, timedelta
from pathlib import Path

from ingestion.utils.bq_client import run_query
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.logger import get_logger
from ingestion.analysis.claude_client import analyze

log = get_logger(__name__)

_DS = f"{GCP_PROJECT_ID}.{BQ_DATASET}"
_REPORTS_DIR = Path(__file__).resolve().parents[1] / "reports"
_SONNET_MODEL = "claude-sonnet-4-5-20250929"

SYSTEM_PROMPT = """\
You are a marketing operations analyst reviewing the performance of an \
autonomous ad optimization system running in shadow mode alongside a human \
agency managing Google Ads and Meta for Pickleball Effect.

Compare the system's proposals against the actual account changes made by the \
agency. Produce a clear, actionable report covering:

1. **Matches** — proposals that align with agency actions (validates the system)
2. **System Catches** — proposals the system made that the agency missed \
(shows value of automation)
3. **Agency Actions** — changes the agency made that the system didn't propose \
(identifies gaps in system logic)
4. **Accuracy Score** — rough percentage of system proposals that were \
directionally correct
5. **Recommendations** — what to tune in the system based on this comparison

Be specific about dollar amounts and campaign names.\
"""


def _query_proposals(days_back: int = 7) -> str:
    """Get system proposals from the last N days."""
    sql = f"""
    SELECT action_id, action_type, platform, entity_id, entity_name,
           current_value, proposed_value, rationale, risk_level,
           status, proposed_at
    FROM `{_DS}.optimization_actions`
    WHERE proposed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
    ORDER BY proposed_at DESC
    """
    rows = list(run_query(sql))
    if not rows:
        return "No system proposals found in this period."
    header = ("action_id | type | entity | current | proposed | rationale "
              "| risk | status | proposed_at")
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.action_id} | {r.action_type} | {r.entity_name} | "
            f"{r.current_value} | {r.proposed_value} | "
            f"{r.rationale[:80]}... | {r.risk_level} | {r.status} | "
            f"{r.proposed_at}"
        )
    return "\n".join(lines)


def _query_campaign_changes(days_back: int = 7) -> str:
    """Get week-over-week campaign changes (budget, status)."""
    sql = f"""
    WITH current_week AS (
        SELECT campaign_id, campaign_name, status, daily_budget,
               SUM(spend) AS total_spend
        FROM `{_DS}.google_ads_campaigns` c
        LEFT JOIN `{_DS}.google_ads_daily_insights` i
            ON c.campaign_id = i.campaign_id
            AND i.date_start >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
        GROUP BY campaign_id, campaign_name, status, daily_budget
    ),
    prev_week AS (
        SELECT campaign_id,
               SUM(spend) AS total_spend
        FROM `{_DS}.google_ads_daily_insights`
        WHERE date_start >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back * 2} DAY)
            AND date_start < DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
        GROUP BY campaign_id
    )
    SELECT cw.campaign_id, cw.campaign_name, cw.status, cw.daily_budget,
           cw.total_spend AS current_spend,
           pw.total_spend AS prev_spend,
           SAFE_DIVIDE(cw.total_spend - pw.total_spend, pw.total_spend) AS spend_change_pct
    FROM current_week cw
    LEFT JOIN prev_week pw ON cw.campaign_id = pw.campaign_id
    ORDER BY cw.total_spend DESC
    """
    rows = list(run_query(sql))
    if not rows:
        return "No campaign data available for comparison."
    header = ("campaign_id | campaign | status | daily_budget | "
              "current_spend | prev_spend | change_pct")
    lines = [header, "-" * len(header)]
    for r in rows:
        change = f"{r.spend_change_pct:.1%}" if r.spend_change_pct else "N/A"
        lines.append(
            f"{r.campaign_id} | {r.campaign_name} | {r.status} | "
            f"${r.daily_budget or 0:.2f} | ${r.current_spend or 0:.2f} | "
            f"${r.prev_spend or 0:.2f} | {change}"
        )
    return "\n".join(lines)


def generate_shadow_report(days_back: int = 7,
                           to_stdout: bool = False) -> str:
    """Compare system proposals vs actual account changes.

    Args:
        days_back: Number of days to look back.
        to_stdout: Print to stdout instead of saving to file.

    Returns:
        The shadow comparison report text.
    """
    log.info(f"Generating shadow comparison report ({days_back}-day window)")

    proposals_data = _query_proposals(days_back)
    changes_data = _query_campaign_changes(days_back)

    data_context = (
        f"### System Proposals (Last {days_back} Days)\n{proposals_data}\n\n"
        f"### Actual Campaign Changes (Week-over-Week)\n{changes_data}"
    )

    question = (
        f"Compare the autonomous system's proposals against the actual "
        f"account changes over the last {days_back} days. "
        f"What did the system get right? What did it miss? "
        f"What did the agency do that the system didn't propose? "
        f"Rate the system's accuracy and recommend tuning. "
        f"Today is {date.today()}."
    )

    report = analyze(SYSTEM_PROMPT, data_context, question,
                     model=_SONNET_MODEL)

    full_report = (
        f"# Shadow Mode Comparison Report\n"
        f"## {date.today()} (Last {days_back} Days)\n\n"
        f"{report}"
    )

    if to_stdout:
        print(full_report)
    else:
        _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        path = _REPORTS_DIR / f"shadow_report_{date.today()}.md"
        path.write_text(full_report, encoding="utf-8")
        log.info(f"Shadow report saved to {path}")

    return full_report
