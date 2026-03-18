"""AI-powered cross-channel budget intelligence and reallocation recommendations."""

import json
from datetime import date, timedelta
from pathlib import Path

import yaml

from ingestion.utils.bq_client import run_query
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.logger import get_logger
from ingestion.analysis.claude_client import analyze

log = get_logger(__name__)

_DS = f"{GCP_PROJECT_ID}.{BQ_DATASET}"
_REPORTS_DIR = Path(__file__).resolve().parents[1] / "reports"
_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"

SYSTEM_PROMPT = """\
You are a media buyer and budget strategist for Pickleball Effect, a DTC \
pickleball accessories brand with ~$2K/month ad spend across Meta and Google Ads.

You analyze cross-channel performance to recommend budget shifts that maximize \
total ROAS. Your recommendations must:
1. Be specific about dollar amounts (e.g., "Shift $5/day from Campaign X to Campaign Y")
2. Respect budget rules (max 20% daily shift, min $10/campaign)
3. Factor in diminishing returns (increasing spend doesn't linearly increase returns)
4. Consider day-of-week patterns
5. Compare Meta vs Google Ads efficiency
6. Flag any campaigns where spend should be paused or significantly increased

Be data-driven and conservative. Small, testable shifts > big risky moves.\
"""


def _load_budget_rules() -> dict:
    """Load budget rules from thresholds.yaml."""
    path = _CONFIG_DIR / "thresholds.yaml"
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    return cfg.get("budget_rules", {})


def _query_channel_performance() -> str:
    """Get 30-day channel performance summary."""
    sql = f"""
    SELECT channel,
           SUM(spend) AS total_spend,
           SUM(revenue) AS total_revenue,
           SUM(orders) AS total_orders,
           SAFE_DIVIDE(SUM(revenue), SUM(spend)) AS roas,
           SAFE_DIVIDE(SUM(spend), SUM(orders)) AS cpa,
           COUNT(DISTINCT report_date) AS days
    FROM `{_DS}.vw_channel_summary`
    WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND spend IS NOT NULL AND spend > 0
    GROUP BY channel
    ORDER BY total_spend DESC
    """
    rows = list(run_query(sql))
    if not rows:
        return "No channel performance data available."
    header = "channel | spend | revenue | orders | roas | cpa | days"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.channel} | ${r.total_spend or 0:.2f} | "
            f"${r.total_revenue or 0:.2f} | {r.total_orders or 0} | "
            f"{r.roas or 0:.2f} | ${r.cpa or 0:.2f} | {r.days}"
        )
    return "\n".join(lines)


def _query_campaign_performance() -> str:
    """Get campaign-level performance for Meta + Google Ads."""
    sql = f"""
    SELECT platform, campaign_name,
           SUM(spend) AS total_spend,
           SUM(conversion_value) AS total_revenue,
           SUM(conversions) AS total_conversions,
           SAFE_DIVIDE(SUM(conversion_value), SUM(spend)) AS roas,
           SAFE_DIVIDE(SUM(spend), SUM(conversions)) AS cpa,
           COUNT(DISTINCT report_date) AS days_active
    FROM `{_DS}.vw_daily_performance`
    WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND spend > 0
    GROUP BY platform, campaign_name
    ORDER BY total_spend DESC
    """
    rows = list(run_query(sql))
    if not rows:
        return "No campaign performance data available."
    header = "platform | campaign | spend | revenue | conv | roas | cpa | days"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.platform} | {r.campaign_name} | "
            f"${r.total_spend or 0:.2f} | ${r.total_revenue or 0:.2f} | "
            f"{r.total_conversions or 0:.1f} | {r.roas or 0:.2f} | "
            f"${r.cpa or 0:.2f} | {r.days_active}"
        )
    return "\n".join(lines)


def _query_day_of_week_patterns() -> str:
    """Get day-of-week performance patterns."""
    sql = f"""
    SELECT
        FORMAT_DATE('%A', report_date) AS day_name,
        EXTRACT(DAYOFWEEK FROM report_date) AS day_num,
        AVG(spend) AS avg_spend,
        AVG(conversion_value) AS avg_revenue,
        AVG(SAFE_DIVIDE(conversion_value, spend)) AS avg_roas,
        SUM(conversions) AS total_conversions
    FROM `{_DS}.vw_daily_performance`
    WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND spend > 0
    GROUP BY day_name, day_num
    ORDER BY day_num
    """
    rows = list(run_query(sql))
    if not rows:
        return "No day-of-week data available."
    header = "day | avg_spend | avg_revenue | avg_roas | total_conv"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.day_name} | ${r.avg_spend or 0:.2f} | "
            f"${r.avg_revenue or 0:.2f} | {r.avg_roas or 0:.2f} | "
            f"{r.total_conversions or 0:.1f}"
        )
    return "\n".join(lines)


def _query_spend_trend() -> str:
    """Get daily spend trend for last 30 days."""
    sql = f"""
    SELECT report_date, channel,
           spend, revenue, roas, orders
    FROM `{_DS}.vw_channel_summary`
    WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND spend IS NOT NULL AND spend > 0
    ORDER BY report_date, channel
    """
    rows = list(run_query(sql))
    if not rows:
        return "No spend trend data available."
    header = "date | channel | spend | revenue | roas | orders"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.report_date} | {r.channel} | "
            f"${r.spend or 0:.2f} | ${r.revenue or 0:.2f} | "
            f"{r.roas or 0:.2f} | {r.orders or 0}"
        )
    return "\n".join(lines)


def recommend(to_stdout: bool = False) -> str:
    """Generate budget reallocation recommendations.

    Returns:
        The budget recommendations report text.
    """
    log.info("Generating budget intelligence report")

    budget_rules = _load_budget_rules()
    channel_data = _query_channel_performance()
    campaign_data = _query_campaign_performance()
    dow_data = _query_day_of_week_patterns()
    trend_data = _query_spend_trend()

    data_context = (
        f"### Channel Performance (Last 30 Days)\n{channel_data}\n\n"
        f"### Campaign Performance (Last 30 Days)\n{campaign_data}\n\n"
        f"### Day-of-Week Patterns\n{dow_data}\n\n"
        f"### Daily Spend Trend\n{trend_data}\n\n"
        f"### Budget Rules\n"
        f"- Max daily shift: {budget_rules.get('max_daily_shift_pct', 20)}%\n"
        f"- Min campaign daily spend: ${budget_rules.get('min_campaign_daily_spend', 10)}\n"
        f"- Min days before decision: {budget_rules.get('min_days_before_decision', 7)}\n"
    )

    question = (
        f"Given 30 days of cross-channel data, recommend specific budget "
        f"allocation changes for this week. Include:\n"
        f"1. Channel-level shifts (Meta vs Google Ads)\n"
        f"2. Campaign-level shifts within each platform\n"
        f"3. Day-of-week optimization (increase/decrease spend by day)\n"
        f"4. Any campaigns to pause or scale\n\n"
        f"Monthly budget is ~$2,000 total. Today is {date.today()}."
    )

    report = analyze(SYSTEM_PROMPT, data_context, question)

    full_report = (
        f"# Budget Intelligence Report\n"
        f"## {date.today()}\n\n"
        f"{report}"
    )

    if to_stdout:
        print(full_report)
    else:
        _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        path = _REPORTS_DIR / f"budget_recommendations_{date.today()}.md"
        path.write_text(full_report, encoding="utf-8")
        log.info(f"Budget recommendations saved to {path}")

    return full_report


# ─── Structured Proposal Generation ─────────────────────────

_SONNET_MODEL = "claude-sonnet-4-5-20250929"

PROPOSE_SYSTEM_PROMPT = """\
You are a media buyer for Pickleball Effect, a DTC pickleball accessories brand \
with ~$2K/month ad spend. You output STRUCTURED JSON budget shift proposals.

You must respond with ONLY a JSON array. No markdown, no explanation, no code fences.

Each element must be an object with exactly these fields:
- "action_type": always "shift_budget"
- "source_campaign_id": campaign ID to reduce budget (string)
- "source_campaign_name": campaign name to reduce budget
- "target_campaign_id": campaign ID to increase budget (string)
- "target_campaign_name": campaign name to increase budget
- "current_daily_budget": current daily budget in dollars (number)
- "proposed_daily_budget": proposed new daily budget in dollars (number)
- "rationale": 1-2 sentence data-backed explanation
- "risk_level": one of "low", "medium", "high"

Rules:
- Max 20% shift per campaign per recommendation
- Never drop a campaign below $10/day
- Require at least 7 days of data before recommending changes
- Factor in product margins — shift toward campaigns driving high-margin products
- Use campaign IDs from the data — never invent IDs
- Be conservative: small testable shifts > big risky moves\
"""


def _query_campaign_performance_with_ids() -> str:
    """Get campaign performance with IDs for budget proposals."""
    sql = f"""
    SELECT platform, campaign_id, campaign_name,
           SUM(spend) AS total_spend,
           SUM(conversion_value) AS total_revenue,
           SUM(conversions) AS total_conversions,
           SAFE_DIVIDE(SUM(conversion_value), SUM(spend)) AS roas,
           SAFE_DIVIDE(SUM(spend), SUM(conversions)) AS cpa,
           COUNT(DISTINCT report_date) AS days_active
    FROM `{_DS}.vw_daily_performance`
    WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND spend > 0
    GROUP BY platform, campaign_id, campaign_name
    ORDER BY total_spend DESC
    """
    rows = list(run_query(sql))
    if not rows:
        return "No campaign performance data available."
    header = ("platform | campaign_id | campaign | spend | revenue "
              "| conv | roas | cpa | days")
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.platform} | {r.campaign_id} | {r.campaign_name} | "
            f"${r.total_spend or 0:.2f} | ${r.total_revenue or 0:.2f} | "
            f"{r.total_conversions or 0:.1f} | {r.roas or 0:.2f} | "
            f"${r.cpa or 0:.2f} | {r.days_active}"
        )
    return "\n".join(lines)


def _query_product_profitability() -> str:
    """Get product-level margin data to inform budget shifts."""
    sql = f"""
    SELECT title, SUM(units_sold) AS total_units,
           SUM(net_revenue) AS total_revenue,
           SUM(gross_profit) AS total_profit,
           AVG(gross_margin) AS avg_margin
    FROM `{_DS}.vw_product_profitability`
    WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY title
    ORDER BY total_profit DESC
    LIMIT 15
    """
    rows = list(run_query(sql))
    if not rows:
        return "No product profitability data available."
    header = "product | units | revenue | profit | margin"
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r.title} | {r.total_units or 0} | "
            f"${r.total_revenue or 0:.2f} | ${r.total_profit or 0:.2f} | "
            f"{r.avg_margin or 0:.1%}"
        )
    return "\n".join(lines)


def _load_autonomy_config() -> dict:
    """Load autonomy config from thresholds.yaml."""
    path = _CONFIG_DIR / "thresholds.yaml"
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    return cfg.get("autonomy", {})


def _parse_budget_proposals(claude_response: str) -> list[dict]:
    """Parse JSON budget proposals from Claude's response."""
    text = claude_response.strip()
    if text.startswith("```"):
        lines = text.split("\n")
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
        "action_type", "source_campaign_id", "source_campaign_name",
        "target_campaign_id", "target_campaign_name",
        "current_daily_budget", "proposed_daily_budget", "rationale",
        "risk_level",
    }
    valid_risk_levels = {"low", "medium", "high"}

    proposals = []
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            log.warning(f"Budget proposal {i}: expected dict, skipping")
            continue

        missing = required_fields - set(item.keys())
        if missing:
            log.warning(f"Budget proposal {i}: missing fields {missing}, skipping")
            continue

        if item["action_type"] != "shift_budget":
            log.warning(f"Budget proposal {i}: unexpected action_type "
                        f"'{item['action_type']}', skipping")
            continue

        if item["risk_level"] not in valid_risk_levels:
            item["risk_level"] = "medium"

        # Coerce budget values to float
        try:
            item["current_daily_budget"] = float(item["current_daily_budget"])
            item["proposed_daily_budget"] = float(item["proposed_daily_budget"])
        except (ValueError, TypeError):
            log.warning(f"Budget proposal {i}: non-numeric budget values, skipping")
            continue

        proposals.append(item)

    return proposals


def _apply_guardrails(proposals: list[dict], budget_rules: dict) -> list[dict]:
    """Filter proposals through budget guardrails."""
    max_shift_pct = budget_rules.get("max_daily_shift_pct", 20) / 100.0
    min_spend = budget_rules.get("min_campaign_daily_spend", 10)
    min_days = budget_rules.get("min_days_before_decision", 7)

    validated = []
    for p in proposals:
        current = p["current_daily_budget"]
        proposed = p["proposed_daily_budget"]

        # Check min spend
        if proposed < min_spend:
            log.warning(
                f"Budget proposal rejected: {p['target_campaign_name']} proposed "
                f"${proposed:.2f}/day < ${min_spend} minimum. "
                f"Clamping to ${min_spend}."
            )
            p["proposed_daily_budget"] = min_spend
            proposed = min_spend

        # Check max shift percentage
        if current > 0:
            shift_pct = abs(proposed - current) / current
            if shift_pct > max_shift_pct:
                if proposed > current:
                    clamped = current * (1 + max_shift_pct)
                else:
                    clamped = current * (1 - max_shift_pct)
                log.warning(
                    f"Budget proposal clamped: {p['target_campaign_name']} "
                    f"shift {shift_pct:.0%} exceeds {max_shift_pct:.0%} max. "
                    f"${proposed:.2f} → ${clamped:.2f}"
                )
                p["proposed_daily_budget"] = round(clamped, 2)

        # Skip no-op proposals (proposed == current after clamping)
        if abs(p["proposed_daily_budget"] - current) < 0.01:
            log.warning(
                f"Budget proposal skipped: {p['target_campaign_name']} "
                f"proposed budget equals current (${current:.2f}) — no change."
            )
            continue

        validated.append(p)

    return validated


def recommend_and_propose(to_stdout: bool = False) -> int:
    """Generate budget recommendations and create structured proposals.

    Returns:
        Number of proposals created.
    """
    from optimization.actions import create_proposal

    log.info("Generating structured budget proposals")

    budget_rules = _load_budget_rules()
    autonomy_cfg = _load_autonomy_config()
    max_proposals = autonomy_cfg.get("max_proposals_per_run", 10)

    # Gather data — reuse existing queries + add campaign IDs and margins
    channel_data = _query_channel_performance()
    campaign_data = _query_campaign_performance_with_ids()
    dow_data = _query_day_of_week_patterns()
    product_data = _query_product_profitability()

    data_context = (
        f"### Channel Performance (Last 30 Days)\n{channel_data}\n\n"
        f"### Campaign Performance with IDs (Last 30 Days)\n{campaign_data}\n\n"
        f"### Day-of-Week Patterns\n{dow_data}\n\n"
        f"### Product Profitability (Last 30 Days)\n{product_data}\n\n"
        f"### Budget Rules\n"
        f"- Max daily shift: {budget_rules.get('max_daily_shift_pct', 20)}%\n"
        f"- Min campaign daily spend: ${budget_rules.get('min_campaign_daily_spend', 10)}\n"
        f"- Min days before decision: {budget_rules.get('min_days_before_decision', 7)}\n"
    )

    question = (
        f"Analyze campaign performance and product margins, then return a JSON "
        f"array of budget shift proposals. Prioritize shifting budget toward "
        f"campaigns that drive high-margin products with strong ROAS.\n"
        f"Only include Google Ads campaigns (we don't have API budget control "
        f"for Meta yet).\n"
        f"Use campaign_id values from the data — never invent IDs.\n"
        f"Return at most {max_proposals} proposals.\n"
        f"Monthly budget is ~$2,000 total. Today is {date.today()}."
    )

    response = analyze(PROPOSE_SYSTEM_PROMPT, data_context, question,
                       model=_SONNET_MODEL)

    proposals = _parse_budget_proposals(response)
    if not proposals:
        log.warning("No valid budget proposals parsed from Claude's response")
        if to_stdout:
            print("No actionable budget proposals generated.")
            print(f"\nRaw response:\n{response[:1000]}")
        return 0

    # Apply guardrails
    proposals = _apply_guardrails(proposals, budget_rules)

    # Cap at max_proposals
    proposals = proposals[:max_proposals]

    created = 0
    for p in proposals:
        try:
            proposal = create_proposal(
                action_type="shift_budget",
                platform="google_ads",
                entity_id=str(p["target_campaign_id"]),
                entity_name=p["target_campaign_name"],
                current_value=f"${p['current_daily_budget']:.2f}/day",
                proposed_value=f"${p['proposed_daily_budget']:.2f}/day",
                rationale=(
                    f"Shift from {p['source_campaign_name']} "
                    f"({p['source_campaign_id']}): {p['rationale']}"
                ),
                expected_impact=(
                    f"Budget: ${p['current_daily_budget']:.2f} → "
                    f"${p['proposed_daily_budget']:.2f}/day"
                ),
                risk_level=p.get("risk_level", "medium"),
            )
            if proposal is not None:
                created += 1
        except Exception as e:
            log.warning(f"Failed to create budget proposal for "
                        f"'{p['target_campaign_name']}': {e}")

    log.info(f"Created {created} budget shift proposals")

    if to_stdout:
        print(f"\n{'='*60}")
        print(f"Budget Shift Proposals Created: {created}")
        print(f"{'='*60}")
        for p in proposals[:created]:
            print(f"\n  [shift_budget] {p['source_campaign_name']} → "
                  f"{p['target_campaign_name']}")
            print(f"    Budget: ${p['current_daily_budget']:.2f} → "
                  f"${p['proposed_daily_budget']:.2f}/day")
            print(f"    Risk: {p['risk_level']}")
            print(f"    Rationale: {p['rationale']}")

    return created
