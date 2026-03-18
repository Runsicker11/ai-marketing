"""Slack webhook notifications for ai-marketing pipeline.

Usage:
    from ingestion.utils.slack import send_slack
    send_slack("Alert: ROAS dropped below floor!")
"""

import json

import requests

from ingestion.utils.config import SLACK_WEBHOOK_URL
from ingestion.utils.logger import get_logger

log = get_logger(__name__)


def send_slack(
    message: str,
    blocks: list[dict] | None = None,
) -> bool:
    """Post a message to Slack via incoming webhook.

    Never raises -- wraps all errors so it can't break the pipeline.
    Returns True on success, False on failure.
    """
    if not SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set -- skipping notification")
        return False

    payload: dict = {}
    if blocks:
        payload["blocks"] = blocks
        payload["text"] = message  # fallback for notifications
    else:
        payload["text"] = message

    try:
        resp = requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code != 200:
            log.warning(f"Slack webhook returned {resp.status_code}: {resp.text}")
            return False
        return True
    except Exception:
        log.exception("Failed to send Slack notification")
        return False


def format_alert_summary(alerts: list[dict], max_per_type: int = 3) -> str:
    """Format a list of alert dicts into a Slack-friendly summary.

    Deduplicates by (type, message) and caps each alert type at max_per_type
    to prevent spam. Each alert dict should have: type, severity, message.
    """
    if not alerts:
        return ""

    severity_emoji = {
        "high": ":red_circle:",
        "medium": ":large_orange_circle:",
        "low": ":large_yellow_circle:",
    }

    # Deduplicate exact duplicates first
    seen = set()
    deduped = []
    for a in alerts:
        key = (a["type"], a["message"])
        if key not in seen:
            seen.add(key)
            deduped.append(a)

    # Cap each alert type so one noisy check can't flood the message
    type_counts: dict[str, int] = {}
    capped = []
    truncated_types: dict[str, int] = {}
    for a in deduped:
        t = a["type"]
        type_counts[t] = type_counts.get(t, 0) + 1
        if type_counts[t] <= max_per_type:
            capped.append(a)
        else:
            truncated_types[t] = truncated_types.get(t, 0) + 1

    total_raw = len(alerts)
    total_deduped = len(deduped)
    dedupe_note = (
        f" ({total_raw - total_deduped} duplicates removed)" if total_raw != total_deduped else ""
    )

    lines = [f":rotating_light: *{total_deduped} Alert(s) Triggered*{dedupe_note}", ""]
    for a in capped:
        emoji = severity_emoji.get(a.get("severity", "medium"), ":white_circle:")
        lines.append(f"{emoji} *{a['type']}*: {a['message']}")

    if truncated_types:
        lines.append("")
        for t, n in truncated_types.items():
            lines.append(f"_...and {n} more {t} alert(s) — see full report_")

    lines.append("")
    lines.append("Full report saved to `reports/` directory.")
    return "\n".join(lines)


def _extract_weekly_impact(proposal: dict) -> float:
    """Parse expected_impact to get weekly dollar value for sorting.

    Handles formats:
    - "Save ~$12/week" → 12.0
    - "Budget: $20.00 → $25.00/day" → 35.0 (daily delta * 7)
    - "+3 orders/week" → 120.0 (orders * $40 AOV estimate)
    """
    import re

    impact = proposal.get("expected_impact", "")

    # Pattern: "Save ~$X/week"
    if "save" in impact.lower() and "/week" in impact.lower():
        match = re.search(r'\$?(\d+\.?\d*)', impact)
        return float(match.group(1)) if match else 0.0

    # Pattern: "Budget: $X → $Y/day" (convert to weekly)
    if "budget:" in impact.lower() and "/day" in impact.lower():
        match = re.search(r'\$(\d+\.?\d*)\s*→\s*\$(\d+\.?\d*)', impact)
        if match:
            old, new = float(match.group(1)), float(match.group(2))
            return abs(new - old) * 7

    # Pattern: "+X orders/week" (use $40 AOV)
    if "order" in impact.lower() and "/week" in impact.lower():
        match = re.search(r'(\d+)', impact)
        return float(match.group(1)) * 40 if match else 0.0

    return 0.0  # fallback


def _format_proposal_item(proposal: dict) -> str:
    """Format a single proposal with action-specific details.

    Format: [outcome] → [dollar impact]
    • [Specific entity details]
    • [Campaign/context info]
    • Why: [Data-backed rationale]
    • ACTION: [Clear directive]
    """
    import re

    risk_emoji = {
        "low": ":large_green_circle:",
        "medium": ":large_orange_circle:",
        "high": ":red_circle:",
    }

    emoji = risk_emoji.get(proposal.get("risk_level", "medium"), ":white_circle:")
    action_type = proposal.get("action_type", "unknown")
    entity_name = proposal.get("entity_name", "")
    rationale = proposal.get("rationale", "")
    impact = proposal.get("expected_impact", "")
    proposed = proposal.get("proposed_value", "")
    current = proposal.get("current_value", "")

    if action_type == "add_negative_keyword":
        search_term = proposed
        campaign = entity_name

        # Parse match type from current_value if present
        match_type = "phrase"
        if "exact" in current.lower():
            match_type = "exact"

        return (
            f"{emoji} *Block \"{search_term}\" → {impact}*\n"
            f"• Search Term: \"{search_term}\" ({match_type} match)\n"
            f"• Campaign: {campaign}\n"
            f"• Why: {rationale}\n"
            f"• ACTION: Add negative keyword"
        )

    elif action_type == "add_as_keyword":
        # Parse "tungsten tape [exact]" format
        if "[" in proposed and "]" in proposed:
            search_term = proposed[:proposed.rfind("[")].strip()
            match_type = proposed[proposed.rfind("[")+1:proposed.rfind("]")].strip()
        else:
            search_term = proposed
            match_type = "broad"

        campaign = entity_name

        return (
            f"{emoji} *Add \"{search_term}\" as keyword → {impact}*\n"
            f"• Search Term: \"{search_term}\" ({match_type} match)\n"
            f"• Campaign: {campaign}\n"
            f"• Why: {rationale}\n"
            f"• ACTION: Add as {match_type}-match keyword"
        )

    elif action_type == "shift_budget":
        # Parse source campaign from rationale: "Shift from Brand Defense (12345): ..."
        source_match = re.search(r'Shift from ([^(]+)', rationale)
        source_campaign = source_match.group(1).strip() if source_match else "Unknown"

        target_campaign = entity_name

        # Clean rationale (remove "Shift from..." prefix)
        clean_rationale = re.sub(r'^Shift from [^:]+:\s*', '', rationale)

        # Extract dollar amounts for delta calculation
        try:
            old_amt = float(current.split("$")[1].split("/")[0]) if "$" in current else 0
            new_amt = float(proposed.split("$")[1].split("/")[0]) if "$" in proposed else 0
            delta = new_amt - old_amt

            # Note: The entity_name is always the TARGET of the shift (receiving budget)
            # If delta > 0, target is being boosted; if delta < 0, this indicates
            # the proposal format is inconsistent with the rationale.
            # We'll format based on the delta to be explicit about what's happening.
            if delta > 0:
                action_verb = "Boost"
                delta_str = f"+${abs(delta):.2f}/day"
                shift_description = f"{source_campaign} → {target_campaign}"
                action_text = f"Increase {target_campaign} budget from {current} to {proposed}, funded by reducing {source_campaign}"
            else:
                # If delta is negative, the "target" is actually being reduced
                # This means budget is shifting FROM target TO source
                action_verb = "Shift budget from"
                delta_str = f"${abs(delta):.2f}/day"
                shift_description = f"{target_campaign} → {source_campaign}"
                action_text = f"Reduce {target_campaign} from {current} to {proposed}, reallocate to {source_campaign}"

            title = f"{action_verb} {shift_description} → {delta_str}"
        except (IndexError, ValueError):
            title = f"Adjust budget between {source_campaign} and {target_campaign}"
            shift_description = f"{source_campaign} ↔ {target_campaign}"
            action_text = f"Reallocate budget between {source_campaign} and {target_campaign}"

        return (
            f"{emoji} *{title}*\n"
            f"• Current: {target_campaign} at {current}\n"
            f"• Proposed: {target_campaign} at {proposed}\n"
            f"• Why: {clean_rationale}\n"
            f"• ACTION: {action_text}"
        )

    elif action_type == "pause_keyword":
        keyword = proposed
        campaign = entity_name

        return (
            f"{emoji} *Pause \"{keyword}\" → {impact}*\n"
            f"• Keyword: \"{keyword}\"\n"
            f"• Campaign: {campaign}\n"
            f"• Why: {rationale}\n"
            f"• ACTION: Pause keyword"
        )

    elif action_type == "adjust_bid":
        keyword = entity_name

        # Parse bid amounts
        try:
            old_bid = current.replace("$", "").strip()
            new_bid = proposed.replace("$", "").strip()
            bid_change = f"${old_bid} → ${new_bid}"
        except:
            bid_change = f"{current} → {proposed}"

        return (
            f"{emoji} *Adjust bid on \"{keyword}\" → {impact}*\n"
            f"• Keyword: \"{keyword}\"\n"
            f"• Bid Change: {bid_change}\n"
            f"• Why: {rationale}\n"
            f"• ACTION: Update CPC bid"
        )

    else:
        # Fallback for unknown action types
        action = action_type.replace("_", " ").title()
        return (
            f"{emoji} *{action}: {entity_name} → {impact}*\n"
            f"• Details: {proposed}\n"
            f"• Why: {rationale}\n"
            f"• ACTION: {action}"
        )


def format_proposal_summary(proposals: list[dict]) -> str:
    """Format optimization proposals for Slack with top 5 prioritized by impact.

    Each proposal dict should have: action_type, entity_name, rationale, risk_level,
    expected_impact, proposed_value, current_value.
    """
    if not proposals:
        return ""

    # Sort by weekly dollar impact and take top 5
    sorted_proposals = sorted(proposals,
                              key=_extract_weekly_impact,
                              reverse=True)[:5]

    count = min(len(proposals), 5)
    show_top_note = " (showing top 5 by impact)" if len(proposals) > 5 else ""

    lines = [
        f":clipboard: *{count} New Optimization Proposal(s) Ready*{show_top_note}",
        "",
        "━" * 60,
        "",
    ]

    for p in sorted_proposals:
        lines.append(_format_proposal_item(p))
        lines.append("")  # spacing

    lines.append("━" * 60)
    lines.append("")
    lines.append("Review full details:")
    lines.append("```")
    lines.append("uv run python -m optimization.run --list-proposals")
    lines.append("```")
    lines.append("")
    lines.append("Execute approved actions:")
    lines.append("```")
    lines.append("uv run python -m optimization.run --execute")
    lines.append("```")

    return "\n".join(lines)
