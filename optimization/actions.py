"""Autonomous optimization actions with human approval gates.

Defines action types, creates proposals, and executes approved actions
via Google Ads API.
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from ingestion.utils.bq_client import load_rows, run_query
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.logger import get_logger
from ingestion import schemas

log = get_logger(__name__)

_DS = f"{GCP_PROJECT_ID}.{BQ_DATASET}"
_PROPOSALS_DIR = Path(__file__).resolve().parent / "proposals"

# Supported action types
ACTION_TYPES = {
    "add_negative_keyword": {
        "description": "Add a negative keyword to a campaign or ad group",
        "requires_approval": True,
    },
    "adjust_bid": {
        "description": "Adjust keyword or ad group bid",
        "requires_approval": True,
    },
    "pause_keyword": {
        "description": "Pause a keyword",
        "requires_approval": True,
    },
    "shift_budget": {
        "description": "Shift daily budget between campaigns",
        "requires_approval": True,
    },
}


def create_proposal(
    action_type: str,
    platform: str,
    entity_id: str,
    entity_name: str,
    current_value: str,
    proposed_value: str,
    rationale: str,
    expected_impact: str,
    risk_level: str = "medium",
) -> dict:
    """Create an optimization proposal and save for review.

    Args:
        action_type: One of ACTION_TYPES keys.
        platform: 'google_ads' or 'meta'.
        entity_id: ID of the entity to modify.
        entity_name: Human-readable name.
        current_value: Current setting/value.
        proposed_value: Proposed new setting/value.
        rationale: Data-backed reason for the change.
        expected_impact: Expected outcome.
        risk_level: 'low', 'medium', or 'high'.

    Returns:
        The proposal dict with action_id.
    """
    if action_type not in ACTION_TYPES:
        raise ValueError(f"Unknown action type: {action_type}. "
                         f"Valid types: {list(ACTION_TYPES.keys())}")

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    action_id = f"act_{uuid.uuid4().hex[:12]}"

    proposal = {
        "action_id": action_id,
        "action_type": action_type,
        "platform": platform,
        "entity_id": entity_id,
        "entity_name": entity_name,
        "current_value": current_value,
        "proposed_value": proposed_value,
        "rationale": rationale,
        "expected_impact": expected_impact,
        "risk_level": risk_level,
        "status": "proposed",
        "proposed_at": now_str,
        "decided_at": None,
        "executed_at": None,
    }

    # Save to proposals directory
    _PROPOSALS_DIR.mkdir(parents=True, exist_ok=True)
    proposal_path = _PROPOSALS_DIR / f"{action_id}.json"
    proposal_path.write_text(
        json.dumps(proposal, indent=2, default=str),
        encoding="utf-8",
    )

    # Save to BigQuery
    load_rows("optimization_actions", [proposal], schemas.OPTIMIZATION_ACTIONS)

    log.info(f"Proposal created: {action_id} ({action_type} on {entity_name})")
    return proposal


def list_pending_proposals() -> list[dict]:
    """List all proposals awaiting approval."""
    sql = f"""
    SELECT action_id, action_type, platform, entity_name,
           current_value, proposed_value, rationale, risk_level, proposed_at
    FROM `{_DS}.optimization_actions`
    WHERE status = 'proposed'
    ORDER BY proposed_at DESC
    """
    rows = list(run_query(sql))
    return [dict(r) for r in rows]


def approve_proposal(action_id: str) -> dict:
    """Mark a proposal as approved."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""
    UPDATE `{_DS}.optimization_actions`
    SET status = 'approved', decided_at = '{now_str}'
    WHERE action_id = '{action_id}' AND status = 'proposed'
    """
    run_query(sql)
    log.info(f"Proposal approved: {action_id}")

    # Update local file if it exists
    proposal_path = _PROPOSALS_DIR / f"{action_id}.json"
    if proposal_path.exists():
        proposal = json.loads(proposal_path.read_text(encoding="utf-8"))
        proposal["status"] = "approved"
        proposal["decided_at"] = now_str
        proposal_path.write_text(
            json.dumps(proposal, indent=2, default=str),
            encoding="utf-8",
        )

    return {"action_id": action_id, "status": "approved"}


def reject_proposal(action_id: str) -> dict:
    """Mark a proposal as rejected."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""
    UPDATE `{_DS}.optimization_actions`
    SET status = 'rejected', decided_at = '{now_str}'
    WHERE action_id = '{action_id}' AND status = 'proposed'
    """
    run_query(sql)
    log.info(f"Proposal rejected: {action_id}")
    return {"action_id": action_id, "status": "rejected"}


def execute_approved() -> list[dict]:
    """Execute all approved proposals via platform APIs.

    Returns:
        List of execution results.
    """
    sql = f"""
    SELECT action_id, action_type, platform, entity_id, entity_name,
           proposed_value
    FROM `{_DS}.optimization_actions`
    WHERE status = 'approved'
    ORDER BY proposed_at
    """
    rows = list(run_query(sql))
    if not rows:
        log.info("No approved proposals to execute")
        return []

    results = []
    for row in rows:
        r = dict(row)
        action_id = r["action_id"]
        action_type = r["action_type"]
        platform = r["platform"]

        try:
            if platform == "google_ads":
                _execute_google_ads_action(r)
            else:
                log.warning(f"Execution for platform '{platform}' not yet implemented")
                continue

            # Mark as executed
            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            update_sql = f"""
            UPDATE `{_DS}.optimization_actions`
            SET status = 'executed', executed_at = '{now_str}'
            WHERE action_id = '{action_id}'
            """
            run_query(update_sql)
            log.info(f"Executed: {action_id} ({action_type})")
            results.append({"action_id": action_id, "status": "executed"})

        except Exception as e:
            log.error(f"Failed to execute {action_id}: {e}")
            results.append({"action_id": action_id, "status": "failed", "error": str(e)})

    return results


def _execute_google_ads_action(action: dict):
    """Execute a Google Ads action via the API."""
    from ingestion.google_ads.auth import get_client, get_service
    from ingestion.utils.config import GOOGLE_ADS_CUSTOMER_ID

    action_type = action["action_type"]
    entity_id = action["entity_id"]
    proposed_value = action["proposed_value"]

    client = get_client()

    if action_type == "add_negative_keyword":
        # Add campaign-level negative keyword
        campaign_criterion_service = client.get_service("CampaignCriterionService")
        campaign_criterion_operation = client.get_type("CampaignCriterionOperation")
        criterion = campaign_criterion_operation.create

        criterion.campaign = client.get_service("GoogleAdsService").campaign_path(
            GOOGLE_ADS_CUSTOMER_ID, entity_id
        )
        criterion.negative = True
        criterion.keyword.text = proposed_value
        criterion.keyword.match_type = client.enums.KeywordMatchTypeEnum.PHRASE

        campaign_criterion_service.mutate_campaign_criteria(
            customer_id=GOOGLE_ADS_CUSTOMER_ID,
            operations=[campaign_criterion_operation],
        )
        log.info(f"Added negative keyword '{proposed_value}' to campaign {entity_id}")

    elif action_type == "pause_keyword":
        ad_group_criterion_service = client.get_service("AdGroupCriterionService")
        operation = client.get_type("AdGroupCriterionOperation")
        criterion = operation.update

        criterion.resource_name = client.get_service(
            "GoogleAdsService"
        ).ad_group_criterion_path(
            GOOGLE_ADS_CUSTOMER_ID, entity_id.split("_")[0], entity_id.split("_")[1]
        )
        criterion.status = client.enums.AdGroupCriterionStatusEnum.PAUSED

        field_mask = client.get_type("FieldMask")
        field_mask.paths.append("status")
        operation.update_mask.CopyFrom(field_mask)

        ad_group_criterion_service.mutate_ad_group_criteria(
            customer_id=GOOGLE_ADS_CUSTOMER_ID,
            operations=[operation],
        )
        log.info(f"Paused keyword {entity_id}")

    else:
        raise NotImplementedError(
            f"Google Ads execution for '{action_type}' not yet implemented"
        )
