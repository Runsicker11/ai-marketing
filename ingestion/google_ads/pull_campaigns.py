"""Pull Google Ads campaigns and ad groups."""

from datetime import datetime, timezone

from ingestion.google_ads.auth import get_service
from ingestion.utils.config import GOOGLE_ADS_CUSTOMER_ID
from ingestion.utils.logger import get_logger

log = get_logger(__name__)


def pull_campaigns() -> list[dict]:
    """Fetch all non-removed campaigns."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    service = get_service()

    query = """
        SELECT
            campaign.id,
            campaign.name,
            campaign.advertising_channel_type,
            campaign.bidding_strategy_type,
            campaign.status,
            campaign.campaign_budget
        FROM campaign
        WHERE campaign.status != 'REMOVED'
    """

    rows = []
    response = service.search_stream(
        customer_id=GOOGLE_ADS_CUSTOMER_ID, query=query
    )

    # Need budget service to resolve budget amounts
    budget_service = get_service("CampaignBudgetService")
    budget_cache = {}

    for batch in response:
        for row in batch.results:
            # Resolve budget amount if we have a budget resource name
            budget_amount = None
            budget_rn = row.campaign.campaign_budget
            if budget_rn and budget_rn not in budget_cache:
                try:
                    budget_query = f"""
                        SELECT
                            campaign_budget.amount_micros
                        FROM campaign_budget
                        WHERE campaign_budget.resource_name = '{budget_rn}'
                    """
                    budget_resp = get_service().search_stream(
                        customer_id=GOOGLE_ADS_CUSTOMER_ID, query=budget_query
                    )
                    for b_batch in budget_resp:
                        for b_row in b_batch.results:
                            budget_cache[budget_rn] = b_row.campaign_budget.amount_micros / 1_000_000
                except Exception:
                    budget_cache[budget_rn] = None
            budget_amount = budget_cache.get(budget_rn)

            rows.append({
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "campaign_type": row.campaign.advertising_channel_type.name,
                "bidding_strategy_type": row.campaign.bidding_strategy_type.name,
                "status": row.campaign.status.name,
                "budget_amount": budget_amount,
                "ingested_at": now_str,
            })

    log.info(f"Fetched {len(rows)} campaigns")
    return rows


def pull_ad_groups() -> list[dict]:
    """Fetch all non-removed ad groups with campaign context."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    service = get_service()

    query = """
        SELECT
            ad_group.id,
            ad_group.name,
            ad_group.campaign,
            ad_group.type,
            ad_group.status,
            ad_group.cpc_bid_micros,
            campaign.id,
            campaign.name
        FROM ad_group
        WHERE ad_group.status != 'REMOVED'
    """

    rows = []
    response = service.search_stream(
        customer_id=GOOGLE_ADS_CUSTOMER_ID, query=query
    )
    for batch in response:
        for row in batch.results:
            cpc_bid = None
            if row.ad_group.cpc_bid_micros:
                cpc_bid = row.ad_group.cpc_bid_micros / 1_000_000

            rows.append({
                "ad_group_id": row.ad_group.id,
                "ad_group_name": row.ad_group.name,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "ad_group_type": row.ad_group.type_.name,
                "status": row.ad_group.status.name,
                "cpc_bid_micros": cpc_bid,
                "ingested_at": now_str,
            })

    log.info(f"Fetched {len(rows)} ad groups")
    return rows
