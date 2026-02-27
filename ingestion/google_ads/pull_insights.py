"""Pull Google Ads daily performance insights at ad group level."""

from datetime import date, datetime, timezone

from ingestion.google_ads.auth import get_service
from ingestion.utils.config import GOOGLE_ADS_CUSTOMER_ID
from ingestion.utils.logger import get_logger

log = get_logger(__name__)


def pull_insights(start_date: date, end_date: date) -> list[dict]:
    """Fetch daily ad-group-level insights for a date range.

    Key gotchas:
    - cost_micros, average_cpc, cost_per_conversion are ALL in micros (divide by 1M)
    - metrics.ctr is already a decimal fraction (0.025), NOT a percentage
    - metrics.conversions is FLOAT64 (data-driven attribution gives fractional values)
    - search_impression_share is NULL for non-search campaigns (expected)
    - segments.date returns YYYY-MM-DD string directly
    """
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    service = get_service()

    query = f"""
        SELECT
            segments.date,
            campaign.id,
            campaign.name,
            campaign.advertising_channel_type,
            ad_group.id,
            ad_group.name,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.average_cpc,
            metrics.ctr,
            metrics.conversions,
            metrics.conversions_value,
            metrics.cost_per_conversion,
            metrics.search_impression_share
        FROM ad_group
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            AND metrics.impressions > 0
    """

    rows = []
    response = service.search_stream(
        customer_id=GOOGLE_ADS_CUSTOMER_ID, query=query
    )
    for batch in response:
        for row in batch.results:
            # cost_per_conversion can be 0 when no conversions
            cost_per_conv = None
            if row.metrics.cost_per_conversion:
                cost_per_conv = row.metrics.cost_per_conversion / 1_000_000

            # search_impression_share is 0.0 for non-search — treat as NULL
            search_is = None
            if row.metrics.search_impression_share:
                search_is = row.metrics.search_impression_share

            rows.append({
                "date_start": row.segments.date,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "campaign_type": row.campaign.advertising_channel_type.name,
                "ad_group_id": row.ad_group.id,
                "ad_group_name": row.ad_group.name,
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "spend": row.metrics.cost_micros / 1_000_000,
                "cpc": row.metrics.average_cpc / 1_000_000,
                "ctr": row.metrics.ctr,
                "conversions": row.metrics.conversions,
                "conversion_value": row.metrics.conversions_value,
                "cost_per_conversion": cost_per_conv,
                "search_impression_share": search_is,
                "ingested_at": now_str,
            })

    log.info(f"Fetched {len(rows)} daily insight rows ({start_date} to {end_date})")
    return rows
