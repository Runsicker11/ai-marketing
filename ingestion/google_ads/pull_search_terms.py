"""Pull Google Ads search term reports.

Search terms are the actual queries users typed that triggered ads.
Critical for negative keyword optimization — in a niche like pickleball,
irrelevant terms can waste 20-30% of budget.
"""

from datetime import date, datetime, timezone

from ingestion.google_ads.auth import get_service
from ingestion.utils.config import GOOGLE_ADS_CUSTOMER_ID
from ingestion.utils.logger import get_logger

log = get_logger(__name__)


def pull_search_terms(start_date: date, end_date: date) -> list[dict]:
    """Fetch search terms that triggered ads for a date range."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    service = get_service()

    query = f"""
        SELECT
            segments.date,
            search_term_view.search_term,
            campaign.id,
            campaign.name,
            ad_group.id,
            ad_group.name,
            search_term_view.status,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM search_term_view
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            AND metrics.impressions > 0
    """

    rows = []
    response = service.search_stream(
        customer_id=GOOGLE_ADS_CUSTOMER_ID, query=query
    )
    for batch in response:
        for row in batch.results:
            rows.append({
                "date_start": row.segments.date,
                "search_term": row.search_term_view.search_term,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "ad_group_id": row.ad_group.id,
                "ad_group_name": row.ad_group.name,
                "keyword_text": "",
                "match_type": "",
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "spend": row.metrics.cost_micros / 1_000_000,
                "conversions": row.metrics.conversions,
                "conversion_value": row.metrics.conversions_value,
                "ingested_at": now_str,
            })

    log.info(f"Fetched {len(rows)} search term rows ({start_date} to {end_date})")
    return rows
