"""Pull Google Ads keywords with quality scores."""

from datetime import datetime, timezone

from ingestion.google_ads.auth import get_service
from ingestion.utils.config import GOOGLE_ADS_CUSTOMER_ID
from ingestion.utils.logger import get_logger

log = get_logger(__name__)


def pull_keywords() -> list[dict]:
    """Fetch keywords with quality scores from keyword_view.

    Quality score = 0 means insufficient data — stored as NULL.
    Performance Max campaigns have no keywords (expected).
    """
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    service = get_service()

    query = """
        SELECT
            ad_group_criterion.criterion_id,
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            ad_group_criterion.status,
            ad_group_criterion.quality_info.quality_score,
            ad_group_criterion.quality_info.creative_quality_score,
            ad_group_criterion.quality_info.post_click_quality_score,
            ad_group_criterion.quality_info.search_predicted_ctr,
            ad_group.id,
            campaign.id
        FROM keyword_view
        WHERE ad_group_criterion.status != 'REMOVED'
    """

    rows = []
    response = service.search_stream(
        customer_id=GOOGLE_ADS_CUSTOMER_ID, query=query
    )
    for batch in response:
        for row in batch.results:
            quality_score = row.ad_group_criterion.quality_info.quality_score
            # Quality score of 0 means not enough data
            if quality_score == 0:
                quality_score = None

            rows.append({
                "keyword_id": row.ad_group_criterion.criterion_id,
                "keyword_text": row.ad_group_criterion.keyword.text,
                "match_type": row.ad_group_criterion.keyword.match_type.name,
                "ad_group_id": row.ad_group.id,
                "campaign_id": row.campaign.id,
                "status": row.ad_group_criterion.status.name,
                "quality_score": quality_score,
                "expected_ctr": row.ad_group_criterion.quality_info.search_predicted_ctr.name,
                "ad_relevance": row.ad_group_criterion.quality_info.creative_quality_score.name,
                "landing_page_experience": row.ad_group_criterion.quality_info.post_click_quality_score.name,
                "ingested_at": now_str,
            })

    log.info(f"Fetched {len(rows)} keywords")
    return rows
