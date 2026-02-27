"""Google Ads ingestion orchestrator: validate -> pull -> BigQuery load."""

import argparse
from datetime import date, timedelta

from ingestion.google_ads.auth import validate_access
from ingestion.google_ads.pull_campaigns import pull_campaigns, pull_ad_groups
from ingestion.google_ads.pull_keywords import pull_keywords
from ingestion.google_ads.pull_insights import pull_insights
from ingestion.google_ads.pull_search_terms import pull_search_terms
from ingestion.utils.bq_client import load_rows, full_replace, delete_date_range
from ingestion.utils.logger import get_logger
from ingestion import schemas

log = get_logger(__name__)


def run(days_back: int = 3):
    log.info(f"Starting Google Ads ingestion (days_back={days_back})")

    # 1. Validate credentials
    validate_access()

    # 2. Pull and load reference data (full replace)
    campaigns = pull_campaigns()
    full_replace("google_ads_campaigns", campaigns, schemas.GOOGLE_ADS_CAMPAIGNS)

    ad_groups = pull_ad_groups()
    full_replace("google_ads_ad_groups", ad_groups, schemas.GOOGLE_ADS_AD_GROUPS)

    keywords = pull_keywords()
    full_replace("google_ads_keywords", keywords, schemas.GOOGLE_ADS_KEYWORDS)

    # 3. Pull and load time-series data (incremental)
    end_date = date.today() - timedelta(days=1)  # yesterday (today's data incomplete)
    start_date = end_date - timedelta(days=days_back - 1)

    insights = pull_insights(start_date, end_date)
    if insights:
        delete_date_range("google_ads_daily_insights", "date_start", start_date, end_date)
        load_rows("google_ads_daily_insights", insights, schemas.GOOGLE_ADS_DAILY_INSIGHTS)

    search_terms = pull_search_terms(start_date, end_date)
    if search_terms:
        delete_date_range("google_ads_search_terms", "date_start", start_date, end_date)
        load_rows("google_ads_search_terms", search_terms, schemas.GOOGLE_ADS_SEARCH_TERMS)

    log.info("Google Ads ingestion complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Google Ads data ingestion")
    parser.add_argument("--days-back", type=int, default=3, help="Days of insights to pull")
    args = parser.parse_args()
    run(days_back=args.days_back)
