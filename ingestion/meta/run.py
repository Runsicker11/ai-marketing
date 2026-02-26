"""Meta Ads ingestion orchestrator: validate -> pull -> BigQuery load."""

import argparse
from datetime import date, timedelta

from ingestion.meta.auth import validate_token
from ingestion.meta.pull_campaigns import pull_campaigns, pull_adsets, pull_ads
from ingestion.meta.pull_creatives import pull_creatives
from ingestion.meta.pull_insights import pull_insights
from ingestion.utils.bq_client import load_rows, full_replace, delete_date_range
from ingestion.utils.logger import get_logger
from ingestion import schemas

log = get_logger(__name__)


def run(days_back: int = 3):
    log.info(f"Starting Meta Ads ingestion (days_back={days_back})")

    # 1. Validate token
    validate_token()

    # 2. Pull and load campaign metadata (full replace)
    campaigns = pull_campaigns()
    full_replace("meta_campaigns", campaigns, schemas.META_CAMPAIGNS)

    adsets = pull_adsets()
    full_replace("meta_adsets", adsets, schemas.META_ADSETS)

    ads = pull_ads()
    full_replace("meta_ads", ads, schemas.META_ADS)

    # 2b. Pull and load creative text (full replace)
    creatives = pull_creatives(ads)
    full_replace("meta_creatives", creatives, schemas.META_CREATIVES)

    # 3. Pull and load daily insights (incremental)
    end_date = date.today() - timedelta(days=1)  # yesterday (today's data incomplete)
    start_date = end_date - timedelta(days=days_back - 1)

    insights = pull_insights(start_date, end_date)

    if insights:
        delete_date_range("meta_daily_insights", "date_start", start_date, end_date)
        load_rows("meta_daily_insights", insights, schemas.META_DAILY_INSIGHTS)

    log.info("Meta Ads ingestion complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Meta Ads data ingestion")
    parser.add_argument("--days-back", type=int, default=3, help="Days of insights to pull")
    args = parser.parse_args()
    run(days_back=args.days_back)
