"""Search Console ingestion orchestrator: validate -> pull -> BigQuery load.

Supports multiple sites configured via env vars:
  GOOGLE_SEARCH_CONSOLE_SITE_URL       (review site)
  GOOGLE_SEARCH_CONSOLE_SITE_URL_SHOP  (shop site)
"""

import argparse
from datetime import date, timedelta

from ingestion.search_console.auth import validate_access, get_site_urls
from ingestion.search_console.pull_performance import pull_performance, _site_label
from ingestion.utils.bq_client import load_rows, delete_date_range
from ingestion.utils.logger import get_logger
from ingestion import schemas

log = get_logger(__name__)


def run(days_back: int = 7):
    """Pull Search Console data for all configured sites and load into BigQuery.

    Note: Search Console data has a 2-3 day lag, so we default to 7 days
    and end 3 days ago to avoid incomplete data.
    """
    site_urls = get_site_urls()
    if not site_urls:
        log.warning("No Search Console site URLs configured — skipping")
        return

    log.info(f"Starting Search Console ingestion (days_back={days_back}, "
             f"sites={len(site_urls)})")

    # 1. Validate credentials for all sites
    validate_access()

    # 2. Date range — SC data lags 2-3 days
    end_date = date.today() - timedelta(days=3)
    start_date = end_date - timedelta(days=days_back - 1)

    # 3. Pull + load for each site
    for site_url in site_urls:
        log.info(f"Pulling data for {site_url}")
        rows = pull_performance(start_date, end_date, site_url)

        if rows:
            site_label = _site_label(site_url)
            delete_date_range("search_console_performance", "query_date",
                              start_date, end_date,
                              extra_conditions={"site": site_label})
            load_rows("search_console_performance", rows,
                       schemas.SEARCH_CONSOLE_PERFORMANCE)

    log.info("Search Console ingestion complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search Console data ingestion")
    parser.add_argument("--days-back", type=int, default=7,
                        help="Days of data to pull (default: 7)")
    args = parser.parse_args()
    run(days_back=args.days_back)
