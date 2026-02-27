"""Search Console ingestion orchestrator: validate -> pull -> BigQuery load."""

import argparse
from datetime import date, timedelta

from ingestion.search_console.auth import validate_access
from ingestion.search_console.pull_performance import pull_performance
from ingestion.utils.bq_client import load_rows, delete_date_range
from ingestion.utils.logger import get_logger
from ingestion import schemas

log = get_logger(__name__)


def run(days_back: int = 7):
    """Pull Search Console data and load into BigQuery.

    Note: Search Console data has a 2-3 day lag, so we default to 7 days
    and end 3 days ago to avoid incomplete data.
    """
    log.info(f"Starting Search Console ingestion (days_back={days_back})")

    # 1. Validate credentials
    validate_access()

    # 2. Date range — SC data lags 2-3 days
    end_date = date.today() - timedelta(days=3)
    start_date = end_date - timedelta(days=days_back - 1)

    # 3. Pull performance data
    rows = pull_performance(start_date, end_date)

    # 4. Load into BigQuery (incremental)
    if rows:
        delete_date_range("search_console_performance", "query_date",
                          start_date, end_date)
        load_rows("search_console_performance", rows,
                   schemas.SEARCH_CONSOLE_PERFORMANCE)

    log.info("Search Console ingestion complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search Console data ingestion")
    parser.add_argument("--days-back", type=int, default=7,
                        help="Days of data to pull (default: 7)")
    args = parser.parse_args()
    run(days_back=args.days_back)
