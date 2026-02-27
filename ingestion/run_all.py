"""Master ingestion script: Shopify + Meta + Views."""

import argparse
import sys

from ingestion.utils.logger import get_logger

log = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Run full marketing data ingestion")
    parser.add_argument("--days-back", type=int, default=3,
                        help="Days of history to pull (default: 3)")
    parser.add_argument("--setup", action="store_true",
                        help="Create/verify BigQuery tables before ingestion")
    parser.add_argument("--shopify-only", action="store_true",
                        help="Only run Shopify ingestion")
    parser.add_argument("--meta-only", action="store_true",
                        help="Only run Meta Ads ingestion")
    parser.add_argument("--google-only", action="store_true",
                        help="Only run Google Ads ingestion")
    parser.add_argument("--search-console-only", action="store_true",
                        help="Only run Search Console ingestion")
    parser.add_argument("--views-only", action="store_true",
                        help="Only deploy views")
    parser.add_argument("--analyze", action="store_true",
                        help="Run AI analysis after ingestion (daily report + alerts)")
    parser.add_argument("--analyze-only", action="store_true",
                        help="Only run AI analysis (skip ingestion)")
    args = parser.parse_args()

    run_all = not (args.shopify_only or args.meta_only or args.google_only
                   or args.search_console_only or args.views_only
                   or args.analyze_only)

    try:
        # Setup tables if requested
        if args.setup:
            log.info("=== Setting up BigQuery tables ===")
            from ingestion.setup_bigquery import setup
            setup()

        # Shopify
        if run_all or args.shopify_only:
            log.info("=== Shopify Ingestion ===")
            from ingestion.shopify.run import run as run_shopify
            run_shopify(days_back=args.days_back)

        # Meta Ads
        if run_all or args.meta_only:
            log.info("=== Meta Ads Ingestion ===")
            from ingestion.meta.run import run as run_meta
            run_meta(days_back=args.days_back)

        # Google Ads
        if run_all or args.google_only:
            log.info("=== Google Ads Ingestion ===")
            from ingestion.google_ads.run import run as run_google
            run_google(days_back=args.days_back)

        # Search Console
        if run_all or args.search_console_only:
            log.info("=== Search Console Ingestion ===")
            from ingestion.search_console.run import run as run_search_console
            run_search_console(days_back=args.days_back)

        # Deploy views
        if run_all or args.views_only:
            log.info("=== Deploying Views ===")
            from ingestion.views.deploy_views import deploy
            deploy()

        # AI Analysis
        if args.analyze or args.analyze_only:
            log.info("=== AI Analysis ===")
            from ingestion.analysis.daily_report import generate as gen_daily
            from ingestion.analysis.alerts import check as run_alerts
            gen_daily()
            run_alerts()

        log.info("=== All ingestion complete ===")

    except Exception:
        log.exception("Ingestion failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
