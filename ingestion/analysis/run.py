"""Orchestrator for analysis tasks (daily report, alerts, weekly strategy)."""

import argparse
import sys

# Windows console doesn't support UTF-8 by default
sys.stdout.reconfigure(encoding="utf-8")

from ingestion.utils.logger import get_logger

log = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Run AI marketing analysis")
    parser.add_argument("--daily", action="store_true",
                        help="Generate daily performance report")
    parser.add_argument("--alerts", action="store_true",
                        help="Run alert threshold checks")
    parser.add_argument("--weekly", action="store_true",
                        help="Generate weekly strategy report")
    parser.add_argument("--all", action="store_true",
                        help="Run daily + alerts + weekly")
    parser.add_argument("--print", action="store_true", dest="to_stdout",
                        help="Print reports to stdout instead of saving files")
    args = parser.parse_args()

    if not any([args.daily, args.alerts, args.weekly, args.all]):
        parser.print_help()
        sys.exit(1)

    run_daily = args.daily or args.all
    run_alerts = args.alerts or args.all
    run_weekly = args.weekly or args.all

    try:
        if run_daily:
            log.info("=== Daily Report ===")
            from ingestion.analysis.daily_report import generate as gen_daily
            gen_daily(to_stdout=args.to_stdout)

        if run_alerts:
            log.info("=== Alert Checks ===")
            from ingestion.analysis.alerts import check as run_alert_checks
            run_alert_checks(to_stdout=args.to_stdout)

        if run_weekly:
            log.info("=== Weekly Strategy ===")
            from ingestion.analysis.weekly_strategy import generate as gen_weekly
            gen_weekly(to_stdout=args.to_stdout)

        log.info("=== Analysis complete ===")

    except Exception:
        log.exception("Analysis failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
