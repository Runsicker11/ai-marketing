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
    parser.add_argument("--dashboard", action="store_true",
                        help="Generate monthly HTML dashboard report")
    parser.add_argument("--month", type=str, default=None,
                        help="Target month YYYY-MM (default: last completed month)")
    parser.add_argument("--optimize", action="store_true",
                        help="Run optimization proposals (search terms + budget)")
    parser.add_argument("--gads-health", action="store_true", dest="gads_health",
                        help="Run Google Ads health check (30 rules, Slack summary)")
    parser.add_argument("--all", action="store_true",
                        help="Run daily + alerts + weekly + dashboard")
    parser.add_argument("--print", action="store_true", dest="to_stdout",
                        help="Print reports to stdout instead of saving files")
    args = parser.parse_args()

    if not any([args.daily, args.alerts, args.weekly, args.dashboard,
                args.optimize, args.gads_health, args.all]):
        parser.print_help()
        sys.exit(1)

    run_daily = args.daily or args.all
    run_alerts = args.alerts or args.all
    run_weekly = args.weekly or args.all
    run_dashboard = args.dashboard or args.all

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

        if run_dashboard:
            log.info("=== Monthly Dashboard ===")
            from ingestion.analysis.dashboard import generate as gen_dashboard
            gen_dashboard(month=args.month, to_stdout=args.to_stdout)

        if args.gads_health:
            log.info("=== Google Ads Health Check ===")
            from ingestion.analysis.gads_health_check import run as run_gads_health
            run_gads_health(to_stdout=args.to_stdout)

        if args.optimize:
            log.info("=== Optimization Proposals ===")
            from optimization.search_terms import review_and_propose
            from optimization.budget import recommend_and_propose
            from optimization.actions import list_pending_proposals
            from ingestion.utils.slack import send_slack, format_proposal_summary

            st_count = review_and_propose(to_stdout=args.to_stdout)
            log.info(f"Created {st_count} search term proposals")

            budget_count = recommend_and_propose(to_stdout=args.to_stdout)
            log.info(f"Created {budget_count} budget proposals")

            # Send Slack summary of all pending proposals
            if st_count + budget_count > 0:
                proposals = list_pending_proposals()
                if proposals:
                    slack_msg = format_proposal_summary(proposals)
                    send_slack(slack_msg)

        log.info("=== Analysis complete ===")

    except Exception:
        log.exception("Analysis failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
