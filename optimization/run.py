"""Optimization CLI: search term hygiene, budget intelligence, action management.

Usage:
    uv run python -m optimization.run --search-terms --print   # Review search terms
    uv run python -m optimization.run --budget --print         # Budget recommendations
    uv run python -m optimization.run --search-terms --propose # Create search term proposals
    uv run python -m optimization.run --budget --propose       # Create budget proposals
    uv run python -m optimization.run --list-proposals          # List pending proposals
    uv run python -m optimization.run --execute                 # Execute approved proposals
    uv run python -m optimization.run --shadow-report --print   # Shadow mode comparison
    uv run python -m optimization.run --all --print             # Full analysis cycle
"""

import argparse
import sys

from ingestion.utils.logger import get_logger

log = get_logger(__name__)


def _check_meta_token():
    """Check Meta token expiry and log warnings."""
    try:
        from ingestion.meta.auth import validate_token
        info = validate_token()
        days = info.get("days_remaining")
        if days is not None and days < 7:
            log.warning(
                f"Meta access token expires in {days} days! "
                "Refresh at https://developers.facebook.com/tools/explorer/"
            )
    except Exception as e:
        log.warning(f"Meta token check failed: {e}")


def main():
    sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Optimization engine")
    parser.add_argument("--search-terms", action="store_true",
                        help="Review search terms for negative keyword recommendations")
    parser.add_argument("--budget", action="store_true",
                        help="Generate budget reallocation recommendations")
    parser.add_argument("--propose", action="store_true",
                        help="Create structured proposals (use with --search-terms or --budget)")
    parser.add_argument("--shadow-report", action="store_true",
                        help="Generate shadow mode comparison report")
    parser.add_argument("--list-proposals", action="store_true",
                        help="List pending optimization proposals")
    parser.add_argument("--execute", action="store_true",
                        help="Execute approved optimization proposals")
    parser.add_argument("--all", action="store_true",
                        help="Run search terms + budget analysis")
    parser.add_argument("--print", dest="to_stdout", action="store_true",
                        help="Print output instead of saving to file")
    args = parser.parse_args()

    if not any([args.search_terms, args.budget, args.list_proposals,
                args.execute, args.all, args.shadow_report]):
        parser.print_help()
        return

    try:
        # Meta token check on propose runs
        if args.propose:
            _check_meta_token()

        all_new_proposals = []

        if args.all or args.search_terms:
            if args.propose:
                log.info("--- Search Term Proposals ---")
                from optimization.search_terms import review_and_propose
                count = review_and_propose(to_stdout=args.to_stdout)
                log.info(f"Created {count} search term proposals")
            else:
                log.info("--- Search Term Hygiene ---")
                from optimization.search_terms import review
                review(to_stdout=args.to_stdout)

        if args.all or args.budget:
            if args.propose:
                log.info("--- Budget Shift Proposals ---")
                from optimization.budget import recommend_and_propose
                count = recommend_and_propose(to_stdout=args.to_stdout)
                log.info(f"Created {count} budget proposals")
            else:
                log.info("--- Budget Intelligence ---")
                from optimization.budget import recommend
                recommend(to_stdout=args.to_stdout)

        # Send Slack summary of all new proposals after --propose
        if args.propose:
            from optimization.actions import list_pending_proposals
            all_new_proposals = list_pending_proposals()
            if all_new_proposals:
                from ingestion.utils.slack import send_slack, format_proposal_summary
                slack_msg = format_proposal_summary(all_new_proposals)
                send_slack(slack_msg)

        if args.shadow_report:
            log.info("--- Shadow Mode Report ---")
            from optimization.shadow_report import generate_shadow_report
            generate_shadow_report(to_stdout=args.to_stdout)

        if args.list_proposals:
            log.info("--- Pending Proposals ---")
            from optimization.actions import list_pending_proposals
            proposals = list_pending_proposals()
            if not proposals:
                print("No pending proposals.")
            else:
                print(f"\n{'='*60}")
                print(f"Pending Optimization Proposals ({len(proposals)})")
                print(f"{'='*60}")
                for p in proposals:
                    print(f"\n[{p['action_id']}] {p['action_type']} ({p['risk_level']})")
                    print(f"  Entity: {p['entity_name']}")
                    print(f"  Current: {p['current_value']}")
                    print(f"  Proposed: {p['proposed_value']}")
                    print(f"  Rationale: {p['rationale']}")

        if args.execute:
            log.info("--- Executing Approved Proposals ---")
            from optimization.actions import execute_approved
            results = execute_approved()
            for r in results:
                print(f"  {r['action_id']}: {r['status']}")

        log.info("Optimization engine complete")

    except Exception:
        log.exception("Optimization engine failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
