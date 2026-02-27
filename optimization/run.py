"""Optimization CLI: search term hygiene, budget intelligence, action management.

Usage:
    uv run python -m optimization.run --search-terms --print   # Review search terms
    uv run python -m optimization.run --budget --print         # Budget recommendations
    uv run python -m optimization.run --list-proposals          # List pending proposals
    uv run python -m optimization.run --execute                 # Execute approved proposals
    uv run python -m optimization.run --all --print             # Full analysis cycle
"""

import argparse
import sys

from ingestion.utils.logger import get_logger

log = get_logger(__name__)


def main():
    sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Optimization engine")
    parser.add_argument("--search-terms", action="store_true",
                        help="Review search terms for negative keyword recommendations")
    parser.add_argument("--budget", action="store_true",
                        help="Generate budget reallocation recommendations")
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
                args.execute, args.all]):
        parser.print_help()
        return

    try:
        if args.all or args.search_terms:
            log.info("--- Search Term Hygiene ---")
            from optimization.search_terms import review
            review(to_stdout=args.to_stdout)

        if args.all or args.budget:
            log.info("--- Budget Intelligence ---")
            from optimization.budget import recommend
            recommend(to_stdout=args.to_stdout)

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
