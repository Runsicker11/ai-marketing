"""Content machine CLI: audit, generate, and score ad copy.

Usage:
    uv run python -m content.run --audit --print       # Audit existing creatives
    uv run python -m content.run --generate --count 10  # Generate 10 Meta variations
    uv run python -m content.run --generate --platform google --count 10  # Google Ads RSA copy
    uv run python -m content.run --generate --platform both --count 10   # Both platforms
    uv run python -m content.run --generate --product tungsten-tape --count 5
    uv run python -m content.run --score --print        # Score after 7+ days
    uv run python -m content.run --all                  # Full cycle
"""

import argparse
import sys

from ingestion.utils.logger import get_logger

log = get_logger(__name__)


def main():
    sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Content machine for ad copy")
    parser.add_argument("--audit", action="store_true", help="Audit existing creatives and populate library")
    parser.add_argument("--generate", action="store_true", help="Generate new ad copy variations")
    parser.add_argument("--score", action="store_true", help="Score library components against live performance")
    parser.add_argument("--all", action="store_true", help="Run full cycle: audit -> generate -> score")
    parser.add_argument("--count", type=int, default=10, help="Number of variations to generate (default: 10)")
    parser.add_argument("--product", type=str, default=None, help="Product name for targeted generation")
    parser.add_argument("--platform", type=str, default="meta",
                        choices=["meta", "google", "both"],
                        help="Ad platform: meta (default), google, or both")
    parser.add_argument("--print", dest="to_stdout", action="store_true", help="Print output instead of saving to file")
    args = parser.parse_args()

    if not any([args.audit, args.generate, args.score, args.all]):
        parser.print_help()
        return

    if args.all or args.audit:
        from content.audit import run_audit
        log.info("--- Running content audit ---")
        run_audit(to_stdout=args.to_stdout)

    if args.all or args.generate:
        platforms = []
        if args.platform in ("meta", "both"):
            platforms.append("meta")
        if args.platform in ("google", "both"):
            platforms.append("google")

        for platform in platforms:
            from content.generator.generate import generate
            log.info(f"--- Running content generation ({platform}) ---")
            generate(count=args.count, product=args.product,
                     platform=platform, to_stdout=args.to_stdout)

    if args.all or args.score:
        from content.scorer.score import run_scoring
        log.info("--- Running content scoring ---")
        run_scoring(to_stdout=args.to_stdout)

    log.info("Content machine complete")


if __name__ == "__main__":
    main()
