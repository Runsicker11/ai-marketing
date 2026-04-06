"""SEO content engine CLI: opportunities, generate, publish, score.

Usage:
    uv run python -m seo.run --opportunities --print     # Identify what to write
    uv run python -m seo.run --generate --type review --keyword "tungsten tape"
    uv run python -m seo.run --generate --type landing_page --keyword "paddle tape" --product tungsten-tape --site shop
    uv run python -m seo.run --publish-drafts             # Push drafts to WordPress/Shopify
    uv run python -m seo.run --sync-inventory             # Pull WordPress + Shopify blog inventory
    uv run python -m seo.run --score --print              # Score published content
    uv run python -m seo.run --all                        # Full cycle
"""

import argparse
import sys

from ingestion.utils.logger import get_logger

log = get_logger(__name__)


def main():
    sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="SEO content engine")
    parser.add_argument("--opportunities", action="store_true",
                        help="Identify content opportunities from SEO data")
    parser.add_argument("--generate", action="store_true",
                        help="Generate a content draft")
    parser.add_argument("--publish-drafts", action="store_true",
                        help="Push pending drafts to WordPress/Shopify")
    parser.add_argument("--sync-inventory", action="store_true",
                        help="Sync WordPress + Shopify blog content inventory to BigQuery")
    parser.add_argument("--score", action="store_true",
                        help="Score published content performance")
    parser.add_argument("--all", action="store_true",
                        help="Run full cycle: opportunities -> generate -> score")
    parser.add_argument("--type", type=str, default="review",
                        choices=["review", "comparison", "how_to", "landing_page", "educational"],
                        help="Content type for generation (default: review)")
    parser.add_argument("--keyword", type=str, default=None,
                        help="Target keyword for content generation")
    parser.add_argument("--product", type=str, default=None,
                        help="Product focus for content generation")
    parser.add_argument("--site", type=str, default="blog",
                        choices=["blog", "shop"],
                        help="Target site: blog (WordPress) or shop (Shopify) (default: blog)")
    parser.add_argument("--print", dest="to_stdout", action="store_true",
                        help="Print output instead of saving to file")
    args = parser.parse_args()

    if not any([args.opportunities, args.generate, args.publish_drafts,
                args.sync_inventory, args.score, args.all]):
        parser.print_help()
        return

    try:
        if args.all or args.opportunities:
            log.info("--- Identifying content opportunities ---")
            from seo.opportunities import identify
            identify(site=args.site, to_stdout=args.to_stdout)

        if args.all or args.generate:
            if not args.keyword and not args.all:
                log.error("--keyword is required for --generate")
                sys.exit(1)
            if args.keyword:
                log.info(f"--- Generating {args.type} for '{args.keyword}' (site={args.site}) ---")
                from seo.generate import generate_article
                generate_article(
                    target_keyword=args.keyword,
                    content_type=args.type,
                    product=args.product,
                    site=args.site,
                    to_stdout=args.to_stdout,
                )

        if args.sync_inventory:
            log.info("--- Syncing WordPress inventory ---")
            from seo.wordpress.inventory import sync_inventory
            sync_inventory()

            log.info("--- Syncing Shopify blog inventory ---")
            from seo.shopify.inventory import sync_inventory as sync_shopify_inventory
            sync_shopify_inventory()

        if args.publish_drafts:
            log.info("--- Publishing drafts ---")
            _publish_pending_drafts()

        if args.all or args.score:
            log.info("--- Scoring published content ---")
            from seo.scorer import run_scoring
            run_scoring(to_stdout=args.to_stdout)

        log.info("SEO content engine complete")

    except Exception:
        log.exception("SEO content engine failed")
        sys.exit(1)


def _publish_pending_drafts():
    """Find draft files in seo/drafts/ and push to WordPress or Shopify."""
    from pathlib import Path
    import re

    import markdown

    drafts_dir = Path(__file__).resolve().parent / "drafts"
    if not drafts_dir.exists():
        log.info("No drafts directory found")
        return

    draft_files = list(drafts_dir.glob("*.md"))
    if not draft_files:
        log.info("No draft files found in seo/drafts/")
        return

    for draft_path in draft_files:
        log.info(f"Processing draft: {draft_path.name}")
        text = draft_path.read_text(encoding="utf-8")

        # Parse frontmatter
        title = ""
        meta_description = ""
        target_keyword = ""
        content_type = ""
        slug = ""
        site = "blog"

        fm_match = re.match(r"^---\s*\n(.*?)\n---\s*\n", text, re.DOTALL)
        if fm_match:
            for line in fm_match.group(1).split("\n"):
                if line.startswith("title:"):
                    title = line.split(":", 1)[1].strip().strip('"').strip("'")
                elif line.startswith("meta_description:"):
                    meta_description = line.split(":", 1)[1].strip().strip('"').strip("'")
                elif line.startswith("target_keyword:"):
                    target_keyword = line.split(":", 1)[1].strip().strip('"').strip("'")
                elif line.startswith("content_type:"):
                    content_type = line.split(":", 1)[1].strip()
                elif line.startswith("slug:"):
                    slug = line.split(":", 1)[1].strip().strip('"').strip("'")
                elif line.startswith("site:"):
                    site = line.split(":", 1)[1].strip()

        # Strip frontmatter and convert to HTML
        content_md = re.sub(r"^---\s*\n.*?\n---\s*\n", "", text, flags=re.DOTALL)
        content_html = markdown.markdown(content_md, extensions=["tables", "fenced_code"])

        word_count = len(content_md.split())

        # Route to the correct publisher based on site + content_type
        if site == "shop" and content_type == "landing_page":
            # Shopify Pages (existing)
            from seo.shopify.pages import create_landing_page
            create_landing_page(
                title=title,
                body_html=content_html,
                target_keyword=target_keyword,
                published=False,
            )
        elif site == "shop":
            # Shopify Blog Articles (new)
            from seo.shopify.articles import create_blog_article
            create_blog_article(
                title=title,
                body_html=content_html,
                tags=target_keyword,
                target_keyword=target_keyword,
                content_type=content_type,
                published=False,
            )
        else:
            # WordPress (default for site=blog)
            from seo.wordpress.publish import publish_draft
            publish_draft(
                title=title,
                content_html=content_html,
                target_keyword=target_keyword,
                content_type=content_type,
                slug=slug,
                meta_description=meta_description,
                word_count=word_count,
            )

        # Move processed draft
        processed_dir = drafts_dir / "published"
        processed_dir.mkdir(exist_ok=True)
        draft_path.rename(processed_dir / draft_path.name)
        log.info(f"Moved {draft_path.name} to published/")


if __name__ == "__main__":
    main()
