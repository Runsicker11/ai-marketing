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
    parser.add_argument("--optimize-meta", action="store_true",
                        help="Generate Yoast title/description proposals for underperforming pages")
    parser.add_argument("--apply-meta-proposals", action="store_true",
                        help="Apply approved proposals from the most recent meta optimization file")
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
                args.sync_inventory, args.score, args.optimize_meta,
                args.apply_meta_proposals, args.all]):
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

        if args.optimize_meta:
            log.info("--- Generating SEO meta optimization proposals ---")
            from seo.meta_optimizer import propose
            path = propose(to_stdout=args.to_stdout)
            if path and not args.to_stdout:
                log.info(f"Review proposals at: {path}")

        if args.apply_meta_proposals:
            log.info("--- Applying approved meta proposals ---")
            from seo.meta_optimizer import apply
            updated = apply()
            log.info(f"Applied {updated} meta updates")

        if args.all or args.score:
            log.info("--- Scoring published content ---")
            from seo.scorer import run_scoring
            run_scoring(to_stdout=args.to_stdout)

        log.info("SEO content engine complete")

    except Exception:
        log.exception("SEO content engine failed")
        sys.exit(1)


def _parse_html_sections(html: str) -> dict:
    """Split HTML content on <h2> tags.

    Returns a dict where:
      - "__intro__" = everything before the first <h2> (h1 stripped)
      - Each other key = lowercase h2 heading text, value = HTML body after that heading
    """
    import re as _re

    # Strip leading <h1>...</h1>
    html = _re.sub(r"<h1[^>]*>.*?</h1>", "", html, count=1, flags=_re.DOTALL | _re.IGNORECASE).strip()

    # Split on <h2> boundaries
    parts = _re.split(r"(?=<h2[\s>])", html, flags=_re.IGNORECASE)
    sections = {}
    intro_parts = []

    for part in parts:
        h2_match = _re.match(
            r"<h2[^>]*>(.*?)</h2>(.*)",
            part,
            flags=_re.DOTALL | _re.IGNORECASE,
        )
        if h2_match:
            heading = _re.sub(r"<[^>]+>", "", h2_match.group(1)).strip().lower()
            body = h2_match.group(2).strip()
            sections[heading] = body
        else:
            intro_parts.append(part)

    sections["__intro__"] = "".join(intro_parts).strip()
    return sections


def _get_paddle_links(paddle_name: str) -> tuple[str, str]:
    """Return (review_url, affiliate_url) for a paddle from its PE review post.

    review_url  — the pickleballeffect.com review page URL
    affiliate_url — the best external purchase/affiliate link found in the review body
                    (Pickleball Central, Pickleball Galaxy, brand site, etc.)
    Either can be "" if not found.
    """
    import re as _re
    import requests as _req
    from seo.wordpress.auth import get_base_url, get_headers

    try:
        resp = _req.get(
            f"{get_base_url()}/posts",
            headers=get_headers(),
            params={"search": paddle_name, "per_page": 10, "status": "publish"},
            timeout=10,
        )
        resp.raise_for_status()
        posts = resp.json()
    except Exception as exc:
        log.warning(f"Paddle link lookup failed for '{paddle_name}': {exc}")
        return "", ""

    if not posts:
        return "", ""

    def _score(p: dict) -> int:
        url = p.get("link", "").lower()
        title = p.get("title", {}).get("rendered", "").lower()
        words = paddle_name.lower().split()
        return sum(2 if w in url else (1 if w in title else 0) for w in words)

    candidates = [p for p in posts if "equipment-review" in p.get("link", "")] or posts
    best = max(candidates, key=_score)
    if _score(best) == 0:
        return "", ""

    review_url = best.get("link", "")

    # Extract all hrefs from the rendered HTML, then find the best affiliate link.
    # Affiliate links are external (not PE-owned domains) and typically point to
    # known paddle retailers or brand sites.
    content_html = best.get("content", {}).get("rendered", "")
    all_hrefs = _re.findall(r'href=["\']([^"\'#][^"\']*)["\']', content_html)

    pe_domains = ("pickleballeffect.com", "pickleballeffectshop.com")
    external = [
        h for h in all_hrefs
        if h.startswith("http") and not any(d in h for d in pe_domains)
    ]

    # Prefer known paddle retailers; fall back to first external link
    preferred = (
        "pickleballcentral", "pickleballgalaxy", "fromuth",
        "joolapickleball", "joola.com", "selkirk.com",
        "sixzero", "vatic", "amazon.com",
    )
    affiliate_url = ""
    for link in external:
        if any(r in link.lower() for r in preferred):
            affiliate_url = link
            break
    if not affiliate_url and external:
        affiliate_url = external[0]

    log.info(
        f"Paddle links for '{paddle_name}': review={review_url} | "
        f"affiliate={affiliate_url or '(none found)'}"
    )
    return review_url, affiliate_url


def _search_wp_media(search_term: str) -> tuple[str, int]:
    """Search WordPress Media Library for an image matching the search term.

    Returns (url, id) or ("", 0) if nothing found.
    """
    import requests as _req
    from seo.wordpress.auth import get_base_url, get_headers
    try:
        resp = _req.get(
            f"{get_base_url()}/media",
            headers=get_headers(),
            params={"search": search_term, "per_page": 5, "media_type": "image"},
            timeout=10,
        )
        resp.raise_for_status()
        results = resp.json()
        if results:
            img = results[0]
            url = img.get("source_url", "")
            img_id = img.get("id", 0)
            log.info(f"Found media for '{search_term}': {url}")
            return url, img_id
    except Exception as exc:
        log.warning(f"Media library search failed for '{search_term}': {exc}")
    return "", 0


def _style_h3_headings(html: str) -> str:
    """Replace <h3> tags with H2-sized bold headers for the PE Elementor theme.

    The Elementor text-editor widget doesn't apply prominent heading styles to <h3>
    — the existing review content uses styled bold paragraphs. We match that pattern
    but at H2-like scale so subsection labels are clearly visible.
    """
    import re as _re
    html = _re.sub(
        r"<h3[^>]*>(.*?)</h3>",
        r'<p><strong style="font-size:1.4em;display:block;margin-top:1.6em;margin-bottom:0.4em;">\1</strong></p>',
        html,
        flags=_re.DOTALL | _re.IGNORECASE,
    )
    return html


def _build_article_content(
    title: str,
    slug: str,
    meta_description: str,
    content_html: str,
    content_type: str,
):
    """Map a converted HTML draft to an ArticleContent object for Elementor publishing."""
    from seo.wordpress.elementor_template import ArticleContent

    # Apply H3 → bold-paragraph conversion before mapping sections,
    # so all content regions get the fix.
    content_html = _style_h3_headings(content_html)

    sections = _parse_html_sections(content_html)
    intro = sections.get("__intro__", "")

    def _find(keys: list) -> str:
        """Return first section whose heading contains any of the given substrings."""
        for k, v in sections.items():
            if k == "__intro__":
                continue
            if any(kw in k for kw in keys):
                return v
        return ""

    if content_type == "comparison":
        import re as _re2
        # Parse paddle names from title ("X vs Y: subtitle" → ["X", "Y"])
        vs_match = _re2.split(r"\s+vs\s+", title.split(":")[0], flags=_re2.IGNORECASE)
        paddle1 = vs_match[0].strip() if vs_match else title
        paddle2 = vs_match[1].strip() if len(vs_match) > 1 else ""

        # Look up review URLs + affiliate purchase links for both paddles.
        paddle1_review_url, paddle1_affiliate_url = _get_paddle_links(paddle1)
        paddle2_review_url, paddle2_affiliate_url = _get_paddle_links(paddle2) if paddle2 else ("", "")

        # CTA and article buttons use affiliate link (purchase intent).
        # Fall back to review page if no affiliate link found.
        paddle1_cta = paddle1_affiliate_url or paddle1_review_url
        paddle2_cta = paddle2_affiliate_url or paddle2_review_url

        # Build a two-paddle sidebar with real purchase links.
        # Button style matches the JOOLA sidebar_btn Elementor widget exactly.
        sidebar_code = ""
        if paddle2:
            sidebar_code = (
                f'<hr style="border-color:rgba(255,255,255,0.25);margin:18px 0 14px;">'
                f'<h3 style="text-align:center;margin:0 0 14px;font-size:1.6rem;">{paddle2}</h3>'
                f'<a href="{paddle2_cta}" '
                f'style="display:block;padding:1rem 2rem;background:#D85E3C;color:#ffffff;'
                f'text-align:center;border-radius:50px;font-weight:400;text-decoration:none;'
                f'font-size:1.6rem;text-transform:uppercase;border:2px solid #D85E3C;">BUY NOW</a>'
            )

        comp = _find(["comparison", "side-by-side", "side by side"])
        testing = _find(["test"])
        verdict = _find(["verdict", "choose", "winner"])
        faq = _find(["faq", "frequently"])
        return ArticleContent(
            title=title,
            slug=slug,
            meta_description=meta_description,
            intro_html=intro,
            # Images: intentionally blank — media library naming is inconsistent.
            # Braydon can upload & assign images directly in WordPress.
            paddle_image1_url="",
            paddle_image2_url="",
            # Suppress review-specific widgets for comparison content
            metrics={},
            paddle_info_bullets_html="",
            specs_table_html="",
            measurements_note="",
            section2_heading="Testing Results",
            section2_body_html=testing,
            section3_heading="Side-by-Side Comparison",
            section3_body_html=comp,
            section4_heading="Verdict",
            section4_body_html=verdict + ("\n" + faq if faq else ""),
            # Top + mid article buttons → affiliate purchase link
            shop_url=paddle1_cta,
            shop_button_text=f"Buy {paddle1}",
            # CTA banner → affiliate purchase link
            cta_url=paddle1_cta,
            cta_heading=f"Get the {paddle1}",
            cta_button_text=f"BUY {paddle1.upper()}",
            # Sidebar: paddle 1 named product + purchase link, paddle 2 below
            product_name=paddle1,
            sidebar_button_text="BUY NOW",
            sidebar_url=paddle1_cta,
            sidebar_code_html=sidebar_code,
        )

    if content_type == "review":
        testing = _find(["test", "court", "on-court"])
        specs = _find(["spec", "build"])
        verdict = _find(["verdict"])
        pros = _find(["pros", "cons"])
        faq = _find(["faq", "frequently"])
        return ArticleContent(
            title=title,
            slug=slug,
            meta_description=meta_description,
            intro_html=intro,
            section2_heading="On-Court Testing",
            section2_body_html=testing,
            section3_heading="Specs & Build",
            section3_body_html=specs,
            section4_heading="Verdict",
            section4_body_html=(verdict or pros) + ("\n" + faq if faq else ""),
        )

    # educational, how_to, landing_page — put everything in section2
    body_parts = [v for k, v in sections.items() if k != "__intro__" and v]
    body = "\n".join(body_parts)
    return ArticleContent(
        title=title,
        slug=slug,
        meta_description=meta_description,
        intro_html=intro,
        section2_heading="Full Article",
        section2_body_html=body,
    )


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
            # WordPress (default for site=blog) — use Elementor layout
            from seo.wordpress.publish import publish_with_elementor
            article = _build_article_content(
                title=title,
                slug=slug,
                meta_description=meta_description,
                content_html=content_html,
                content_type=content_type,
            )
            publish_with_elementor(
                content=article,
                status="draft",
                target_keyword=target_keyword,
                content_type=content_type,
                word_count=word_count,
            )

        # Move processed draft
        processed_dir = drafts_dir / "published"
        processed_dir.mkdir(exist_ok=True)
        draft_path.rename(processed_dir / draft_path.name)
        log.info(f"Moved {draft_path.name} to published/")


if __name__ == "__main__":
    main()
