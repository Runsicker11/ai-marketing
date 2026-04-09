"""AI-powered SEO content generation with strict SEO guidelines and brand voice."""

import re
from datetime import datetime, timezone
from pathlib import Path

import yaml

from ingestion.utils.bq_client import run_query
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.logger import get_logger
from ingestion.analysis.claude_client import analyze

log = get_logger(__name__)

_DS = f"{GCP_PROJECT_ID}.{BQ_DATASET}"
_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
_TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"
_DRAFTS_DIR = Path(__file__).resolve().parent / "drafts"


def _load_seo_config() -> dict:
    """Load SEO content configuration."""
    path = _CONFIG_DIR / "seo_content.yaml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _load_brand_voice() -> dict:
    """Load brand voice section from brand.yaml."""
    path = _CONFIG_DIR / "brand.yaml"
    brand = yaml.safe_load(path.read_text(encoding="utf-8"))
    return brand.get("voice", {})


def _load_template(content_type: str) -> str:
    """Load markdown template for a content type."""
    path = _TEMPLATE_DIR / f"{content_type}.md"
    if not path.exists():
        log.warning(f"Template not found: {path}")
        return ""
    return path.read_text(encoding="utf-8")


def _fetch_pe_review_content(keyword: str, content_type: str) -> str:
    """Fetch PE review content for paddles referenced in the keyword.

    For comparisons ("X vs Y"), fetches both reviews.
    For single reviews, fetches the one review.
    Returns combined source text, or empty string on failure.
    """
    if content_type not in ("comparison", "review"):
        return ""

    if " vs " in keyword.lower():
        parts = re.split(r"\s+vs\s+", keyword, flags=re.IGNORECASE)
        paddle_names = [p.strip() for p in parts]
    else:
        paddle_names = [keyword.strip()]

    sections = []
    for name in paddle_names:
        content = _search_and_fetch_wp_review(name)
        if content:
            sections.append(f"### PE Review: {name.title()}\n{content}")
        else:
            log.warning(f"No PE review content found for '{name}'")

    return "\n\n".join(sections)


def _search_and_fetch_wp_review(paddle_name: str) -> str:
    """Search WordPress REST API for a review post, then fetch its rendered page.

    Returns plain-text review content (up to ~4000 chars), or empty string.
    """
    import requests as _req

    base = "https://pickleballeffect.com"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }

    # ── Step 1: Find the post via WP REST API ─────────────────────────
    try:
        resp = _req.get(
            f"{base}/wp-json/wp/v2/posts",
            params={"search": paddle_name, "per_page": 10, "status": "publish"},
            headers=headers,
            timeout=15,
        )
        resp.raise_for_status()
        posts = resp.json()
    except Exception as exc:
        log.warning(f"WP REST search failed for '{paddle_name}': {exc}")
        return ""

    if not posts:
        return ""

    # Score results by how many paddle-name words appear in the URL/title.
    # URL match weighted 2x — slugs are more reliable than body mentions.
    def _score(post: dict) -> int:
        url = post.get("link", "").lower()
        title = post.get("title", {}).get("rendered", "").lower()
        words = paddle_name.lower().split()
        return sum(2 if w in url else (1 if w in title else 0) for w in words)

    posts_in_reviews = [p for p in posts if "equipment-review" in p.get("link", "")]
    candidates = posts_in_reviews or posts
    post = max(candidates, key=_score)

    # Require at least one word match; bail if nothing relevant found
    if _score(post) == 0:
        log.warning(f"No relevant WP post found for '{paddle_name}' (best match: {post.get('link', '')})")
        return ""
    title = post.get("title", {}).get("rendered", paddle_name)
    page_url = post.get("link", "")

    # ── Step 2: Extract text from REST API rendered content ────────────
    # content.rendered contains the full Elementor HTML — strip tags to get
    # plain text. This is more reliable than fetching the page directly,
    # which requires parsing Cloudflare-served HTML with Elementor structure.
    content_html = post.get("content", {}).get("rendered", "")
    if not content_html:
        log.warning(f"No content in REST API response for '{paddle_name}'")
        return ""

    text = re.sub(r"<[^>]+>", " ", content_html)
    text = re.sub(r"\s+", " ", text).strip()

    MAX_CHARS = 5000
    if len(text) > MAX_CHARS:
        text = text[:MAX_CHARS] + "…"

    log.info(f"Fetched PE review for '{paddle_name}': {len(text)} chars — {page_url}")
    return f"[Source: {page_url}]\n[Title: {title}]\n\n{text}"


def _load_product_data(product: str | None) -> str:
    """Load product data from BigQuery."""
    if product:
        sql = f"""
        SELECT p.title, p.handle, p.product_type, p.tags,
               v.price, v.compare_at_price
        FROM `{_DS}.shopify_products` p
        LEFT JOIN `{_DS}.shopify_product_variants` v ON p.product_id = v.product_id
        WHERE LOWER(p.handle) LIKE '%{product.lower()}%'
            OR LOWER(p.title) LIKE '%{product.lower()}%'
        LIMIT 5
        """
    else:
        sql = f"""
        SELECT p.title, p.handle, p.product_type, p.tags,
               v.price, v.compare_at_price
        FROM `{_DS}.shopify_products` p
        LEFT JOIN `{_DS}.shopify_product_variants` v ON p.product_id = v.product_id
        WHERE p.status = 'ACTIVE'
        LIMIT 20
        """
    try:
        rows = list(run_query(sql))
    except Exception:
        return "No product data available."

    if not rows:
        return "No matching products found."

    lines = []
    for r in rows:
        lines.append(
            f"- {r.title} ({r.handle}): ${r.price or 0:.2f}, "
            f"type={r.product_type or 'N/A'}"
        )
    return "\n".join(lines)


def _load_existing_content() -> str:
    """Load existing content inventory for internal linking suggestions."""
    sql = f"""
    SELECT title, url, content_type, target_keyword
    FROM `{_DS}.content_posts`
    WHERE status IN ('published', 'draft')
    ORDER BY created_at DESC
    LIMIT 30
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        return "No existing content inventory available."

    if not rows:
        return "No existing content tracked."

    lines = []
    for r in rows:
        lines.append(f"- [{r.title}]({r.url}) (keyword: {r.target_keyword or 'N/A'})")
    return "\n".join(lines)


def _build_system_prompt(content_type: str) -> str:
    """Build a content-type-specific system prompt grounded in the brand voice guide."""
    voice = _load_brand_voice()

    # ── Shared brand voice rules (from brand.yaml) ─────────────────────
    avoid_words = ", ".join(f'"{w}"' for w in voice.get("avoid", {}).get("words", []))
    avoid_patterns = "\n".join(
        f"  - {p}" for p in voice.get("avoid", {}).get("patterns", [])
    )
    signature_phrases = voice.get("signature_phrases", {})
    verdicts   = ", ".join(f'"{p}"' for p in signature_phrases.get("verdicts", []))
    feel_desc  = ", ".join(f'"{p}"' for p in signature_phrases.get("feel_descriptors", []))
    guidance   = "\n".join(f'  - {p}' for p in signature_phrases.get("guidance", []))

    shared = f"""\
You write for Pickleball Effect, run by Braydon Unsicker.

Voice: {voice.get("tone", "")}. Style: {voice.get("style", "")}.
Tagline: {voice.get("tagline", "")}

Sentence style:
  - Mix short declarative sentences (emphasis) with medium complex (15-25 words)
  - Em-dashes for mid-sentence context
  - Short paragraphs: 2-5 sentences max. Single-sentence paragraphs for emphasis.
  - Questions frame sections — never rhetorical or leading

Person: first-person (I, my) for opinions and testing experience. \
Second-person (you, your) for reader guidance. Never "we" for opinions.

Technical depth: assumes intermediate player knowledge. \
Explains construction methods, thermoforming, twistweight, swingweight when relevant. \
Pattern: Introduce → Define → Contextualize → Apply.

Never use these words: {avoid_words}
Avoid these patterns:
{avoid_patterns}

Signature verdict phrases (use naturally, not formulaically): {verdicts}
Feel descriptors: {feel_desc}
Guidance phrases:
{guidance}

Every section must be fully written — no placeholders, no TODOs.
Write for a Grade 8 reading level (Flesch-Kincaid).
"""

    # ── Review / comparison: paddle evaluation mode ─────────────────────
    if content_type in ("review", "comparison"):
        return shared + """
MODE: Paddle evaluation.

You are helping a player decide whether a specific paddle is right for them.
Structure your analysis by characteristic (power, control, feel, etc.) — never
chronological play-by-play.

Performance scoring uses a Low / Medium / High scale across:
Power, Pop, Control, Forgiveness/Sweet Spot, Spin, Maneuverability.

No perfect paddles. Control paddles get low power scores. Power paddles get low
control scores. Be honest about tradeoffs.

Buy If / Pass If: 3-5 specific, actionable bullets each.
Bottom line: one clear verdict sentence starting with "Bottom line:"

CTAs: link to the affiliate/shop URL for the reviewed paddle.
Internal links: link to related reviews and the PE paddle database.
"""

    # ── Educational: teacher mode ────────────────────────────────────────
    if content_type == "educational":
        return shared + """
MODE: Expert teacher.

You are helping a player understand a concept — not evaluate a specific paddle
for purchase. Your job is to explain clearly and connect the knowledge to
something they can act on.

Structure:
  - Open with why this concept matters to their game (not a dictionary definition)
  - Explain using real-world examples and, where relevant, measured data
  - "Why It Matters" section connects the concept to actual play outcomes
  - Key Takeaways: 3-5 tight bullet points a player can remember

Shop CTA (one per article, near the end):
  If the concept connects naturally to a PE shop product (tungsten tape,
  overgrips, edge guard tape, tuning tape), include a single soft CTA.
  Write it in Braydon's voice — helpful, not pushy. Example style:
  "If you want to experiment with swingweight yourself, we carry tungsten
  tape strips at the PE shop — use code EFFECT for a discount."
  Do NOT include a shop CTA if there is no natural product connection.

Internal links: link to related reviews and the PE paddle database as
"go deeper" references — not purchase pushes.

No Buy If / Pass If. No paddle purchase CTAs. Keep it tight — 600-1100 words.
"""

    # ── How-to: task completion mode ─────────────────────────────────────
    if content_type == "how_to":
        return shared + """
MODE: Task guide.

Walk the reader through completing a specific task. Steps should be numbered,
specific, and actionable. Include the "why" behind each step — not just what
to do, but why it matters.

Where PE shop products (tungsten tape, grips, edge guard tape) are relevant
to the task, mention them naturally with a soft shop CTA and discount code.
One CTA maximum — don't pepper the article.

Internal links: related how-tos, reviews, or the paddle database.
No Buy If / Pass If.
"""

    # ── Landing page: conversion mode ───────────────────────────────────
    if content_type == "landing_page":
        return shared + """
MODE: Conversion page.

Short, benefit-led, conversion-focused. This is for a PE shop product page
or category page. Lead with the player problem, follow with the product solution.

Keep it tight — 300-800 words. One clear CTA. No jargon.
Social proof: reference court testing, player feedback, or Braydon's use.
"""

    # ── Default fallback ─────────────────────────────────────────────────
    return shared


def _validate_draft(
    content: str,
    title: str,
    meta_description: str,
    target_keyword: str,
    content_type: str,
    config: dict,
) -> list[str]:
    """Validate the generated draft against SEO rules.

    Returns:
        List of validation issues (empty = all good).
    """
    issues = []
    type_config = config.get("content_types", {}).get(content_type, {})
    seo_rules = config.get("seo_rules", {})

    # Title length
    title_max = seo_rules.get("title_max_chars", 60)
    if len(title) > title_max:
        issues.append(f"Title is {len(title)} chars (max {title_max})")

    # Meta description length
    meta_max = seo_rules.get("meta_description_max_chars", 155)
    if len(meta_description) > meta_max:
        issues.append(f"Meta description is {len(meta_description)} chars (max {meta_max})")

    # Word count
    word_count_range = type_config.get("word_count", [500, 3000])
    word_count = len(content.split())
    if word_count < word_count_range[0]:
        issues.append(f"Word count {word_count} below minimum {word_count_range[0]}")
    if word_count > word_count_range[1]:
        issues.append(f"Word count {word_count} above maximum {word_count_range[1]}")

    # Required sections
    required_sections = type_config.get("required_sections", [])
    for section in required_sections:
        # Check for ## heading containing the section name
        section_patterns = {
            "intro": r"^#\s",
            "specs": r"(?i)spec",
            "testing": r"(?i)test",
            "pros_cons": r"(?i)pros|cons",
            "verdict": r"(?i)verdict",
            "faq": r"(?i)faq|frequently",
            "side_by_side": r"(?i)side.by.side|comparison",
            "steps": r"(?i)step",
            "tips": r"(?i)tip",
            "hero": r"^#\s",
            "benefits": r"(?i)benefit|why",
            "social_proof": r"(?i)saying|review|testimonial",
            "cta": r"(?i)ready|get|shop|buy",
        }
        pattern = section_patterns.get(section, section)
        if not re.search(pattern, content, re.MULTILINE):
            issues.append(f"Missing required section: {section}")

    # Keyword in title
    if target_keyword.lower() not in title.lower():
        issues.append(f"Target keyword '{target_keyword}' not found in title")

    return issues


def generate_article(
    target_keyword: str,
    content_type: str = "review",
    product: str | None = None,
    site: str = "blog",
    to_stdout: bool = False,
) -> dict:
    """Generate a full article draft with SEO optimization.

    Args:
        target_keyword: Primary keyword to target.
        content_type: Type of content (review, comparison, how_to, landing_page).
        product: Optional product focus.
        site: Target site — "blog" (WordPress) or "shop" (Shopify).
        to_stdout: Print output instead of saving.

    Returns:
        Dict with title, meta_description, slug, content, and validation results.
    """
    log.info(f"Generating {content_type} for keyword '{target_keyword}'"
             f"{f' (product: {product})' if product else ''}")

    config = _load_seo_config()
    template = _load_template(content_type)
    product_data = _load_product_data(product)
    existing_content = _load_existing_content()
    pe_source = _fetch_pe_review_content(target_keyword, content_type)

    type_config = config.get("content_types", {}).get(content_type, {})
    seo_rules = config.get("seo_rules", {})

    word_range = type_config.get("word_count", [800, 2000])
    required_sections = type_config.get("required_sections", [])
    keyword_placement = type_config.get("keyword_placement", [])

    data_context = (
        f"## Template Structure\n{template}\n\n"
        f"## Product Data\n{product_data}\n\n"
        f"## Existing Content (for internal links)\n{existing_content}"
    )
    if pe_source:
        data_context += (
            f"\n\n## PE Source Review Content\n"
            f"(Actual on-court notes, specs, and measurements from Pickleball Effect reviews — "
            f"use these as the authoritative source for all technical details)\n\n"
            f"{pe_source}"
        )

    source_note = (
        "\n\nSOURCE REQUIREMENT: PE review content is provided above. "
        "Use those actual specs, measurements, and on-court assessments verbatim — "
        "do NOT invent or estimate paddle specifications. "
        "If a spec is not mentioned in the source reviews, omit it rather than guessing."
    ) if pe_source else ""

    question = (
        f"Write a complete {content_type} article targeting the keyword "
        f"'{target_keyword}'.\n\n"
        f"Requirements:\n"
        f"- Word count: {word_range[0]}-{word_range[1]} words\n"
        f"- Required sections: {', '.join(required_sections)}\n"
        f"- Keyword must appear in: {', '.join(keyword_placement)}\n"
        f"- Title: under {seo_rules.get('title_max_chars', 60)} characters\n"
        f"- Meta description: under {seo_rules.get('meta_description_max_chars', 155)} characters\n"
        f"{'- Product focus: ' + product if product else ''}\n\n"
        f"Output the COMPLETE article in markdown with frontmatter "
        f"(title, meta_description, target_keyword, slug). "
        f"Write every section fully — no placeholders or TODOs."
        f"{source_note}"
    )

    response = analyze(_build_system_prompt(content_type), data_context, question)

    # Parse frontmatter from response
    title, meta_description, slug = _parse_frontmatter(response, target_keyword)

    # Strip frontmatter from content
    content = _strip_frontmatter(response)

    # Validate
    issues = _validate_draft(
        content, title, meta_description, target_keyword, content_type, config
    )

    if issues:
        log.warning(f"Validation issues: {issues}")

    result = {
        "title": title,
        "meta_description": meta_description,
        "slug": slug,
        "target_keyword": target_keyword,
        "content_type": content_type,
        "site": site,
        "content": content,
        "word_count": len(content.split()),
        "validation_issues": issues,
    }

    if to_stdout:
        print(f"\n{'='*60}")
        print(f"Title: {title}")
        print(f"Meta: {meta_description}")
        print(f"Slug: {slug}")
        print(f"Words: {result['word_count']}")
        if issues:
            print(f"Issues: {', '.join(issues)}")
        print(f"{'='*60}\n")
        print(content)
    else:
        _save_draft(result)

    return result


def _parse_frontmatter(text: str, default_keyword: str) -> tuple[str, str, str]:
    """Extract title, meta_description, slug from frontmatter."""
    title = default_keyword.title()
    meta_description = ""
    slug = default_keyword.lower().replace(" ", "-")

    # Strip markdown code fences if the AI wrapped the response
    text = re.sub(r"^```(?:markdown)?\s*\n", "", text.strip(), flags=re.IGNORECASE)
    text = re.sub(r"\n```\s*$", "", text.strip())

    # Try YAML frontmatter
    fm_match = re.match(r"^---\s*\n(.*?)\n---", text, re.DOTALL)
    if fm_match:
        fm_text = fm_match.group(1)
        for line in fm_text.split("\n"):
            if line.startswith("title:"):
                title = line.split(":", 1)[1].strip().strip('"').strip("'")
            elif line.startswith("meta_description:"):
                meta_description = line.split(":", 1)[1].strip().strip('"').strip("'")
            elif line.startswith("slug:"):
                slug = line.split(":", 1)[1].strip().strip('"').strip("'")
    else:
        # Try to extract from first H1
        h1_match = re.search(r"^#\s+(.+)$", text, re.MULTILINE)
        if h1_match:
            title = h1_match.group(1).strip()

    return title, meta_description, slug


def _strip_frontmatter(text: str) -> str:
    """Remove YAML frontmatter and any wrapping markdown code fences from content."""
    text = re.sub(r"^```(?:markdown)?\s*\n", "", text.strip(), flags=re.IGNORECASE)
    text = re.sub(r"\n```\s*$", "", text.strip())
    return re.sub(r"^---\s*\n.*?\n---\s*\n", "", text, flags=re.DOTALL).strip()


def _save_draft(result: dict):
    """Save generated draft to file."""
    _DRAFTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{result['content_type']}_{result['slug']}_{timestamp}.md"
    path = _DRAFTS_DIR / filename

    # Reconstruct with frontmatter
    frontmatter = (
        f"---\n"
        f"title: \"{result['title']}\"\n"
        f"meta_description: \"{result['meta_description']}\"\n"
        f"target_keyword: \"{result['target_keyword']}\"\n"
        f"slug: \"{result['slug']}\"\n"
        f"content_type: {result['content_type']}\n"
        f"site: {result.get('site', 'blog')}\n"
        f"status: draft\n"
        f"word_count: {result['word_count']}\n"
        f"generated_at: {datetime.now(timezone.utc).isoformat()}\n"
        f"---\n\n"
    )

    path.write_text(frontmatter + result["content"], encoding="utf-8")
    log.info(f"Draft saved to {path}")
