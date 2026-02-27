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


def _load_template(content_type: str) -> str:
    """Load markdown template for a content type."""
    path = _TEMPLATE_DIR / f"{content_type}.md"
    if not path.exists():
        log.warning(f"Template not found: {path}")
        return ""
    return path.read_text(encoding="utf-8")


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


SYSTEM_PROMPT = """\
You are a professional SEO content writer for Pickleball Effect, a pickleball \
review and accessories site run by Braydon. You write in Braydon's voice: \
direct, authentic, no-BS, confident but approachable. Real player perspective.

You will produce a complete article draft following strict SEO rules and the \
template structure provided. Every section must be fully written — no placeholders.

Writing guidelines:
- Write in Braydon's voice: conversational, data-backed, real-player perspective
- Use the target keyword naturally in the required positions
- Include specific data, measurements, and testing results where relevant
- Suggest internal links to existing content where appropriate
- Write for a Grade 8 reading level (Flesch-Kincaid)
- No corporate speak, no hype, no unverified claims
- No emojis, no ALL CAPS, no exclamation mark abuse
"""


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
    to_stdout: bool = False,
) -> dict:
    """Generate a full article draft with SEO optimization.

    Args:
        target_keyword: Primary keyword to target.
        content_type: Type of content (review, comparison, how_to, landing_page).
        product: Optional product focus.
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
    )

    response = analyze(SYSTEM_PROMPT, data_context, question)

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
    """Remove YAML frontmatter from content."""
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
        f"status: draft\n"
        f"word_count: {result['word_count']}\n"
        f"generated_at: {datetime.now(timezone.utc).isoformat()}\n"
        f"---\n\n"
    )

    path.write_text(frontmatter + result["content"], encoding="utf-8")
    log.info(f"Draft saved to {path}")
