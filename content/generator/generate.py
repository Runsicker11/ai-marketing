"""AI-powered ad copy generation inspired by top-performing components."""

import csv
from datetime import datetime, timezone
from pathlib import Path

from ingestion.utils.bq_client import run_query
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.logger import get_logger
from ingestion.analysis.claude_client import analyze

log = get_logger(__name__)

_DS = f"{GCP_PROJECT_ID}.{BQ_DATASET}"
_LIBRARY_DIR = Path(__file__).resolve().parents[1] / "library"
_OUTPUT_DIR = Path(__file__).resolve().parent / "output"

# Meta ad platform constraints
MAX_HEADLINE_CHARS = 40
MAX_PRIMARY_TEXT_CHARS = 125

SYSTEM_PROMPT = """\
You are a direct-response ad copywriter for Pickleball Effect, a DTC pickleball \
accessories brand run by Braydon. You write in Braydon's voice: direct, authentic, \
no-BS, confident but approachable. Real player perspective. No corporate speak.

You will receive top-performing ad copy components and brand/product context. \
Generate new variations inspired by the winners.

Rules:
- Headlines must be {max_headline} characters or fewer
- Primary text must be {max_primary_text} characters or fewer
- Write in Braydon's authentic voice - like a friend recommending gear
- Focus on benefits, not features
- Use specific numbers/data when possible (e.g., "Tested on 50+ paddles")
- No emojis, no ALL CAPS, no exclamation marks abuse
- Each variation should feel distinct, not just word-swapping

Output format (CSV, one per line):
TYPE,COMPONENT_ID,TEXT
hook,H_NEW_001,"Your headline text here"
body,B_NEW_001,"Your primary text here"

Generate the exact number requested. Every line must be valid CSV.\
""".format(max_headline=MAX_HEADLINE_CHARS, max_primary_text=MAX_PRIMARY_TEXT_CHARS)


def _load_top_components(min_score: float = 7.0) -> dict[str, list[dict]]:
    """Load top-performing components from BigQuery content_library."""
    sql = f"""
    SELECT component_id, component_type, text, score, source_ad_name
    FROM `{_DS}.content_library`
    WHERE score >= {min_score}
      AND status IN ('active', 'proven')
    ORDER BY score DESC
    """
    try:
        rows = list(run_query(sql))
    except Exception as e:
        log.warning(f"Could not query content_library: {e}")
        rows = []

    # Fall back to CSV files if BQ is empty
    if not rows:
        return _load_from_csvs(min_score)

    by_type = {"hook": [], "body": [], "cta": []}
    for r in rows:
        d = dict(r)
        ct = d.get("component_type", "")
        if ct in by_type:
            by_type[ct].append(d)
    return by_type


def _load_from_csvs(min_score: float) -> dict[str, list[dict]]:
    """Fallback: load components from CSV files."""
    by_type = {"hook": [], "body": [], "cta": []}
    for comp_type in ("hook", "body", "cta"):
        path = _LIBRARY_DIR / f"{comp_type}s.csv"
        if not path.exists():
            continue
        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    score = float(row.get("score", 0))
                except ValueError:
                    score = 0
                if score >= min_score:
                    by_type[comp_type].append(row)
    return by_type


def _load_product_context(product: str | None) -> str:
    """Load product info from brand.yaml for targeted generation."""
    import yaml
    brand_path = Path(__file__).resolve().parents[2] / "config" / "brand.yaml"
    if not brand_path.exists():
        return ""

    brand = yaml.safe_load(brand_path.read_text(encoding="utf-8"))
    offerings = brand.get("offerings", [])

    if product:
        # Find matching product
        for o in offerings:
            name = o.get("name", "").lower()
            if product.lower() in name or name in product.lower():
                benefits = ", ".join(o.get("key_benefits", []))
                return f"Product focus: {o['name']} ({o.get('category', '')}). Benefits: {benefits}"
        return f"Product focus: {product}"

    # Return all products
    parts = []
    for o in offerings:
        benefits = ", ".join(o.get("key_benefits", []))
        parts.append(f"- {o['name']} ({o.get('category', '')}): {benefits}")
    return "Products:\n" + "\n".join(parts)


def _format_winners(components: dict[str, list[dict]]) -> str:
    """Format top components for Claude context."""
    lines = []
    for comp_type, items in components.items():
        if items:
            lines.append(f"\n### Top {comp_type}s:")
            for item in items[:10]:
                score = item.get("score", "?")
                lines.append(f"  [{score}] {item.get('text', '')}")
    return "\n".join(lines) if lines else "No existing top performers found."


def _parse_generated(claude_response: str) -> list[dict]:
    """Parse generated components from Claude's CSV output."""
    components = []
    for line in claude_response.split("\n"):
        stripped = line.strip()
        if not stripped or stripped.startswith("TYPE,") or stripped.startswith("```"):
            continue

        parts = list(csv.reader([stripped]))
        if not parts or not parts[0]:
            continue
        row = parts[0]
        if len(row) < 3:
            continue

        comp_type = row[0].strip().lower()
        if comp_type not in ("hook", "body"):
            continue

        comp_id = row[1].strip()
        text = row[2].strip()

        # Validate character limits
        if comp_type == "hook" and len(text) > MAX_HEADLINE_CHARS:
            log.warning(f"Headline too long ({len(text)} chars), truncating: {text}")
            text = text[:MAX_HEADLINE_CHARS]
        if comp_type == "body" and len(text) > MAX_PRIMARY_TEXT_CHARS:
            log.warning(f"Primary text too long ({len(text)} chars), truncating: {text}")
            text = text[:MAX_PRIMARY_TEXT_CHARS]

        components.append({
            "component_type": comp_type,
            "component_id": comp_id,
            "text": text,
        })

    return components


def _save_pending_review(components: list[dict], product: str | None):
    """Save generated components to pending_review.csv."""
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"_{product}" if product else ""
    path = _OUTPUT_DIR / f"pending_review{suffix}_{timestamp}.csv"

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["component_id", "component_type", "text", "status", "generated_at"])
        for c in components:
            writer.writerow([
                c["component_id"],
                c["component_type"],
                c["text"],
                "pending_review",
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            ])

    log.info(f"Saved {len(components)} generated components to {path}")
    return path


def generate(count: int = 10, product: str | None = None,
             to_stdout: bool = False) -> list[dict]:
    """Generate new ad copy variations inspired by top performers.

    Args:
        count: Number of variations to generate (split between hooks and bodies).
        product: Optional product name for targeted generation.
        to_stdout: Print results instead of saving.

    Returns:
        List of generated component dicts.
    """
    log.info(f"Generating {count} new variations"
             f"{f' for {product}' if product else ''}")

    # Load inputs
    top_components = _load_top_components(min_score=7.0)
    product_context = _load_product_context(product)
    winners_text = _format_winners(top_components)

    hooks_count = count // 2
    bodies_count = count - hooks_count

    data_context = (
        f"## Top-Performing Components\n{winners_text}\n\n"
        f"## Product Context\n{product_context}"
    )

    question = (
        f"Generate exactly {hooks_count} new hooks (headlines) and "
        f"{bodies_count} new bodies (primary text) for Meta ads. "
        f"{'Focus on ' + product + '. ' if product else ''}"
        f"Inspire from the winners above but create distinct variations. "
        f"Use Braydon's voice. Output as CSV only."
    )

    response = analyze(SYSTEM_PROMPT, data_context, question)
    components = _parse_generated(response)

    if not components:
        log.warning("No components parsed from Claude's response")
        if to_stdout:
            print("No components generated. Raw response:")
            print(response)
        return []

    log.info(f"Generated {len(components)} components "
             f"({sum(1 for c in components if c['component_type'] == 'hook')} hooks, "
             f"{sum(1 for c in components if c['component_type'] == 'body')} bodies)")

    if to_stdout:
        print(f"\n{'='*60}")
        print(f"Generated {len(components)} ad copy variations")
        print(f"{'='*60}")
        for c in components:
            label = "HEADLINE" if c["component_type"] == "hook" else "PRIMARY TEXT"
            char_count = len(c["text"])
            limit = MAX_HEADLINE_CHARS if c["component_type"] == "hook" else MAX_PRIMARY_TEXT_CHARS
            print(f"\n[{c['component_id']}] {label} ({char_count}/{limit} chars)")
            print(f"  {c['text']}")
    else:
        _save_pending_review(components, product)

    return components
