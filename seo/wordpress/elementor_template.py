"""
Elementor template cloner for WordPress post publishing.

Takes article content and produces a fully Elementor-structured post by
deep-cloning the review_template.json and swapping in new content values.

Usage:
    from seo.wordpress.elementor_template import ArticleContent, build_elementor_data

    content = ArticleContent(
        title="Joola Hyperion CGS 16mm Review",
        slug="joola-hyperion-cgs-16mm-review",
        intro_html="<p>The Hyperion CGS 16mm is ...</p>",
        shop_url="https://joola.com/...",
        discount_code="EFFECT",
        metrics={"Power": "Low", "Pop": "Medium", "Control": "High", ...},
        ...
    )
    elementor_json_string = build_elementor_data(content)
"""

import copy
import json
from dataclasses import dataclass, field
from pathlib import Path

from ingestion.utils.logger import get_logger

log = get_logger(__name__)

_TEMPLATE_PATH = Path(__file__).parent / "review_template.json"

# ─── Widget IDs — sourced from Engage X2 post (ID 37908) ──────────────────
# These IDs map to specific widgets in review_template.json.
# If the template is ever regenerated from a different post, update these.
_WID = {
    # Intro section (two-column hero)
    "intro_text":        "2dbd5f0",   # text-editor: opening paragraphs
    "shop_btn_top":      "ad53288",   # button: "Shop [Paddle]" — top
    "discount_top":      "2809520",   # text-editor: "Save with code X"
    "toc_list":          "7809720",   # text-editor: <ol> TOC links

    # Paddle Snapshot section
    "section1_heading":  "adc9cff",   # heading: "Paddle Snapshot"
    "image1":            "d20a1bf",   # image: paddle front photo
    "image2":            "722bf88",   # image: paddle X-ray / second angle
    "metrics_html":      "7322d0a",   # html: pe-metrics widget
    "slider_soft_stiff": "a04bce4",   # html: Soft ↔ Stiff slider
    "slider_dense_hollow":"0c12ee4",  # html: Dense ↔ Hollow slider
    "paddle_info_bullets":"08ab318",  # text-editor: paddle type/core/face/price list
    "measurements_note": "8860ef3",   # text-editor: measurement disclaimer
    "specs_table":       "e9afbbc",   # text-editor: specs table HTML

    # On-Court section
    "section2_heading":  "f603e52",   # heading: "On-Court Feel & Play Experience"
    "video":             "1779d26",   # video widget: YouTube URL + thumbnail
    "section2_body":     "5a1a24b",   # text-editor: Key Performance Traits + body
    "shop_btn_mid":      "95d3979",   # button: mid-article shop CTA
    "discount_mid":      "fa6bd5e",   # text-editor: "Save with code X" (mid)

    # Comparisons section
    "section3_heading":  "659176d",   # heading: "Comparisons"
    "section3_body":     "c18df9d",   # text-editor: vs comparisons

    # Who It's Best For section
    "section4_heading":  "4cd34d4",   # heading: "Who It's Best For"
    "section4_body":     "2f0e32e",   # text-editor: best-for analysis

    # CTA banner
    "cta_heading":       "655ec4d",   # heading: "Save directly from X with code Y"
    "cta_btn":           "24b65c2",   # button: "SHOP [PADDLE]"

    # Reviewer profile (testimonial widget)
    "reviewer":          "8dfb843",

    # Product sidebar
    "product_image":     "b33891f",   # image: product photo
    "product_name":      "8dc6606",   # heading: paddle name
    "sidebar_btn":       "6230bee",   # button: "GET YOURS NOW"
    "sidebar_code":      "501579e",   # text-editor: "Use code X"
}


# ─── Content Schema ────────────────────────────────────────────────────────

@dataclass
class ArticleContent:
    """All variable content fields for a paddle review post.

    Fields map directly to Elementor widgets in review_template.json.
    Only title, slug, intro_html, and shop_url are truly required —
    everything else has a sensible default or is optional.
    """

    # ── WordPress meta (required) ──────────────────────────────────────
    title: str                              # Post title — H1 auto-pulls from WP
    slug: str                               # URL slug
    meta_description: str = ""             # For SEO plugin

    # ── Intro hero section ─────────────────────────────────────────────
    intro_html: str = ""                    # Opening paragraph(s) HTML
    shop_url: str = ""                      # Primary affiliate/shop URL
    shop_button_text: str = "Shop Now"      # e.g. "Shop Joola Hyperion"
    discount_code: str = ""                 # e.g. "EFFECT"

    # ── Table of Contents ──────────────────────────────────────────────
    # Provide section names; anchors are auto-generated as #toc1, #toc2, ...
    toc_items: list = field(default_factory=lambda: [
        "Paddle Snapshot",
        "On-Court Performance",
        "Comparisons",
        "Who It's Best For",
    ])

    # ── Paddle Snapshot ────────────────────────────────────────────────
    section1_heading: str = "Paddle Snapshot"

    # Image 1: paddle front photo
    paddle_image1_url: str = ""
    paddle_image1_id: int = 0
    paddle_image1_caption: str = ""

    # Image 2: X-ray scan or second angle
    paddle_image2_url: str = ""
    paddle_image2_id: int = 0
    paddle_image2_caption: str = "X-Ray Scan"

    # Performance metrics — value must be "Low", "Medium", or "High"
    metrics: dict = field(default_factory=lambda: {
        "Power":                  "Medium",
        "Pop":                    "Medium",
        "Control":                "Medium",
        "Forgiveness / Sweet Spot": "Medium",
        "Spin":                   "Medium",
        "Maneuverability":        "Medium",
    })

    # Feel sliders — percentage left position (0 = leftmost, 100 = rightmost)
    # soft_stiff: 0 = Pure Soft, 100 = Pure Stiff
    # dense_hollow: 0 = Pure Dense, 100 = Pure Hollow
    feel_soft_stiff_pct: int = 50
    feel_dense_hollow_pct: int = 50

    # Paddle info bullets — shown below feel sliders
    paddle_info_bullets_html: str = ""     # <ul><li>...</li></ul>
    measurements_note: str = (
        "Measurements are from my test unit and may vary slightly "
        "from yours due to manufacturing tolerances."
    )
    specs_table_html: str = ""             # Full <table> HTML (with overflow wrapper)

    # ── On-Court section ───────────────────────────────────────────────
    section2_heading: str = "On-Court Feel & Play Experience"
    video_url: str = ""                    # YouTube URL
    video_thumbnail_url: str = ""
    video_thumbnail_id: int = 0
    section2_body_html: str = ""           # Key performance traits + analysis

    # ── Comparisons section ────────────────────────────────────────────
    section3_heading: str = "Comparisons"
    section3_body_html: str = ""

    # ── Who It's Best For section ──────────────────────────────────────
    section4_heading: str = "Who It's Best For"
    section4_body_html: str = ""

    # ── CTA banner ─────────────────────────────────────────────────────
    cta_heading: str = ""                  # "Save directly from X with code Y"
    cta_button_text: str = "SHOP NOW"
    cta_url: str = ""

    # ── Reviewer profile ───────────────────────────────────────────────
    reviewer_name: str = "Braydon Unsicker"
    reviewer_title: str = "5.04 DUPR"     # Shows as the "job" subtitle
    reviewer_bio_html: str = (
        "<p><strong>Reviewer Profile</strong></p>"
        "<p>Braydon Unsicker is the founder of Pickleball Effect and has been "
        "reviewing pickleball paddles since 2021. Every paddle is tested on court "
        "with in-house measured specs — twistweight, swingweight, and spin RPMs. "
        "No paid reviews. No brand relationships.</p>"
    )
    reviewer_image_url: str = (
        "https://pickleballeffect.com/wp-content/uploads/2025/07/"
        "DSC049221-207x300-1.webp"
    )
    reviewer_image_id: int = 28673

    # ── Product sidebar ────────────────────────────────────────────────
    product_name: str = ""
    product_image_url: str = ""
    product_image_id: int = 0
    sidebar_button_text: str = "GET YOURS NOW"
    sidebar_url: str = ""
    sidebar_code_html: str = ""            # e.g. "<p>Use code EFFECT</p>"


# ─── HTML Builders ─────────────────────────────────────────────────────────

_METRICS_CSS = """<style>
.pe-metrics,
.pe-metrics * {
  color: #ffffff !important;
}
.pe-metrics .metric {
  margin-bottom: 10px;
}
.pe-metrics .label {
  font-weight: 700;
  display: block;
  margin-bottom: 2px;
}
.pe-metrics .selected {
  font-weight: 700;
  background: #d85e3c;
  padding: 2px 8px;
  border-radius: 12px;
}
</style>"""

_SLIDER_CSS = """<style>
.paddle-slider {
  max-width: 400px;
  font-family: inherit;
  color: #fff;
}
.slider-labels {
  display: flex;
  justify-content: space-between;
  font-size: 16px;
  margin-bottom: 6px;
  color: #fff;
}
.slider-track {
  position: relative;
  height: 4px;
  background: #fff;
  border-radius: 2px;
}
.slider-dot {
  position: absolute;
  top: 50%;
  transform: translate(-50%, -50%);
  width: 12px;
  height: 12px;
  background: #d85e3c;
  border-radius: 50%;
}
</style>"""


def _build_metrics_html(metrics: dict) -> str:
    """Generate the .pe-metrics HTML block from a {label: Low/Medium/High} dict."""
    options = ["Low", "Medium", "High"]
    rows = []
    for label, selected in metrics.items():
        parts = []
        found = False
        for i, opt in enumerate(options):
            if opt == selected:
                parts.append(f'<span class="selected">{opt}</span>')
                found = True
            elif not found:
                parts.append(f"<span>{opt} • </span>")
            else:
                parts.append(f"<span> • {opt}</span>")
        row = (
            f'<div class="metric">\n'
            f'  <span class="label">{label}</span>\n'
            f'  {"".join(parts)}\n'
            f"</div>"
        )
        rows.append(row)

    return (
        f'<div class="pe-metrics">\n'
        f'{_METRICS_CSS}\n\n'
        + "\n\n".join(rows)
        + "\n\n</div>"
    )


def _build_slider_html(left_label: str, right_label: str, pct: int) -> str:
    """Generate a feel-profile slider widget."""
    pct = max(0, min(100, pct))
    return (
        f'<div class="paddle-slider">\n'
        f'  <div class="slider-labels">\n'
        f"    <span>{left_label}</span>\n"
        f"    <span>{right_label}</span>\n"
        f"  </div>\n"
        f'  <div class="slider-track">\n'
        f'    <div class="slider-dot" style="left: {pct}%;"></div>\n'
        f"  </div>\n"
        f"</div>\n"
        f"{_SLIDER_CSS}"
    )


def _build_toc_html(items: list) -> str:
    """Generate TOC ordered list HTML."""
    lis = "".join(
        f'<li><a href="#toc{i+1}">{item}</a></li>'
        for i, item in enumerate(items)
    )
    return f"<ol>{lis}</ol>"


def _build_discount_html(code: str, brand: str = "") -> str:
    prefix = f"Save with {brand} " if brand else "Save with code "
    return f"<p>{prefix}<strong>{code}</strong></p>"


def _build_specs_table(rows: list[tuple]) -> str:
    """Build a specs table from a list of (label, value) tuples."""
    trs = "".join(
        f"<tr><td><strong>{label}</strong></td><td>{value}</td></tr>"
        for label, value in rows
    )
    return (
        '<div style="overflow-x: auto; width: 100%;">'
        '<table style="width:100%; border-collapse:collapse;">'
        f"{trs}"
        "</table></div>"
    )


# ─── Template Engine ────────────────────────────────────────────────────────

def _load_template() -> list:
    """Load and return the base Elementor template as a Python list."""
    return json.loads(_TEMPLATE_PATH.read_text(encoding="utf-8"))


def _find_and_update(nodes: list, target_id: str, updater) -> bool:
    """Recursively find a widget by ID and call updater(settings) on it."""
    for node in nodes:
        if node.get("id") == target_id:
            updater(node["settings"])
            return True
        if _find_and_update(node.get("elements", []), target_id, updater):
            return True
    return False


def _set(data: list, widget_key: str, updater) -> None:
    """Find widget by logical key and apply updater. Warns if not found."""
    wid = _WID.get(widget_key)
    if not wid:
        log.warning(f"Unknown widget key: {widget_key}")
        return
    if not _find_and_update(data, wid, updater):
        log.warning(f"Widget not found in template: {widget_key} (id={wid})")


def build_elementor_data(content: ArticleContent) -> str:
    """Clone the review template and inject article content.

    Returns:
        JSON string ready to be pushed to _elementor_data post meta.
    """
    data = copy.deepcopy(_load_template())

    # ── Intro section ──────────────────────────────────────────────────
    _set(data, "intro_text", lambda s: s.update({"editor": content.intro_html}))

    _set(data, "shop_btn_top", lambda s: s.update({
        "text": content.shop_button_text,
        "link": {**s.get("link", {}), "url": content.shop_url, "is_external": "on"},
    }))

    if content.discount_code:
        code_html = _build_discount_html(content.discount_code)
        _set(data, "discount_top", lambda s: s.update({"editor": code_html}))

    _set(data, "toc_list", lambda s: s.update({
        "editor": _build_toc_html(content.toc_items)
    }))

    # ── Paddle Snapshot ────────────────────────────────────────────────
    _set(data, "section1_heading", lambda s: s.update({"title": content.section1_heading}))

    if content.paddle_image1_url:
        _set(data, "image1", lambda s: s.update({
            "image": {"url": content.paddle_image1_url, "id": content.paddle_image1_id},
            "caption": content.paddle_image1_caption,
            "link": {**s.get("link", {}), "url": content.shop_url},
        }))

    if content.paddle_image2_url:
        _set(data, "image2", lambda s: s.update({
            "image": {"url": content.paddle_image2_url, "id": content.paddle_image2_id},
            "caption": content.paddle_image2_caption,
            "link": {**s.get("link", {}), "url": content.shop_url},
        }))

    _set(data, "metrics_html", lambda s: s.update({
        "html": _build_metrics_html(content.metrics)
    }))

    _set(data, "slider_soft_stiff", lambda s: s.update({
        "html": _build_slider_html("Soft", "Stiff", content.feel_soft_stiff_pct)
    }))

    _set(data, "slider_dense_hollow", lambda s: s.update({
        "html": _build_slider_html("Dense", "Hollow", content.feel_dense_hollow_pct)
    }))

    if content.paddle_info_bullets_html:
        _set(data, "paddle_info_bullets", lambda s: s.update({
            "editor": content.paddle_info_bullets_html
        }))

    _set(data, "measurements_note", lambda s: s.update({
        "editor": f"<p>{content.measurements_note}</p>"
    }))

    if content.specs_table_html:
        _set(data, "specs_table", lambda s: s.update({"editor": content.specs_table_html}))

    # ── On-Court section ───────────────────────────────────────────────
    _set(data, "section2_heading", lambda s: s.update({"title": content.section2_heading}))

    if content.video_url:
        _set(data, "video", lambda s: s.update({
            "youtube_url": content.video_url,
            **({"image_overlay": {
                "url": content.video_thumbnail_url,
                "id": content.video_thumbnail_id,
            }} if content.video_thumbnail_url else {}),
        }))

    if content.section2_body_html:
        _set(data, "section2_body", lambda s: s.update({"editor": content.section2_body_html}))

    _set(data, "shop_btn_mid", lambda s: s.update({
        "text": content.shop_button_text,
        "link": {**s.get("link", {}), "url": content.shop_url, "is_external": "on"},
    }))

    if content.discount_code:
        code_html = _build_discount_html(content.discount_code)
        _set(data, "discount_mid", lambda s: s.update({"editor": code_html}))

    # ── Comparisons section ────────────────────────────────────────────
    _set(data, "section3_heading", lambda s: s.update({"title": content.section3_heading}))
    if content.section3_body_html:
        _set(data, "section3_body", lambda s: s.update({"editor": content.section3_body_html}))

    # ── Who It's Best For section ──────────────────────────────────────
    _set(data, "section4_heading", lambda s: s.update({"title": content.section4_heading}))
    if content.section4_body_html:
        _set(data, "section4_body", lambda s: s.update({"editor": content.section4_body_html}))

    # ── CTA banner ─────────────────────────────────────────────────────
    if content.cta_heading:
        _set(data, "cta_heading", lambda s: s.update({"title": content.cta_heading}))

    _set(data, "cta_btn", lambda s: s.update({
        "text": content.cta_button_text,
        "link": {**s.get("link", {}), "url": content.cta_url or content.shop_url, "is_external": "on"},
    }))

    # ── Reviewer profile ───────────────────────────────────────────────
    _set(data, "reviewer", lambda s: s.update({
        "testimonial_name": content.reviewer_name,
        "testimonial_job": content.reviewer_title,
        "testimonial_content": content.reviewer_bio_html,
        "testimonial_image": {
            "url": content.reviewer_image_url,
            "id": content.reviewer_image_id,
        },
    }))

    # ── Product sidebar ────────────────────────────────────────────────
    if content.product_image_url:
        _set(data, "product_image", lambda s: s.update({
            "image": {"url": content.product_image_url, "id": content.product_image_id},
        }))

    if content.product_name:
        _set(data, "product_name", lambda s: s.update({"title": content.product_name}))

    _set(data, "sidebar_btn", lambda s: s.update({
        "text": content.sidebar_button_text,
        "link": {**s.get("link", {}), "url": content.sidebar_url or content.shop_url, "is_external": "on"},
    }))

    if content.sidebar_code_html:
        _set(data, "sidebar_code", lambda s: s.update({"editor": content.sidebar_code_html}))

    log.info(f"Elementor template built for: {content.title}")
    return json.dumps(data, ensure_ascii=False)
