"""Create product-specific landing pages on Shopify shop for Google Ads campaigns."""

from datetime import datetime, timezone

import requests

from ingestion.utils.config import (
    SHOPIFY_SHOP_DOMAIN,
    SHOPIFY_API_VERSION,
)
from ingestion.shopify.auth import get_headers
from ingestion.utils.bq_client import load_rows, run_query
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.logger import get_logger
from ingestion import schemas

log = get_logger(__name__)

_DS = f"{GCP_PROJECT_ID}.{BQ_DATASET}"


def _get_product_data(product_handle: str | None = None) -> list[dict]:
    """Load product data from BigQuery for landing page generation."""
    if product_handle:
        sql = f"""
        SELECT p.product_id, p.title, p.handle, p.product_type, p.tags,
               v.price, v.compare_at_price, v.sku
        FROM `{_DS}.shopify_products` p
        LEFT JOIN `{_DS}.shopify_product_variants` v ON p.product_id = v.product_id
        WHERE LOWER(p.handle) LIKE '%{product_handle.lower()}%'
            OR LOWER(p.title) LIKE '%{product_handle.lower()}%'
        LIMIT 10
        """
    else:
        sql = f"""
        SELECT p.product_id, p.title, p.handle, p.product_type, p.tags,
               v.price, v.compare_at_price, v.sku
        FROM `{_DS}.shopify_products` p
        LEFT JOIN `{_DS}.shopify_product_variants` v ON p.product_id = v.product_id
        WHERE p.status = 'ACTIVE'
        LIMIT 50
        """
    return [dict(r) for r in run_query(sql)]


def create_landing_page(
    title: str,
    body_html: str,
    target_keyword: str = "",
    published: bool = False,
) -> dict:
    """Create a page on Shopify shop.

    Args:
        title: Page title.
        body_html: Page content as HTML.
        target_keyword: SEO target keyword for tracking.
        published: Whether to publish immediately (default: unpublished for review).

    Returns:
        Shopify API response dict.
    """
    base_url = f"https://{SHOPIFY_SHOP_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}"
    headers = get_headers()

    payload = {
        "page": {
            "title": title,
            "body_html": body_html,
            "published": published,
        }
    }

    resp = requests.post(
        f"{base_url}/pages.json",
        headers=headers,
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()

    page = resp.json().get("page", {})
    log.info(f"Created Shopify page: '{title}' (ID: {page.get('id')}, published={published})")

    # Track in BigQuery
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    shop_url = SHOPIFY_SHOP_DOMAIN.replace(".myshopify.com", "")
    page_url = f"https://{shop_url}.com/pages/{page.get('handle', '')}"

    bq_row = {
        "post_id": f"shopify_{page.get('id', '')}",
        "platform": "shopify",
        "title": title,
        "target_keyword": target_keyword,
        "content_type": "landing_page",
        "status": "published" if published else "draft",
        "url": page_url,
        "word_count": len(body_html.split()),
        "publish_date": page.get("published_at", "")[:10] if published else None,
        "created_at": now_str,
        "updated_at": now_str,
    }

    load_rows("content_posts", [bq_row], schemas.CONTENT_POSTS)
    log.info(f"Tracked in BigQuery: {bq_row['post_id']}")

    return page


def get_product_context(product_handle: str) -> str:
    """Get product data formatted for content generation."""
    products = _get_product_data(product_handle)
    if not products:
        return f"Product '{product_handle}' not found in BigQuery."

    lines = []
    for p in products:
        lines.append(
            f"- {p.get('title', '')} ({p.get('handle', '')}): "
            f"${p.get('price', 0)}, type={p.get('product_type', '')}"
        )
    return "\n".join(lines)
