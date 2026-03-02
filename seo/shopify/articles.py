"""Create blog articles on Shopify shop via Admin REST API."""

from datetime import datetime, timezone

import requests

from ingestion.utils.config import SHOPIFY_SHOP_DOMAIN, SHOPIFY_API_VERSION
from ingestion.shopify.auth import get_headers
from ingestion.utils.bq_client import load_rows
from ingestion.utils.logger import get_logger
from ingestion import schemas

log = get_logger(__name__)

_blog_id: int | None = None


def get_blog_id() -> int:
    """Return the first blog's ID from the Shopify shop (cached)."""
    global _blog_id
    if _blog_id is not None:
        return _blog_id

    base_url = f"https://{SHOPIFY_SHOP_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}"
    headers = get_headers()

    resp = requests.get(f"{base_url}/blogs.json", headers=headers, timeout=15)
    resp.raise_for_status()

    blogs = resp.json().get("blogs", [])
    if not blogs:
        raise RuntimeError("No blogs found on Shopify shop. Create one first.")

    _blog_id = blogs[0]["id"]
    log.info(f"Using Shopify blog: '{blogs[0].get('title', '')}' (ID: {_blog_id})")
    return _blog_id


def create_blog_article(
    title: str,
    body_html: str,
    tags: str = "",
    target_keyword: str = "",
    content_type: str = "",
    published: bool = False,
) -> dict:
    """Create a blog article on Shopify shop.

    Args:
        title: Article title.
        body_html: Article content as HTML.
        tags: Comma-separated tags.
        target_keyword: SEO target keyword for tracking.
        content_type: Content type (review, comparison, how_to).
        published: Publish immediately (default: unpublished for review).

    Returns:
        Shopify API response dict for the article.
    """
    blog_id = get_blog_id()
    base_url = f"https://{SHOPIFY_SHOP_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}"
    headers = get_headers()

    payload = {
        "article": {
            "title": title,
            "body_html": body_html,
            "published": published,
        }
    }
    if tags:
        payload["article"]["tags"] = tags

    resp = requests.post(
        f"{base_url}/blogs/{blog_id}/articles.json",
        headers=headers,
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()

    article = resp.json().get("article", {})
    log.info(f"Created Shopify blog article: '{title}' (ID: {article.get('id')}, "
             f"published={published})")

    # Track in BigQuery
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    shop_domain = SHOPIFY_SHOP_DOMAIN.replace(".myshopify.com", "")
    article_handle = article.get("handle", "")
    blog_handle = article.get("blog_id", blog_id)
    article_url = f"https://{shop_domain}.com/blogs/news/{article_handle}"

    bq_row = {
        "post_id": f"shopify_blog_{article.get('id', '')}",
        "platform": "shopify_blog",
        "title": title,
        "target_keyword": target_keyword,
        "content_type": content_type,
        "status": "published" if published else "draft",
        "url": article_url,
        "word_count": len(body_html.split()),
        "publish_date": article.get("published_at", "")[:10] if published else None,
        "created_at": now_str,
        "updated_at": now_str,
    }

    load_rows("content_posts", [bq_row], schemas.CONTENT_POSTS)
    log.info(f"Tracked in BigQuery: {bq_row['post_id']}")

    return article
