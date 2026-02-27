"""Publish draft content to WordPress as draft posts for human review."""

from datetime import datetime, timezone

import requests

from seo.wordpress.auth import get_base_url, get_headers
from ingestion.utils.bq_client import load_rows
from ingestion.utils.logger import get_logger
from ingestion import schemas

log = get_logger(__name__)


def create_draft_post(
    title: str,
    content_html: str,
    excerpt: str = "",
    slug: str = "",
    categories: list[int] | None = None,
    tags: list[int] | None = None,
    seo_title: str = "",
    meta_description: str = "",
) -> dict:
    """Create a draft post in WordPress.

    Args:
        title: Post title.
        content_html: Full post content as HTML.
        excerpt: Post excerpt (used as meta description fallback).
        slug: URL slug.
        categories: List of category IDs.
        tags: List of tag IDs.
        seo_title: SEO title for Yoast/RankMath.
        meta_description: Meta description for SEO plugins.

    Returns:
        WordPress API response dict.
    """
    base_url = get_base_url()
    headers = get_headers()

    payload = {
        "title": title,
        "content": content_html,
        "status": "draft",
    }

    if excerpt:
        payload["excerpt"] = excerpt
    if slug:
        payload["slug"] = slug
    if categories:
        payload["categories"] = categories
    if tags:
        payload["tags"] = tags

    # Yoast SEO meta fields (if Yoast is installed)
    meta = {}
    if seo_title:
        meta["_yoast_wpseo_title"] = seo_title
    if meta_description:
        meta["_yoast_wpseo_metadesc"] = meta_description
    if meta:
        payload["meta"] = meta

    resp = requests.post(
        f"{base_url}/posts",
        headers=headers,
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()

    post = resp.json()
    log.info(f"Created WordPress draft: '{title}' (ID: {post['id']})")
    return post


def publish_draft(
    title: str,
    content_html: str,
    target_keyword: str = "",
    content_type: str = "",
    slug: str = "",
    meta_description: str = "",
    word_count: int = 0,
) -> dict:
    """Create a draft post and track it in BigQuery.

    Args:
        title: Post title.
        content_html: Full post content as HTML.
        target_keyword: SEO target keyword.
        content_type: Content type (review, comparison, etc.).
        slug: URL slug.
        meta_description: Meta description.
        word_count: Word count of content.

    Returns:
        WordPress API response dict.
    """
    # Create the WordPress draft
    post = create_draft_post(
        title=title,
        content_html=content_html,
        excerpt=meta_description,
        slug=slug,
        seo_title=title,
        meta_description=meta_description,
    )

    # Track in BigQuery
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    bq_row = {
        "post_id": f"wp_{post['id']}",
        "platform": "wordpress",
        "title": title,
        "target_keyword": target_keyword,
        "content_type": content_type,
        "status": "draft",
        "url": post.get("link", ""),
        "word_count": word_count,
        "publish_date": None,
        "created_at": now_str,
        "updated_at": now_str,
    }

    load_rows("content_posts", [bq_row], schemas.CONTENT_POSTS)
    log.info(f"Tracked in BigQuery: {bq_row['post_id']}")

    return post


def get_categories() -> list[dict]:
    """Fetch WordPress categories."""
    base_url = get_base_url()
    headers = get_headers()
    resp = requests.get(
        f"{base_url}/categories?per_page=100",
        headers=headers,
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def get_tags() -> list[dict]:
    """Fetch WordPress tags."""
    base_url = get_base_url()
    headers = get_headers()
    resp = requests.get(
        f"{base_url}/tags?per_page=100",
        headers=headers,
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()
