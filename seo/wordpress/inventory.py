"""Pull existing WordPress content inventory for internal linking and dedup."""

from datetime import datetime, timezone

import requests

from seo.wordpress.auth import get_base_url, get_headers
from ingestion.utils.bq_client import full_replace
from ingestion.utils.logger import get_logger
from ingestion import schemas

log = get_logger(__name__)


def pull_posts(per_page: int = 100) -> list[dict]:
    """Fetch all published posts from WordPress.

    Returns:
        List of dicts matching CONTENT_POSTS schema (for BQ loading).
    """
    base_url = get_base_url()
    headers = get_headers()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    all_posts = []
    page = 1

    while True:
        url = f"{base_url}/posts?per_page={per_page}&page={page}&status=publish,draft"
        resp = requests.get(url, headers=headers, timeout=30)

        if resp.status_code == 400:
            break  # No more pages
        resp.raise_for_status()

        posts = resp.json()
        if not posts:
            break

        for post in posts:
            rendered_content = post.get("content", {}).get("rendered", "")
            word_count = len(rendered_content.split()) if rendered_content else 0

            pub_date = post.get("date", "")[:10] if post.get("date") else None

            all_posts.append({
                "post_id": f"wp_{post['id']}",
                "platform": "wordpress",
                "title": post.get("title", {}).get("rendered", ""),
                "target_keyword": "",  # Not available from WP API
                "content_type": "",    # Would need manual classification
                "status": "published" if post.get("status") == "publish" else "draft",
                "url": post.get("link", ""),
                "word_count": word_count,
                "publish_date": pub_date,
                "created_at": now_str,
                "updated_at": now_str,
            })

        log.info(f"Fetched page {page}: {len(posts)} posts")

        # Check total pages from headers
        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        if page >= total_pages:
            break
        page += 1

    log.info(f"Total WordPress posts fetched: {len(all_posts)}")
    return all_posts


def pull_pages(per_page: int = 100) -> list[dict]:
    """Fetch all pages from WordPress."""
    base_url = get_base_url()
    headers = get_headers()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    all_pages = []
    page_num = 1

    while True:
        url = f"{base_url}/pages?per_page={per_page}&page={page_num}&status=publish,draft"
        resp = requests.get(url, headers=headers, timeout=30)

        if resp.status_code == 400:
            break
        resp.raise_for_status()

        pages = resp.json()
        if not pages:
            break

        for pg in pages:
            rendered_content = pg.get("content", {}).get("rendered", "")
            word_count = len(rendered_content.split()) if rendered_content else 0
            pub_date = pg.get("date", "")[:10] if pg.get("date") else None

            all_pages.append({
                "post_id": f"wp_page_{pg['id']}",
                "platform": "wordpress",
                "title": pg.get("title", {}).get("rendered", ""),
                "target_keyword": "",
                "content_type": "page",
                "status": "published" if pg.get("status") == "publish" else "draft",
                "url": pg.get("link", ""),
                "word_count": word_count,
                "publish_date": pub_date,
                "created_at": now_str,
                "updated_at": now_str,
            })

        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        if page_num >= total_pages:
            break
        page_num += 1

    log.info(f"Total WordPress pages fetched: {len(all_pages)}")
    return all_pages


def sync_inventory():
    """Pull all WordPress content and load into BigQuery content_posts."""
    log.info("Syncing WordPress content inventory")
    posts = pull_posts()
    pages = pull_pages()
    all_content = posts + pages

    if all_content:
        full_replace("content_posts", all_content, schemas.CONTENT_POSTS)
        log.info(f"Loaded {len(all_content)} WordPress items into content_posts")
    else:
        log.warning("No WordPress content found")

    return all_content
