"""Pull existing Shopify blog articles for inventory tracking."""

from datetime import datetime, timezone

import requests

from ingestion.utils.config import SHOPIFY_SHOP_DOMAIN, SHOPIFY_API_VERSION
from ingestion.shopify.auth import get_headers
from ingestion.utils.bq_client import load_rows
from ingestion.utils.logger import get_logger
from ingestion import schemas
from seo.shopify.articles import get_blog_id

log = get_logger(__name__)


def pull_articles(per_page: int = 250) -> list[dict]:
    """Fetch all blog articles from Shopify shop.

    Returns:
        List of dicts matching CONTENT_POSTS schema.
    """
    blog_id = get_blog_id()
    base_url = f"https://{SHOPIFY_SHOP_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}"
    headers = get_headers()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    shop_domain = SHOPIFY_SHOP_DOMAIN.replace(".myshopify.com", "")

    all_articles = []
    params = {"limit": per_page}

    while True:
        resp = requests.get(
            f"{base_url}/blogs/{blog_id}/articles.json",
            headers=headers,
            params=params,
            timeout=30,
        )
        resp.raise_for_status()

        articles = resp.json().get("articles", [])
        if not articles:
            break

        for art in articles:
            body = art.get("body_html", "") or ""
            word_count = len(body.split()) if body else 0
            pub_date = art.get("published_at", "")[:10] if art.get("published_at") else None
            handle = art.get("handle", "")
            article_url = f"https://{shop_domain}.com/blogs/news/{handle}"

            all_articles.append({
                "post_id": f"shopify_blog_{art['id']}",
                "platform": "shopify_blog",
                "title": art.get("title", ""),
                "target_keyword": "",
                "content_type": "",
                "status": "published" if art.get("published_at") else "draft",
                "url": article_url,
                "word_count": word_count,
                "publish_date": pub_date,
                "created_at": now_str,
                "updated_at": now_str,
            })

        log.info(f"Fetched {len(articles)} Shopify blog articles")

        # Pagination via Link header
        link_header = resp.headers.get("Link", "")
        if 'rel="next"' in link_header:
            # Extract page_info from Link header
            import re
            match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
            if match:
                next_url = match.group(1)
                # Extract page_info param
                from urllib.parse import urlparse, parse_qs
                parsed = urlparse(next_url)
                qs = parse_qs(parsed.query)
                if "page_info" in qs:
                    params = {"limit": per_page, "page_info": qs["page_info"][0]}
                    continue
        break

    log.info(f"Total Shopify blog articles fetched: {len(all_articles)}")
    return all_articles


def sync_inventory():
    """Pull Shopify blog articles and append to BigQuery content_posts."""
    log.info("Syncing Shopify blog article inventory")
    articles = pull_articles()

    if articles:
        load_rows("content_posts", articles, schemas.CONTENT_POSTS)
        log.info(f"Loaded {len(articles)} Shopify blog articles into content_posts")
    else:
        log.info("No Shopify blog articles found")

    return articles
