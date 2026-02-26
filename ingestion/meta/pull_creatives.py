"""Pull Meta Ads creative text (headline, primary text, CTA) for copy analysis."""

from datetime import datetime, timezone

import requests

from ingestion.meta.auth import api_url
from ingestion.meta.pull_campaigns import _to_bq_timestamp
from ingestion.utils.config import META_ACCESS_TOKEN, META_ADS_ACCOUNT_ID
from ingestion.utils.logger import get_logger

log = get_logger(__name__)

_FIELDS_FULL = (
    "id,name,title,body,call_to_action_type,"
    "object_story_spec,thumbnail_url,image_url,object_type"
)
_FIELDS_BASIC = "id,name,title,body,call_to_action_type,thumbnail_url,image_url,object_type"


def _paginate_creatives(url: str, params: dict) -> list[dict]:
    """Paginate with smaller pages and fallback for 500 errors on certain creatives."""
    all_data = []
    while url:
        resp = requests.get(url, params=params, timeout=60)
        if resp.status_code == 500 and params and "object_story_spec" in params.get("fields", ""):
            log.warning("500 error with object_story_spec, retrying without it")
            params["fields"] = _FIELDS_BASIC
            resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        all_data.extend(data.get("data", []))
        paging = data.get("paging", {})
        url = paging.get("next")
        params = None
    return all_data


def _extract_creative_text(creative: dict) -> dict:
    """Extract title/body/CTA from object_story_spec, falling back to top-level fields."""
    title = creative.get("title")
    body = creative.get("body")
    link_description = None
    cta_type = None
    video_id = None
    image_url = creative.get("image_url")
    page_id = None
    instagram_actor_id = None

    oss = creative.get("object_story_spec", {})
    if oss:
        page_id = oss.get("page_id")
        instagram_actor_id = oss.get("instagram_actor_id")

        # Video ads (most common for Pickleball Effect)
        video_data = oss.get("video_data", {})
        if video_data:
            body = body or video_data.get("message")
            title = title or video_data.get("title")
            link_description = video_data.get("link_description")
            video_id = video_data.get("video_id")
            image_url = image_url or video_data.get("image_url")
            cta = video_data.get("call_to_action", {})
            cta_type = cta.get("type") if cta else None

        # Link/carousel ads
        link_data = oss.get("link_data", {})
        if link_data and not body:
            body = body or link_data.get("message")
            title = title or link_data.get("name")
            link_description = link_description or link_data.get("description")
            image_url = image_url or link_data.get("image_url") or link_data.get("picture")
            cta = link_data.get("call_to_action", {})
            cta_type = cta_type or (cta.get("type") if cta else None)

        # Photo ads
        photo_data = oss.get("photo_data", {})
        if photo_data and not body:
            body = body or photo_data.get("message")
            image_url = image_url or photo_data.get("image_url")

    # Fall back to call_to_action_type top-level field
    cta_type = cta_type or creative.get("call_to_action_type")

    return {
        "title": title,
        "body": body,
        "link_description": link_description,
        "cta_type": cta_type,
        "video_id": video_id,
        "image_url": image_url,
        "page_id": page_id,
        "instagram_actor_id": instagram_actor_id,
    }


def pull_creatives(ads: list[dict]) -> list[dict]:
    """Fetch ad creatives and link them to ads via creative_id.

    Args:
        ads: List of ad rows (from pull_ads) containing ad_id and creative_id.

    Returns:
        List of creative rows ready for BigQuery.
    """
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Build creative_id -> ad mapping for linking
    creative_to_ad = {}
    for ad in ads:
        cid = ad.get("creative_id")
        if cid:
            creative_to_ad[cid] = {
                "ad_id": ad["ad_id"],
                "ad_name": ad.get("ad_name"),
            }

    if not creative_to_ad:
        log.info("No creative IDs found in ads")
        return []

    url = api_url(f"act_{META_ADS_ACCOUNT_ID}/adcreatives")
    params = {
        "access_token": META_ACCESS_TOKEN,
        "fields": _FIELDS_FULL,
        "limit": 50,
    }
    raw = _paginate_creatives(url, params)

    rows = []
    for c in raw:
        creative_id = c["id"]
        ad_info = creative_to_ad.get(creative_id, {})
        text = _extract_creative_text(c)

        rows.append({
            "creative_id": creative_id,
            "ad_id": ad_info.get("ad_id"),
            "ad_name": ad_info.get("ad_name"),
            "title": text["title"],
            "body": text["body"],
            "link_description": text["link_description"],
            "cta_type": text["cta_type"],
            "image_url": text["image_url"],
            "video_id": text["video_id"],
            "thumbnail_url": c.get("thumbnail_url"),
            "object_type": c.get("object_type"),
            "page_id": text["page_id"],
            "instagram_actor_id": text["instagram_actor_id"],
            "created_time": _to_bq_timestamp(c.get("created_time")),
            "ingested_at": now_str,
        })

    log.info(f"Fetched {len(rows)} creatives ({len(creative_to_ad)} linked to ads)")
    return rows
