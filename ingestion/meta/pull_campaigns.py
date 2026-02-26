"""Pull Meta Ads campaign, adset, and ad metadata."""

from datetime import datetime, timezone
from dateutil import parser as dtparser
import requests

from ingestion.meta.auth import api_url
from ingestion.utils.config import META_ACCESS_TOKEN, META_ADS_ACCOUNT_ID
from ingestion.utils.logger import get_logger

log = get_logger(__name__)


def _paginate(url: str, params: dict) -> list[dict]:
    """Handle Meta API cursor pagination."""
    all_data = []
    while url:
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        all_data.extend(data.get("data", []))
        paging = data.get("paging", {})
        url = paging.get("next")
        params = None  # next URL has params baked in
    return all_data


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _to_bq_timestamp(val: str | None) -> str | None:
    """Convert Meta's ISO timestamp (e.g. 2025-06-23T09:19:47-0600) to BQ format."""
    if not val:
        return None
    try:
        dt = dtparser.isoparse(val).astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def pull_campaigns() -> list[dict]:
    """Fetch all campaigns for the ad account."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    url = api_url(f"act_{META_ADS_ACCOUNT_ID}/campaigns")
    params = {
        "access_token": META_ACCESS_TOKEN,
        "fields": "id,name,objective,status,daily_budget,lifetime_budget,created_time,updated_time",
        "limit": 500,
    }
    raw = _paginate(url, params)

    rows = []
    for c in raw:
        rows.append({
            "campaign_id": c["id"],
            "campaign_name": c.get("name"),
            "objective": c.get("objective"),
            "status": c.get("status"),
            "daily_budget": _safe_float(c.get("daily_budget")),
            "lifetime_budget": _safe_float(c.get("lifetime_budget")),
            "created_time": _to_bq_timestamp(c.get("created_time")),
            "updated_time": _to_bq_timestamp(c.get("updated_time")),
            "ingested_at": now_str,
        })

    log.info(f"Fetched {len(rows)} campaigns")
    return rows


def pull_adsets() -> list[dict]:
    """Fetch all adsets for the ad account."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    url = api_url(f"act_{META_ADS_ACCOUNT_ID}/adsets")
    params = {
        "access_token": META_ACCESS_TOKEN,
        "fields": "id,name,campaign_id,status,daily_budget,lifetime_budget,"
                  "targeting,optimization_goal,billing_event,created_time,updated_time",
        "limit": 500,
    }
    raw = _paginate(url, params)

    rows = []
    for a in raw:
        targeting = a.get("targeting", {})
        targeting_summary = str(targeting.get("geo_locations", ""))[:1000] if targeting else None

        rows.append({
            "adset_id": a["id"],
            "adset_name": a.get("name"),
            "campaign_id": a.get("campaign_id"),
            "status": a.get("status"),
            "daily_budget": _safe_float(a.get("daily_budget")),
            "lifetime_budget": _safe_float(a.get("lifetime_budget")),
            "targeting_summary": targeting_summary,
            "optimization_goal": a.get("optimization_goal"),
            "billing_event": a.get("billing_event"),
            "created_time": _to_bq_timestamp(a.get("created_time")),
            "updated_time": _to_bq_timestamp(a.get("updated_time")),
            "ingested_at": now_str,
        })

    log.info(f"Fetched {len(rows)} adsets")
    return rows


def pull_ads() -> list[dict]:
    """Fetch all ads for the ad account."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    url = api_url(f"act_{META_ADS_ACCOUNT_ID}/ads")
    params = {
        "access_token": META_ACCESS_TOKEN,
        "fields": "id,name,adset_id,campaign_id,status,creative{id},created_time,updated_time",
        "limit": 500,
    }
    raw = _paginate(url, params)

    rows = []
    for a in raw:
        creative = a.get("creative", {})
        rows.append({
            "ad_id": a["id"],
            "ad_name": a.get("name"),
            "adset_id": a.get("adset_id"),
            "campaign_id": a.get("campaign_id"),
            "status": a.get("status"),
            "creative_id": creative.get("id"),
            "created_time": _to_bq_timestamp(a.get("created_time")),
            "updated_time": _to_bq_timestamp(a.get("updated_time")),
            "ingested_at": now_str,
        })

    log.info(f"Fetched {len(rows)} ads")
    return rows
