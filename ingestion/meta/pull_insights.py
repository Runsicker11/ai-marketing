"""Pull Meta Ads daily ad-level insights with actions parsing."""

from datetime import date, datetime, timedelta, timezone
import requests

from ingestion.meta.auth import api_url
from ingestion.utils.config import META_ACCESS_TOKEN, META_ADS_ACCOUNT_ID
from ingestion.utils.logger import get_logger

log = get_logger(__name__)

# Meta uses multiple action_type names for the same event
ACTION_TYPE_MAP = {
    "purchases": [
        "omni_purchase", "purchase", "offsite_conversion.fb_pixel_purchase",
    ],
    "purchase_value": [
        "omni_purchase", "purchase", "offsite_conversion.fb_pixel_purchase",
    ],
    "add_to_cart": [
        "omni_add_to_cart", "add_to_cart", "offsite_conversion.fb_pixel_add_to_cart",
    ],
    "add_to_cart_value": [
        "omni_add_to_cart", "add_to_cart", "offsite_conversion.fb_pixel_add_to_cart",
    ],
    "initiate_checkout": [
        "omni_initiated_checkout", "initiated_checkout",
        "offsite_conversion.fb_pixel_initiate_checkout",
    ],
    "initiate_checkout_value": [
        "omni_initiated_checkout", "initiated_checkout",
        "offsite_conversion.fb_pixel_initiate_checkout",
    ],
    "landing_page_views": ["landing_page_view"],
    "link_clicks": ["link_click"],
}


def _extract_actions(actions: list[dict] | None, action_values: list[dict] | None) -> dict:
    """Parse Meta's nested actions/action_values into flat columns."""
    result = {}

    # Count-based actions
    actions_lookup = {}
    for a in (actions or []):
        actions_lookup[a["action_type"]] = int(float(a["value"]))

    # Value-based actions
    values_lookup = {}
    for av in (action_values or []):
        values_lookup[av["action_type"]] = float(av["value"])

    for col, type_names in ACTION_TYPE_MAP.items():
        is_value = col.endswith("_value")
        lookup = values_lookup if is_value else actions_lookup
        val = None
        for tn in type_names:
            if tn in lookup:
                val = lookup[tn]
                break
        result[col] = val

    return result


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def pull_insights(start_date: date, end_date: date) -> list[dict]:
    """Fetch daily ad-level insights for a date range."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    url = api_url(f"act_{META_ADS_ACCOUNT_ID}/insights")
    params = {
        "access_token": META_ACCESS_TOKEN,
        "fields": "date_start,campaign_id,campaign_name,adset_id,adset_name,"
                  "ad_id,ad_name,impressions,clicks,spend,cpc,cpm,ctr,"
                  "reach,frequency,actions,action_values",
        "level": "ad",
        "time_increment": 1,  # daily
        "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}',
        "limit": 500,
    }

    all_rows = []
    page_url = url

    while page_url:
        resp = requests.get(page_url, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()

        for row in data.get("data", []):
            actions_parsed = _extract_actions(
                row.get("actions"),
                row.get("action_values"),
            )

            insight_row = {
                "date_start": row["date_start"],
                "campaign_id": row.get("campaign_id"),
                "campaign_name": row.get("campaign_name"),
                "adset_id": row.get("adset_id"),
                "adset_name": row.get("adset_name"),
                "ad_id": row.get("ad_id"),
                "ad_name": row.get("ad_name"),
                "impressions": _safe_int(row.get("impressions")),
                "clicks": _safe_int(row.get("clicks")),
                "spend": _safe_float(row.get("spend")),
                "cpc": _safe_float(row.get("cpc")),
                "cpm": _safe_float(row.get("cpm")),
                "ctr": _safe_float(row.get("ctr")),
                "reach": _safe_int(row.get("reach")),
                "frequency": _safe_float(row.get("frequency")),
                **actions_parsed,
                "ingested_at": now_str,
            }
            all_rows.append(insight_row)

        paging = data.get("paging", {})
        page_url = paging.get("next")
        params = None  # next URL has params baked in

    log.info(f"Fetched {len(all_rows)} insight rows ({start_date} to {end_date})")
    return all_rows
