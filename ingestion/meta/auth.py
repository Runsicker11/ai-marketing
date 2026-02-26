"""Meta Ads token validation and expiry warning.

Long-lived tokens last ~60 days. This module checks validity and warns
when the token is near expiration so it can be refreshed.
"""

from datetime import datetime, timezone
import requests

from ingestion.utils.config import META_ACCESS_TOKEN, META_APP_ID, META_APP_SECRET, META_API_VERSION
from ingestion.utils.logger import get_logger

log = get_logger(__name__)


def validate_token() -> dict:
    """Check token validity and return debug info including expiry."""
    url = f"https://graph.facebook.com/{META_API_VERSION}/debug_token"
    app_token = f"{META_APP_ID}|{META_APP_SECRET}"
    params = {
        "input_token": META_ACCESS_TOKEN,
        "access_token": app_token,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json().get("data", {})

    is_valid = data.get("is_valid", False)
    expires_at = data.get("expires_at", 0)
    scopes = data.get("scopes", [])

    if not is_valid:
        error_msg = data.get("error", {}).get("message", "unknown reason")
        raise RuntimeError(
            f"Meta access token is invalid: {error_msg}\n"
            "To fix: Go to https://developers.facebook.com/tools/explorer/ → "
            "select your app → generate a new User Token with ads_read + ads_management scopes → "
            "exchange for a long-lived token → update META_ACCESS_TOKEN in .env"
        )

    if expires_at == 0:
        log.info("Meta token is valid (never expires)")
        days_remaining = None
    else:
        expiry_dt = datetime.fromtimestamp(expires_at, tz=timezone.utc)
        days_remaining = (expiry_dt - datetime.now(timezone.utc)).days
        log.info(f"Meta token valid. Expires: {expiry_dt.date()} ({days_remaining} days remaining)")

        if days_remaining < 7:
            log.warning(f"Meta token expires in {days_remaining} days! Refresh it soon.")

    log.info(f"Token scopes: {', '.join(scopes)}")
    return {"is_valid": is_valid, "days_remaining": days_remaining, "scopes": scopes}


def get_headers() -> dict:
    return {"Authorization": f"Bearer {META_ACCESS_TOKEN}"}


def api_url(path: str) -> str:
    return f"https://graph.facebook.com/{META_API_VERSION}/{path}"
