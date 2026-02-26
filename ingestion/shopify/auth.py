"""Shopify authentication.

Supports both legacy custom app tokens (shpat_) and Client Credentials Grant.
If SHOPIFY_ACCESS_TOKEN is set in .env, uses it directly (no expiry).
Otherwise falls back to Client Credentials Grant (24h tokens).
"""

import os
import requests
from ingestion.utils.config import SHOPIFY_SHOP_DOMAIN, SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET
from ingestion.utils.logger import get_logger

log = get_logger(__name__)

_cached_token: str | None = None


def get_access_token() -> str:
    """Get an access token — from .env if available, otherwise via Client Credentials Grant."""
    global _cached_token

    # Check for a static token first (legacy custom app)
    static_token = os.environ.get("SHOPIFY_ACCESS_TOKEN", "").strip()
    if static_token:
        _cached_token = static_token
        log.info("Using static Shopify access token from .env")
        return _cached_token

    # Fall back to Client Credentials Grant
    url = f"https://{SHOPIFY_SHOP_DOMAIN}/admin/oauth/access_token"
    payload = {
        "client_id": SHOPIFY_CLIENT_ID,
        "client_secret": SHOPIFY_CLIENT_SECRET,
        "grant_type": "client_credentials",
    }

    resp = requests.post(url, data=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    _cached_token = data["access_token"]
    scopes = data.get("scope", "")
    log.info(f"Shopify token acquired via Client Credentials. Scopes: {scopes}")
    return _cached_token


def get_headers() -> dict:
    """Return auth headers for Shopify API calls."""
    token = _cached_token or get_access_token()
    return {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
    }


def base_url() -> str:
    """Return the Shopify Admin REST API base URL."""
    from ingestion.utils.config import SHOPIFY_API_VERSION
    return f"https://{SHOPIFY_SHOP_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}"
