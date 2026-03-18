"""Google Search Console API authentication via OAuth2.

Supports per-site credentials: the review site and shop site use different
Google accounts, so each gets its own refresh token.
"""

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from ingestion.utils.config import (
    GOOGLE_SEARCH_CONSOLE_CLIENT_ID,
    GOOGLE_SEARCH_CONSOLE_CLIENT_SECRET,
    GOOGLE_SEARCH_CONSOLE_REFRESH_TOKEN,
    GOOGLE_SEARCH_CONSOLE_REFRESH_TOKEN_SHOP,
    GOOGLE_SEARCH_CONSOLE_SITE_URL,
    GOOGLE_SEARCH_CONSOLE_SITE_URL_SHOP,
)
from ingestion.utils.logger import get_logger

log = get_logger(__name__)

# Cache one service per refresh token to avoid re-auth
_services: dict[str, object] = {}


def _get_refresh_token(site_url: str) -> str:
    """Return the refresh token for a given site URL."""
    if site_url == GOOGLE_SEARCH_CONSOLE_SITE_URL_SHOP:
        return GOOGLE_SEARCH_CONSOLE_REFRESH_TOKEN_SHOP
    return GOOGLE_SEARCH_CONSOLE_REFRESH_TOKEN


def get_service(site_url: str | None = None):
    """Return a cached Search Console API service for the given site.

    Args:
        site_url: The Search Console site URL. If None, uses the review site token.
    """
    refresh_token = _get_refresh_token(site_url) if site_url else GOOGLE_SEARCH_CONSOLE_REFRESH_TOKEN

    if refresh_token in _services:
        return _services[refresh_token]

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_SEARCH_CONSOLE_CLIENT_ID,
        client_secret=GOOGLE_SEARCH_CONSOLE_CLIENT_SECRET,
    )

    service = build("searchconsole", "v1", credentials=creds)
    _services[refresh_token] = service
    return service


def get_site_url() -> str:
    """Return the primary configured Search Console site URL."""
    return GOOGLE_SEARCH_CONSOLE_SITE_URL


def get_site_urls() -> list[str]:
    """Return all configured Search Console site URLs.

    Reads GOOGLE_SEARCH_CONSOLE_SITE_URL (review site) and
    GOOGLE_SEARCH_CONSOLE_SITE_URL_SHOP (shop site). Returns only
    non-empty values.
    """
    urls = []
    if GOOGLE_SEARCH_CONSOLE_SITE_URL:
        urls.append(GOOGLE_SEARCH_CONSOLE_SITE_URL)
    if GOOGLE_SEARCH_CONSOLE_SITE_URL_SHOP:
        urls.append(GOOGLE_SEARCH_CONSOLE_SITE_URL_SHOP)
    return urls


def validate_access():
    """Verify credentials by listing available sites and checking all configured URLs."""
    for site_url in get_site_urls():
        service = get_service(site_url)
        sites = service.sites().list().execute()
        site_entries = sites.get("siteEntry", [])
        available = [s.get("siteUrl") for s in site_entries]

        found = False
        for site in site_entries:
            if site.get("siteUrl") == site_url:
                log.info(
                    f"Search Console access verified: {site_url} "
                    f"(permission: {site.get('permissionLevel', 'unknown')})"
                )
                found = True
                break

        if not found:
            raise RuntimeError(
                f"Site {site_url} not found in Search Console. "
                f"Available sites: {available}"
            )
