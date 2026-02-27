"""Google Search Console API authentication via OAuth2."""

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from ingestion.utils.config import (
    GOOGLE_SEARCH_CONSOLE_CLIENT_ID,
    GOOGLE_SEARCH_CONSOLE_CLIENT_SECRET,
    GOOGLE_SEARCH_CONSOLE_REFRESH_TOKEN,
    GOOGLE_SEARCH_CONSOLE_SITE_URL,
)
from ingestion.utils.logger import get_logger

log = get_logger(__name__)

_service = None


def get_service():
    """Return a cached Search Console API service."""
    global _service
    if _service is not None:
        return _service

    creds = Credentials(
        token=None,
        refresh_token=GOOGLE_SEARCH_CONSOLE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_SEARCH_CONSOLE_CLIENT_ID,
        client_secret=GOOGLE_SEARCH_CONSOLE_CLIENT_SECRET,
    )

    _service = build("searchconsole", "v1", credentials=creds)
    return _service


def get_site_url() -> str:
    """Return the configured Search Console site URL."""
    return GOOGLE_SEARCH_CONSOLE_SITE_URL


def validate_access():
    """Verify credentials by listing available sites."""
    service = get_service()
    sites = service.sites().list().execute()
    site_entries = sites.get("siteEntry", [])

    target = get_site_url()
    found = False
    for site in site_entries:
        if site.get("siteUrl") == target:
            log.info(
                f"Search Console access verified: {target} "
                f"(permission: {site.get('permissionLevel', 'unknown')})"
            )
            found = True
            break

    if not found:
        available = [s.get("siteUrl") for s in site_entries]
        raise RuntimeError(
            f"Site {target} not found in Search Console. "
            f"Available sites: {available}"
        )
