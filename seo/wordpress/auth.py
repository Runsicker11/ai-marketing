"""WordPress REST API authentication via Application Passwords."""

import base64

from ingestion.utils.config import (
    WORDPRESS_URL,
    WORDPRESS_USER,
    WORDPRESS_APP_PASSWORD,
)
from ingestion.utils.logger import get_logger

log = get_logger(__name__)


def get_base_url() -> str:
    """Return the WordPress REST API base URL."""
    url = WORDPRESS_URL.rstrip("/")
    return f"{url}/wp-json/wp/v2"


def get_headers() -> dict:
    """Return auth headers for WordPress REST API (Application Passwords)."""
    credentials = f"{WORDPRESS_USER}:{WORDPRESS_APP_PASSWORD}"
    token = base64.b64encode(credentials.encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
    }


def validate_access() -> dict:
    """Verify WordPress credentials by fetching current user info."""
    import requests

    url = WORDPRESS_URL.rstrip("/") + "/wp-json/wp/v2/users/me"
    resp = requests.get(url, headers=get_headers(), timeout=15)
    resp.raise_for_status()

    user = resp.json()
    log.info(
        f"WordPress access verified: {user.get('name', '')} "
        f"(ID: {user.get('id', '')})"
    )
    return user
