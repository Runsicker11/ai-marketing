"""Google Ads API client creation and credential validation."""

from google.ads.googleads.client import GoogleAdsClient

from ingestion.utils.config import (
    GOOGLE_ADS_CUSTOMER_ID,
    GOOGLE_ADS_DEVELOPER_TOKEN,
    GOOGLE_ADS_CLIENT_ID,
    GOOGLE_ADS_CLIENT_SECRET,
    GOOGLE_ADS_REFRESH_TOKEN,
    GOOGLE_ADS_LOGIN_CUSTOMER_ID,
)
from ingestion.utils.logger import get_logger

log = get_logger(__name__)

_client = None


def get_client() -> GoogleAdsClient:
    """Return a cached GoogleAdsClient singleton."""
    global _client
    if _client is not None:
        return _client

    config = {
        "developer_token": GOOGLE_ADS_DEVELOPER_TOKEN,
        "client_id": GOOGLE_ADS_CLIENT_ID,
        "client_secret": GOOGLE_ADS_CLIENT_SECRET,
        "refresh_token": GOOGLE_ADS_REFRESH_TOKEN,
        "use_proto_plus": True,
    }

    if GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        config["login_customer_id"] = GOOGLE_ADS_LOGIN_CUSTOMER_ID

    _client = GoogleAdsClient.load_from_dict(config)
    return _client


def get_service(service_name: str = "GoogleAdsService"):
    """Return a Google Ads service (default: GoogleAdsService for GAQL queries)."""
    return get_client().get_service(service_name)


def validate_access():
    """Verify credentials by querying the customer resource. Logs account name and currency."""
    service = get_service()
    query = """
        SELECT
            customer.descriptive_name,
            customer.currency_code,
            customer.id
        FROM customer
        LIMIT 1
    """
    response = service.search_stream(
        customer_id=GOOGLE_ADS_CUSTOMER_ID, query=query
    )
    for batch in response:
        for row in batch.results:
            log.info(
                f"Google Ads account verified: "
                f"{row.customer.descriptive_name} "
                f"(ID: {row.customer.id}, "
                f"Currency: {row.customer.currency_code})"
            )
            return

    raise RuntimeError("Google Ads credential validation failed — no customer data returned")
