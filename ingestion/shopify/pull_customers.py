"""Pull Shopify customers via REST API with cursor pagination."""

from datetime import datetime, timezone
import requests

from ingestion.shopify.auth import get_headers, base_url
from ingestion.shopify.pull_orders import _parse_link_header
from ingestion.utils.logger import get_logger

log = get_logger(__name__)


def pull_customers() -> list[dict]:
    """Fetch all customers. Returns list of customer rows."""
    headers = get_headers()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    url = f"{base_url()}/customers.json"
    params = {
        "limit": 250,
        "fields": "id,email,first_name,last_name,orders_count,total_spent,"
                  "created_at,updated_at,state,accepts_marketing,"
                  "default_address,tags",
    }

    all_customers = []
    page = 0

    while url:
        page += 1
        resp = requests.get(url, headers=headers, params=params if page == 1 else None, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        customers = data.get("customers", [])

        for c in customers:
            addr = c.get("default_address") or {}
            row = {
                "customer_id": c["id"],
                "email": c.get("email"),
                "first_name": c.get("first_name"),
                "last_name": c.get("last_name"),
                "orders_count": c.get("orders_count"),
                "total_spent": float(c.get("total_spent", 0)),
                "created_at": c.get("created_at"),
                "updated_at": c.get("updated_at"),
                "state": c.get("state"),
                "accepts_marketing": c.get("accepts_marketing", False),
                "city": addr.get("city"),
                "province": addr.get("province"),
                "country": addr.get("country"),
                "tags": c.get("tags"),
                "ingested_at": now_str,
            }
            all_customers.append(row)

        log.info(f"Page {page}: fetched {len(customers)} customers (total: {len(all_customers)})")
        url = _parse_link_header(resp.headers.get("Link"))
        params = None

    log.info(f"Total: {len(all_customers)} customers")
    return all_customers
