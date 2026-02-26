"""Pull Shopify orders + line items via REST API with cursor pagination."""

from datetime import datetime, date, timedelta, timezone
from urllib.parse import urlparse, parse_qs
import requests

from ingestion.shopify.auth import get_headers, base_url
from ingestion.utils.logger import get_logger

log = get_logger(__name__)

UTM_PARAMS = ["utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term"]


def parse_utms(landing_site: str | None) -> dict:
    """Extract UTM parameters from a Shopify landing_site URL."""
    result = {p: None for p in UTM_PARAMS}
    if not landing_site:
        return result
    try:
        parsed = urlparse(landing_site)
        params = parse_qs(parsed.query)
        for p in UTM_PARAMS:
            values = params.get(p)
            if values:
                result[p] = values[0]
    except Exception:
        pass
    return result


def _parse_link_header(link_header: str | None) -> str | None:
    """Extract the 'next' page URL from Shopify's Link header."""
    if not link_header:
        return None
    for part in link_header.split(","):
        if 'rel="next"' in part:
            url = part.split(";")[0].strip().strip("<>")
            return url
    return None


def pull_orders(since_date: date) -> tuple[list[dict], list[dict]]:
    """Fetch all orders since `since_date`. Returns (orders, line_items)."""
    headers = get_headers()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    url = f"{base_url()}/orders.json"
    params = {
        "status": "any",
        "created_at_min": datetime.combine(since_date, datetime.min.time(), tzinfo=timezone.utc).isoformat(),
        "limit": 250,
        "fields": "id,order_number,created_at,updated_at,financial_status,fulfillment_status,"
                  "total_price,subtotal_price,total_tax,total_shipping_price_set,"
                  "total_discounts,currency,customer,landing_site,referring_site,"
                  "source_name,cancelled_at,cancel_reason,tags,note,line_items",
    }

    all_orders = []
    all_line_items = []
    page = 0

    while url:
        page += 1
        resp = requests.get(url, headers=headers, params=params if page == 1 else None, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        orders = data.get("orders", [])

        for o in orders:
            order_date_val = o["created_at"][:10]
            utms = parse_utms(o.get("landing_site"))

            # Extract total_shipping from the shipping price set
            shipping_set = o.get("total_shipping_price_set")
            total_shipping = 0.0
            if shipping_set and shipping_set.get("shop_money"):
                total_shipping = float(shipping_set["shop_money"].get("amount", 0))

            customer = o.get("customer") or {}

            order_row = {
                "order_id": o["id"],
                "order_number": o.get("order_number"),
                "created_at": o["created_at"],
                "updated_at": o.get("updated_at"),
                "financial_status": o.get("financial_status"),
                "fulfillment_status": o.get("fulfillment_status"),
                "total_price": float(o.get("total_price", 0)),
                "subtotal_price": float(o.get("subtotal_price", 0)),
                "total_tax": float(o.get("total_tax", 0)),
                "total_shipping": total_shipping,
                "total_discounts": float(o.get("total_discounts", 0)),
                "currency": o.get("currency"),
                "customer_id": customer.get("id"),
                "customer_email": customer.get("email"),
                "landing_site": o.get("landing_site"),
                "referring_site": o.get("referring_site"),
                "source_name": o.get("source_name"),
                **utms,
                "cancelled_at": o.get("cancelled_at"),
                "cancel_reason": o.get("cancel_reason"),
                "tags": o.get("tags"),
                "note": o.get("note"),
                "order_date": order_date_val,
                "ingested_at": now_str,
            }
            all_orders.append(order_row)

            for li in o.get("line_items", []):
                li_row = {
                    "line_item_id": li["id"],
                    "order_id": o["id"],
                    "product_id": li.get("product_id"),
                    "variant_id": li.get("variant_id"),
                    "title": li.get("title"),
                    "variant_title": li.get("variant_title"),
                    "sku": li.get("sku"),
                    "quantity": li.get("quantity"),
                    "price": float(li.get("price", 0)),
                    "total_discount": float(li.get("total_discount", 0)),
                    "order_date": order_date_val,
                    "ingested_at": now_str,
                }
                all_line_items.append(li_row)

        log.info(f"Page {page}: fetched {len(orders)} orders (total: {len(all_orders)})")

        # Cursor pagination via Link header
        url = _parse_link_header(resp.headers.get("Link"))
        params = None  # params only used on first request

    log.info(f"Total: {len(all_orders)} orders, {len(all_line_items)} line items since {since_date}")
    return all_orders, all_line_items
