"""Pull Shopify products + variants via GraphQL (REST deprecated in 2024-10)."""

from datetime import datetime, timezone
import requests

from ingestion.shopify.auth import get_headers
from ingestion.utils.config import SHOPIFY_SHOP_DOMAIN, SHOPIFY_API_VERSION
from ingestion.utils.logger import get_logger

log = get_logger(__name__)

PRODUCTS_QUERY = """
query ($cursor: String) {
  products(first: 50, after: $cursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      id
      title
      handle
      productType
      vendor
      status
      tags
      createdAt
      updatedAt
      variants(first: 100) {
        nodes {
          id
          title
          sku
          price
          compareAtPrice
          inventoryQuantity
        }
      }
    }
  }
}
"""


def _gid_to_int(gid: str) -> int:
    """Convert Shopify GID (e.g. 'gid://shopify/Product/123') to integer."""
    return int(gid.rsplit("/", 1)[-1])


def pull_products() -> tuple[list[dict], list[dict]]:
    """Fetch all products and variants. Returns (products, variants)."""
    headers = get_headers()
    url = f"https://{SHOPIFY_SHOP_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    all_products = []
    all_variants = []
    cursor = None

    while True:
        payload = {"query": PRODUCTS_QUERY, "variables": {"cursor": cursor}}
        resp = requests.post(url, json=payload, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if "errors" in data:
            raise RuntimeError(f"GraphQL errors: {data['errors']}")

        products_data = data["data"]["products"]
        for p in products_data["nodes"]:
            product_id = _gid_to_int(p["id"])
            product_row = {
                "product_id": product_id,
                "title": p["title"],
                "handle": p["handle"],
                "product_type": p.get("productType"),
                "vendor": p.get("vendor"),
                "status": p.get("status"),
                "tags": ", ".join(p.get("tags", [])),
                "created_at": p.get("createdAt"),
                "updated_at": p.get("updatedAt"),
                "ingested_at": now_str,
            }
            all_products.append(product_row)

            for v in p.get("variants", {}).get("nodes", []):
                variant_row = {
                    "variant_id": _gid_to_int(v["id"]),
                    "product_id": product_id,
                    "title": v.get("title"),
                    "sku": v.get("sku"),
                    "price": float(v["price"]) if v.get("price") else None,
                    "compare_at_price": float(v["compareAtPrice"]) if v.get("compareAtPrice") else None,
                    "inventory_quantity": v.get("inventoryQuantity"),
                    "weight": None,
                    "weight_unit": None,
                    "ingested_at": now_str,
                }
                all_variants.append(variant_row)

        page_info = products_data["pageInfo"]
        if page_info["hasNextPage"]:
            cursor = page_info["endCursor"]
        else:
            break

    log.info(f"Fetched {len(all_products)} products, {len(all_variants)} variants")
    return all_products, all_variants
