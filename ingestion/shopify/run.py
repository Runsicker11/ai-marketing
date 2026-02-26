"""Shopify ingestion orchestrator: auth -> pull -> BigQuery load."""

import argparse
from datetime import date, timedelta

from ingestion.shopify.auth import get_access_token
from ingestion.shopify.pull_orders import pull_orders
from ingestion.shopify.pull_products import pull_products
from ingestion.shopify.pull_customers import pull_customers
from ingestion.utils.bq_client import load_rows, full_replace, delete_date_range
from ingestion.utils.logger import get_logger
from ingestion import schemas

log = get_logger(__name__)


def run(days_back: int = 3):
    log.info(f"Starting Shopify ingestion (days_back={days_back})")

    # 1. Authenticate
    get_access_token()

    # 2. Pull and load orders + line items (incremental)
    since = date.today() - timedelta(days=days_back)
    today = date.today()

    orders, line_items = pull_orders(since)

    if orders:
        delete_date_range("shopify_orders", "order_date", since, today)
        load_rows("shopify_orders", orders, schemas.SHOPIFY_ORDERS)

    if line_items:
        delete_date_range("shopify_order_line_items", "order_date", since, today)
        load_rows("shopify_order_line_items", line_items, schemas.SHOPIFY_ORDER_LINE_ITEMS)

    # 3. Pull and load products + variants (full replace)
    products, variants = pull_products()
    full_replace("shopify_products", products, schemas.SHOPIFY_PRODUCTS)
    full_replace("shopify_product_variants", variants, schemas.SHOPIFY_PRODUCT_VARIANTS)

    # 4. Pull and load customers (full replace)
    customers = pull_customers()
    full_replace("shopify_customers", customers, schemas.SHOPIFY_CUSTOMERS)

    log.info("Shopify ingestion complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Shopify data ingestion")
    parser.add_argument("--days-back", type=int, default=3, help="Days of order history to pull")
    args = parser.parse_args()
    run(days_back=args.days_back)
