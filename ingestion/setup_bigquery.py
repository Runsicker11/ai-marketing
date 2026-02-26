"""Create BigQuery dataset and all tables (idempotent)."""

from google.cloud import bigquery
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.bq_client import get_client
from ingestion.utils.logger import get_logger
from ingestion import schemas

log = get_logger(__name__)

# Table definitions: (name, schema, partition_field, cluster_fields)
TABLES = [
    ("shopify_orders", schemas.SHOPIFY_ORDERS, "order_date", ["utm_source", "financial_status"]),
    ("shopify_order_line_items", schemas.SHOPIFY_ORDER_LINE_ITEMS, "order_date", ["product_id"]),
    ("shopify_products", schemas.SHOPIFY_PRODUCTS, None, None),
    ("shopify_product_variants", schemas.SHOPIFY_PRODUCT_VARIANTS, None, None),
    ("shopify_customers", schemas.SHOPIFY_CUSTOMERS, None, None),
    ("meta_campaigns", schemas.META_CAMPAIGNS, None, None),
    ("meta_adsets", schemas.META_ADSETS, None, None),
    ("meta_ads", schemas.META_ADS, None, None),
    ("meta_daily_insights", schemas.META_DAILY_INSIGHTS, "date_start", ["campaign_id", "ad_id"]),
]


def setup():
    client = get_client()
    dataset_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}"

    # Create dataset if needed
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)
    log.info(f"Dataset {BQ_DATASET} ready")

    # Create tables
    for name, schema, partition_field, cluster_fields in TABLES:
        table_id = f"{dataset_ref}.{name}"
        table = bigquery.Table(table_id, schema=schema)

        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            )
        if cluster_fields:
            table.clustering_fields = cluster_fields

        client.create_table(table, exists_ok=True)
        log.info(f"Table {name} ready")

    log.info("All tables created successfully")


if __name__ == "__main__":
    setup()
