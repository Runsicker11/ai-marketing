"""BigQuery table schema definitions for all marketing data tables."""

from google.cloud.bigquery import SchemaField

# ─── Shopify ────────────────────────────────────────────────

SHOPIFY_ORDERS = [
    SchemaField("order_id", "INT64", mode="REQUIRED"),
    SchemaField("order_number", "INT64"),
    SchemaField("created_at", "TIMESTAMP"),
    SchemaField("updated_at", "TIMESTAMP"),
    SchemaField("financial_status", "STRING"),
    SchemaField("fulfillment_status", "STRING"),
    SchemaField("total_price", "FLOAT64"),
    SchemaField("subtotal_price", "FLOAT64"),
    SchemaField("total_tax", "FLOAT64"),
    SchemaField("total_shipping", "FLOAT64"),
    SchemaField("total_discounts", "FLOAT64"),
    SchemaField("currency", "STRING"),
    SchemaField("customer_id", "INT64"),
    SchemaField("customer_email", "STRING"),
    SchemaField("landing_site", "STRING"),
    SchemaField("referring_site", "STRING"),
    SchemaField("source_name", "STRING"),
    SchemaField("utm_source", "STRING"),
    SchemaField("utm_medium", "STRING"),
    SchemaField("utm_campaign", "STRING"),
    SchemaField("utm_content", "STRING"),
    SchemaField("utm_term", "STRING"),
    SchemaField("cancelled_at", "TIMESTAMP"),
    SchemaField("cancel_reason", "STRING"),
    SchemaField("tags", "STRING"),
    SchemaField("note", "STRING"),
    SchemaField("order_date", "DATE"),
    SchemaField("ingested_at", "TIMESTAMP"),
]

SHOPIFY_ORDER_LINE_ITEMS = [
    SchemaField("line_item_id", "INT64", mode="REQUIRED"),
    SchemaField("order_id", "INT64", mode="REQUIRED"),
    SchemaField("product_id", "INT64"),
    SchemaField("variant_id", "INT64"),
    SchemaField("title", "STRING"),
    SchemaField("variant_title", "STRING"),
    SchemaField("sku", "STRING"),
    SchemaField("quantity", "INT64"),
    SchemaField("price", "FLOAT64"),
    SchemaField("total_discount", "FLOAT64"),
    SchemaField("order_date", "DATE"),
    SchemaField("ingested_at", "TIMESTAMP"),
]

SHOPIFY_PRODUCTS = [
    SchemaField("product_id", "INT64", mode="REQUIRED"),
    SchemaField("title", "STRING"),
    SchemaField("handle", "STRING"),
    SchemaField("product_type", "STRING"),
    SchemaField("vendor", "STRING"),
    SchemaField("status", "STRING"),
    SchemaField("tags", "STRING"),
    SchemaField("created_at", "TIMESTAMP"),
    SchemaField("updated_at", "TIMESTAMP"),
    SchemaField("ingested_at", "TIMESTAMP"),
]

SHOPIFY_PRODUCT_VARIANTS = [
    SchemaField("variant_id", "INT64", mode="REQUIRED"),
    SchemaField("product_id", "INT64", mode="REQUIRED"),
    SchemaField("title", "STRING"),
    SchemaField("sku", "STRING"),
    SchemaField("price", "FLOAT64"),
    SchemaField("compare_at_price", "FLOAT64"),
    SchemaField("inventory_quantity", "INT64"),
    SchemaField("weight", "FLOAT64"),
    SchemaField("weight_unit", "STRING"),
    SchemaField("ingested_at", "TIMESTAMP"),
]

SHOPIFY_CUSTOMERS = [
    SchemaField("customer_id", "INT64", mode="REQUIRED"),
    SchemaField("email", "STRING"),
    SchemaField("first_name", "STRING"),
    SchemaField("last_name", "STRING"),
    SchemaField("orders_count", "INT64"),
    SchemaField("total_spent", "FLOAT64"),
    SchemaField("created_at", "TIMESTAMP"),
    SchemaField("updated_at", "TIMESTAMP"),
    SchemaField("state", "STRING"),
    SchemaField("accepts_marketing", "BOOL"),
    SchemaField("city", "STRING"),
    SchemaField("province", "STRING"),
    SchemaField("country", "STRING"),
    SchemaField("tags", "STRING"),
    SchemaField("ingested_at", "TIMESTAMP"),
]

# ─── Meta Ads ───────────────────────────────────────────────

META_CAMPAIGNS = [
    SchemaField("campaign_id", "STRING", mode="REQUIRED"),
    SchemaField("campaign_name", "STRING"),
    SchemaField("objective", "STRING"),
    SchemaField("status", "STRING"),
    SchemaField("daily_budget", "FLOAT64"),
    SchemaField("lifetime_budget", "FLOAT64"),
    SchemaField("created_time", "TIMESTAMP"),
    SchemaField("updated_time", "TIMESTAMP"),
    SchemaField("ingested_at", "TIMESTAMP"),
]

META_ADSETS = [
    SchemaField("adset_id", "STRING", mode="REQUIRED"),
    SchemaField("adset_name", "STRING"),
    SchemaField("campaign_id", "STRING"),
    SchemaField("status", "STRING"),
    SchemaField("daily_budget", "FLOAT64"),
    SchemaField("lifetime_budget", "FLOAT64"),
    SchemaField("targeting_summary", "STRING"),
    SchemaField("optimization_goal", "STRING"),
    SchemaField("billing_event", "STRING"),
    SchemaField("created_time", "TIMESTAMP"),
    SchemaField("updated_time", "TIMESTAMP"),
    SchemaField("ingested_at", "TIMESTAMP"),
]

META_ADS = [
    SchemaField("ad_id", "STRING", mode="REQUIRED"),
    SchemaField("ad_name", "STRING"),
    SchemaField("adset_id", "STRING"),
    SchemaField("campaign_id", "STRING"),
    SchemaField("status", "STRING"),
    SchemaField("creative_id", "STRING"),
    SchemaField("created_time", "TIMESTAMP"),
    SchemaField("updated_time", "TIMESTAMP"),
    SchemaField("ingested_at", "TIMESTAMP"),
]

META_DAILY_INSIGHTS = [
    SchemaField("date_start", "DATE", mode="REQUIRED"),
    SchemaField("campaign_id", "STRING"),
    SchemaField("campaign_name", "STRING"),
    SchemaField("adset_id", "STRING"),
    SchemaField("adset_name", "STRING"),
    SchemaField("ad_id", "STRING"),
    SchemaField("ad_name", "STRING"),
    SchemaField("impressions", "INT64"),
    SchemaField("clicks", "INT64"),
    SchemaField("spend", "FLOAT64"),
    SchemaField("cpc", "FLOAT64"),
    SchemaField("cpm", "FLOAT64"),
    SchemaField("ctr", "FLOAT64"),
    SchemaField("reach", "INT64"),
    SchemaField("frequency", "FLOAT64"),
    SchemaField("purchases", "INT64"),
    SchemaField("purchase_value", "FLOAT64"),
    SchemaField("add_to_cart", "INT64"),
    SchemaField("add_to_cart_value", "FLOAT64"),
    SchemaField("initiate_checkout", "INT64"),
    SchemaField("initiate_checkout_value", "FLOAT64"),
    SchemaField("landing_page_views", "INT64"),
    SchemaField("link_clicks", "INT64"),
    SchemaField("ingested_at", "TIMESTAMP"),
]

META_CREATIVES = [
    SchemaField("creative_id", "STRING", mode="REQUIRED"),
    SchemaField("ad_id", "STRING"),
    SchemaField("ad_name", "STRING"),
    SchemaField("title", "STRING"),
    SchemaField("body", "STRING"),
    SchemaField("link_description", "STRING"),
    SchemaField("cta_type", "STRING"),
    SchemaField("image_url", "STRING"),
    SchemaField("video_id", "STRING"),
    SchemaField("thumbnail_url", "STRING"),
    SchemaField("object_type", "STRING"),
    SchemaField("page_id", "STRING"),
    SchemaField("instagram_actor_id", "STRING"),
    SchemaField("created_time", "TIMESTAMP"),
    SchemaField("ingested_at", "TIMESTAMP"),
]

# ─── Content Library ───────────────────────────────────────

CONTENT_LIBRARY = [
    SchemaField("component_id", "STRING", mode="REQUIRED"),
    SchemaField("component_type", "STRING"),  # hook, body, cta
    SchemaField("text", "STRING"),
    SchemaField("source", "STRING"),  # audit, generated, manual
    SchemaField("source_ad_id", "STRING"),
    SchemaField("source_ad_name", "STRING"),
    SchemaField("score", "FLOAT64"),
    SchemaField("status", "STRING"),  # active, proven, retired
    SchemaField("product_focus", "STRING"),
    SchemaField("created_at", "TIMESTAMP"),
    SchemaField("updated_at", "TIMESTAMP"),
]
