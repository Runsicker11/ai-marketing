-- ============================================================
-- Unified Marketing Views
-- All views use {dataset} placeholder, replaced at deploy time
-- ============================================================

-- 1. vw_daily_performance
-- Meta spend/clicks/conversions by day/campaign/ad
-- UNION-ready for Google Ads later
CREATE OR REPLACE VIEW `{dataset}.vw_daily_performance` AS
SELECT
    date_start AS report_date,
    'meta' AS platform,
    campaign_id,
    campaign_name,
    adset_id,
    adset_name,
    ad_id,
    ad_name,
    impressions,
    clicks,
    link_clicks,
    spend,
    cpc,
    cpm,
    ctr,
    reach,
    frequency,
    purchases AS conversions,
    purchase_value AS conversion_value,
    add_to_cart,
    add_to_cart_value,
    initiate_checkout,
    initiate_checkout_value,
    landing_page_views
FROM `{dataset}.meta_daily_insights`
;

-- 2. vw_true_roas
-- Meta spend vs actual Shopify revenue (UTM-attributed)
-- Shows the gap between Meta-reported and actual revenue
CREATE OR REPLACE VIEW `{dataset}.vw_true_roas` AS
WITH meta_daily AS (
    SELECT
        date_start AS report_date,
        SUM(spend) AS meta_spend,
        SUM(impressions) AS meta_impressions,
        SUM(clicks) AS meta_clicks,
        SUM(purchases) AS meta_reported_purchases,
        SUM(purchase_value) AS meta_reported_revenue
    FROM `{dataset}.meta_daily_insights`
    GROUP BY date_start
),
shopify_meta AS (
    SELECT
        order_date AS report_date,
        COUNT(*) AS shopify_orders,
        SUM(total_price) AS shopify_meta_revenue
    FROM `{dataset}.shopify_orders`
    WHERE LOWER(COALESCE(utm_source, '')) IN ('facebook', 'fb', 'ig', 'instagram', 'meta')
        AND financial_status NOT IN ('refunded', 'voided')
    GROUP BY order_date
)
SELECT
    COALESCE(m.report_date, s.report_date) AS report_date,
    m.meta_spend,
    m.meta_impressions,
    m.meta_clicks,
    m.meta_reported_purchases,
    m.meta_reported_revenue,
    s.shopify_orders AS shopify_meta_orders,
    s.shopify_meta_revenue,
    -- True ROAS = actual Shopify revenue / ad spend
    SAFE_DIVIDE(s.shopify_meta_revenue, m.meta_spend) AS true_roas,
    -- Meta-reported ROAS (the inflated number)
    SAFE_DIVIDE(m.meta_reported_revenue, m.meta_spend) AS meta_reported_roas,
    -- Revenue gap: how much Meta over-reports
    COALESCE(m.meta_reported_revenue, 0) - COALESCE(s.shopify_meta_revenue, 0) AS revenue_gap
FROM meta_daily m
FULL OUTER JOIN shopify_meta s ON m.report_date = s.report_date
;

-- 3. vw_product_performance
-- Product sales by channel (which products sell from which ad source, at what CPA)
CREATE OR REPLACE VIEW `{dataset}.vw_product_performance` AS
SELECT
    li.product_id,
    COALESCE(p.title, li.title) AS product_title,
    p.product_type,
    COALESCE(o.utm_source, 'direct') AS source,
    COALESCE(o.utm_medium, 'none') AS medium,
    o.utm_campaign AS campaign,
    COUNT(DISTINCT o.order_id) AS orders,
    SUM(li.quantity) AS units_sold,
    SUM(li.price * li.quantity) AS gross_revenue,
    SUM(li.total_discount) AS total_discounts,
    SUM(li.price * li.quantity - li.total_discount) AS net_revenue,
    AVG(li.price) AS avg_price
FROM `{dataset}.shopify_order_line_items` li
JOIN `{dataset}.shopify_orders` o ON li.order_id = o.order_id
LEFT JOIN `{dataset}.shopify_products` p ON li.product_id = p.product_id
WHERE o.financial_status NOT IN ('refunded', 'voided')
GROUP BY li.product_id, product_title, p.product_type, source, medium, campaign
;

-- 4. vw_trends
-- 7-day and 30-day rolling averages for spend, revenue, ROAS, orders
-- Plus day-over-day and week-over-week changes
CREATE OR REPLACE VIEW `{dataset}.vw_trends` AS
WITH daily AS (
    SELECT
        report_date,
        meta_spend,
        meta_reported_revenue,
        shopify_meta_revenue,
        shopify_meta_orders,
        true_roas
    FROM `{dataset}.vw_true_roas`
)
SELECT
    report_date,
    meta_spend,
    shopify_meta_revenue,
    shopify_meta_orders,
    true_roas,

    -- 7-day rolling averages
    AVG(meta_spend) OVER (ORDER BY report_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS spend_7d_avg,
    AVG(shopify_meta_revenue) OVER (ORDER BY report_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS revenue_7d_avg,
    AVG(true_roas) OVER (ORDER BY report_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS roas_7d_avg,
    AVG(shopify_meta_orders) OVER (ORDER BY report_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS orders_7d_avg,

    -- 30-day rolling averages
    AVG(meta_spend) OVER (ORDER BY report_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS spend_30d_avg,
    AVG(shopify_meta_revenue) OVER (ORDER BY report_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS revenue_30d_avg,
    AVG(true_roas) OVER (ORDER BY report_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS roas_30d_avg,

    -- Day-over-day changes
    meta_spend - LAG(meta_spend) OVER (ORDER BY report_date) AS spend_dod_change,
    shopify_meta_revenue - LAG(shopify_meta_revenue) OVER (ORDER BY report_date) AS revenue_dod_change,

    -- Week-over-week changes (vs same day 7 days ago)
    meta_spend - LAG(meta_spend, 7) OVER (ORDER BY report_date) AS spend_wow_change,
    shopify_meta_revenue - LAG(shopify_meta_revenue, 7) OVER (ORDER BY report_date) AS revenue_wow_change
FROM daily
;

-- ============================================================
-- GA4-Powered Views (Phase 2)
-- Uses GA4 BigQuery export for richer attribution than Shopify UTMs
-- GA4 dataset: {ga4_dataset}
-- ============================================================

-- 5. vw_ga4_attribution
-- Joins GA4 purchase events with Shopify orders via transaction_id
-- GA4 captures session_traffic_source_last_click even when Shopify UTMs are empty
CREATE OR REPLACE VIEW `{dataset}.vw_ga4_attribution` AS
WITH ga4_purchases_raw AS (
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS order_date,
        ecommerce.transaction_id,
        COALESCE(
            collected_traffic_source.manual_source,
            traffic_source.source
        ) AS ga4_source,
        COALESCE(
            collected_traffic_source.manual_medium,
            traffic_source.medium
        ) AS ga4_medium,
        COALESCE(
            collected_traffic_source.manual_campaign_name,
            traffic_source.name
        ) AS ga4_campaign,
        CAST(NULL AS STRING) AS google_ads_campaign_id,
        ecommerce.purchase_revenue AS ga4_revenue,
        user_pseudo_id,
        ROW_NUMBER() OVER (PARTITION BY ecommerce.transaction_id ORDER BY event_timestamp DESC) AS rn
    FROM `{ga4_dataset}.events_*`
    WHERE event_name = 'purchase'
        AND ecommerce.transaction_id IS NOT NULL
        AND _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
),
ga4_purchases AS (
    SELECT * EXCEPT(rn) FROM ga4_purchases_raw WHERE rn = 1
)
SELECT
    g.order_date,
    g.transaction_id,
    o.order_id,
    g.ga4_source,
    g.ga4_medium,
    g.ga4_campaign,
    g.google_ads_campaign_id,
    o.total_price AS shopify_revenue,
    g.ga4_revenue,
    g.user_pseudo_id
FROM ga4_purchases g
LEFT JOIN `{dataset}.shopify_orders` o
    ON CAST(o.order_id AS STRING) = g.transaction_id
;

-- 6. vw_enhanced_roas
-- Uses GA4 attribution (not Shopify UTMs) to assign revenue to channels
-- Then joins with Meta spend for true ROAS by channel
CREATE OR REPLACE VIEW `{dataset}.vw_enhanced_roas` AS
WITH ga4_revenue AS (
    SELECT
        order_date AS report_date,
        CASE
            WHEN LOWER(COALESCE(ga4_source, '')) IN ('facebook', 'fb', 'ig', 'instagram', 'meta')
                OR LOWER(COALESCE(ga4_medium, '')) IN ('paid_social', 'paidsocial')
                THEN 'meta'
            WHEN LOWER(COALESCE(ga4_source, '')) = 'google'
                AND LOWER(COALESCE(ga4_medium, '')) IN ('cpc', 'ppc', 'paid')
                THEN 'google_ads'
            WHEN LOWER(COALESCE(ga4_medium, '')) IN ('organic', 'referral')
                THEN LOWER(ga4_medium)
            WHEN LOWER(COALESCE(ga4_source, '')) = '(direct)'
                OR ga4_source IS NULL
                THEN 'direct'
            ELSE 'other'
        END AS channel,
        shopify_revenue,
        order_id
    FROM `{dataset}.vw_ga4_attribution`
    WHERE shopify_revenue IS NOT NULL
),
channel_daily AS (
    SELECT
        report_date,
        channel,
        SUM(shopify_revenue) AS ga4_attributed_revenue,
        COUNT(DISTINCT order_id) AS ga4_attributed_orders
    FROM ga4_revenue
    GROUP BY report_date, channel
),
meta_spend AS (
    SELECT
        date_start AS report_date,
        SUM(spend) AS ad_spend
    FROM `{dataset}.meta_daily_insights`
    GROUP BY date_start
)
SELECT
    c.report_date,
    c.channel,
    c.ga4_attributed_revenue,
    c.ga4_attributed_orders,
    CASE WHEN c.channel = 'meta' THEN m.ad_spend ELSE NULL END AS ad_spend,
    CASE WHEN c.channel = 'meta' THEN SAFE_DIVIDE(c.ga4_attributed_revenue, m.ad_spend) ELSE NULL END AS enhanced_roas
FROM channel_daily c
LEFT JOIN meta_spend m ON c.report_date = m.report_date AND c.channel = 'meta'
;

-- 7. vw_ga4_funnel
-- Full purchase funnel from GA4 events by traffic source
-- Shows where users drop off at each stage
CREATE OR REPLACE VIEW `{dataset}.vw_ga4_funnel` AS
WITH events AS (
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS report_date,
        event_name,
        user_pseudo_id,
        COALESCE(
            collected_traffic_source.manual_source,
            traffic_source.source,
            '(direct)'
        ) AS source,
        COALESCE(
            collected_traffic_source.manual_medium,
            traffic_source.medium,
            '(none)'
        ) AS medium
    FROM `{ga4_dataset}.events_*`
    WHERE event_name IN ('session_start', 'page_view', 'view_item', 'add_to_cart', 'begin_checkout', 'purchase')
        AND _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
)
SELECT
    report_date,
    source,
    medium,
    COUNT(DISTINCT CASE WHEN event_name = 'session_start' THEN user_pseudo_id END) AS sessions,
    COUNT(DISTINCT CASE WHEN event_name = 'page_view' THEN user_pseudo_id END) AS page_views,
    COUNT(DISTINCT CASE WHEN event_name = 'view_item' THEN user_pseudo_id END) AS product_views,
    COUNT(DISTINCT CASE WHEN event_name = 'add_to_cart' THEN user_pseudo_id END) AS add_to_carts,
    COUNT(DISTINCT CASE WHEN event_name = 'begin_checkout' THEN user_pseudo_id END) AS checkouts,
    COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_pseudo_id END) AS purchases,
    SAFE_DIVIDE(
        COUNT(DISTINCT CASE WHEN event_name = 'add_to_cart' THEN user_pseudo_id END),
        COUNT(DISTINCT CASE WHEN event_name = 'view_item' THEN user_pseudo_id END)
    ) AS view_to_cart_rate,
    SAFE_DIVIDE(
        COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_pseudo_id END),
        COUNT(DISTINCT CASE WHEN event_name = 'add_to_cart' THEN user_pseudo_id END)
    ) AS cart_to_purchase_rate,
    SAFE_DIVIDE(
        COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_pseudo_id END),
        COUNT(DISTINCT CASE WHEN event_name = 'session_start' THEN user_pseudo_id END)
    ) AS overall_conversion_rate
FROM events
GROUP BY report_date, source, medium
;

-- ============================================================
-- Phase 3: Content Machine Views
-- ============================================================

-- 9. vw_creative_performance
-- Joins creative text with daily performance data
-- Shows headline/body alongside ROAS, CTR, CPA per creative
CREATE OR REPLACE VIEW `{dataset}.vw_creative_performance` AS
WITH creative_daily AS (
    SELECT
        i.ad_id,
        i.ad_name,
        SUM(i.spend) AS lifetime_spend,
        SUM(i.impressions) AS lifetime_impressions,
        SUM(i.clicks) AS lifetime_clicks,
        SUM(i.purchases) AS lifetime_purchases,
        SUM(i.purchase_value) AS lifetime_revenue,
        SAFE_DIVIDE(SUM(i.purchase_value), SUM(i.spend)) AS lifetime_roas,
        SAFE_DIVIDE(SUM(i.clicks), SUM(i.impressions)) * 100 AS lifetime_ctr,
        SAFE_DIVIDE(SUM(i.spend), SUM(i.purchases)) AS lifetime_cpa,
        MIN(i.date_start) AS first_date,
        MAX(i.date_start) AS last_date,
        COUNT(DISTINCT i.date_start) AS days_active
    FROM `{dataset}.meta_daily_insights` i
    WHERE i.spend > 0
    GROUP BY i.ad_id, i.ad_name
)
SELECT
    c.creative_id,
    c.ad_id,
    cd.ad_name,
    c.title AS headline,
    c.body AS primary_text,
    c.cta_type,
    c.object_type,
    c.video_id,
    cd.lifetime_spend,
    cd.lifetime_impressions,
    cd.lifetime_clicks,
    cd.lifetime_purchases,
    cd.lifetime_revenue,
    cd.lifetime_roas,
    cd.lifetime_ctr,
    cd.lifetime_cpa,
    cd.first_date,
    cd.last_date,
    cd.days_active,
    -- Performance tier
    CASE
        WHEN cd.lifetime_roas >= 3.0 THEN 'top_performer'
        WHEN cd.lifetime_roas >= 2.0 THEN 'good'
        WHEN cd.lifetime_roas >= 1.0 THEN 'marginal'
        ELSE 'underperformer'
    END AS performance_tier,
    -- Health status
    CASE
        WHEN cd.days_active >= 14
            AND cd.lifetime_ctr < 1.0 THEN 'possible_fatigue'
        WHEN cd.lifetime_roas < 0.5
            AND cd.lifetime_spend > 50 THEN 'cut_candidate'
        ELSE 'healthy'
    END AS health_status
FROM `{dataset}.meta_creatives` c
LEFT JOIN creative_daily cd ON c.ad_id = cd.ad_id
;

-- 10. vw_component_scores
-- Matches content library components to live ad creatives via text matching
-- Aggregates performance per component after 7+ days of data
CREATE OR REPLACE VIEW `{dataset}.vw_component_scores` AS
WITH component_matches AS (
    SELECT
        cl.component_id,
        cl.component_type,
        cl.text,
        cp.creative_id,
        cp.ad_id,
        cp.ad_name,
        cp.lifetime_roas,
        cp.lifetime_ctr,
        cp.lifetime_cpa,
        cp.lifetime_spend,
        cp.lifetime_purchases,
        cp.days_active
    FROM `{dataset}.content_library` cl
    JOIN `{dataset}.vw_creative_performance` cp
        ON (cl.component_type = 'hook' AND cp.headline IS NOT NULL
            AND LOWER(TRIM(cl.text)) = LOWER(TRIM(cp.headline)))
        OR (cl.component_type = 'body' AND cp.primary_text IS NOT NULL
            AND LOWER(TRIM(cl.text)) = LOWER(TRIM(cp.primary_text)))
    WHERE cp.days_active >= 7
)
SELECT
    component_id,
    component_type,
    text,
    COUNT(DISTINCT ad_id) AS ads_using,
    SUM(lifetime_spend) AS total_spend,
    SUM(lifetime_purchases) AS total_purchases,
    SAFE_DIVIDE(SUM(lifetime_purchases * lifetime_roas * lifetime_spend),
                SUM(lifetime_spend)) AS weighted_roas,
    AVG(lifetime_ctr) AS avg_ctr,
    AVG(lifetime_cpa) AS avg_cpa,
    MAX(days_active) AS max_days_active
FROM component_matches
GROUP BY component_id, component_type, text
;

-- 8. vw_ga4_product_insights
-- Product-level performance from GA4 events
-- Shows products that get views but don't sell (unlike Shopify which only has sales data)
CREATE OR REPLACE VIEW `{dataset}.vw_ga4_product_insights` AS
WITH product_events AS (
    SELECT
        event_name,
        items.item_name,
        items.item_id,
        items.price,
        items.quantity
    FROM `{ga4_dataset}.events_*`,
    UNNEST(items) AS items
    WHERE event_name IN ('view_item', 'add_to_cart', 'purchase')
        AND items.item_name IS NOT NULL
        AND _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
)
SELECT
    item_name,
    item_id,
    COUNTIF(event_name = 'view_item') AS product_views,
    COUNTIF(event_name = 'add_to_cart') AS add_to_carts,
    COUNTIF(event_name = 'purchase') AS purchases,
    SUM(CASE WHEN event_name = 'purchase' THEN price * quantity ELSE 0 END) AS revenue,
    SAFE_DIVIDE(COUNTIF(event_name = 'add_to_cart'), COUNTIF(event_name = 'view_item')) AS view_to_cart_rate,
    SAFE_DIVIDE(COUNTIF(event_name = 'purchase'), COUNTIF(event_name = 'add_to_cart')) AS cart_to_purchase_rate,
    AVG(price) AS avg_price
FROM product_events
GROUP BY item_name, item_id
;
