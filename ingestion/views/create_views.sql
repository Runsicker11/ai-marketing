-- ============================================================
-- Unified Marketing Views
-- All views use {dataset} placeholder, replaced at deploy time
-- ============================================================

-- 1. vw_daily_performance
-- Unified daily performance across Meta + Google Ads
CREATE OR REPLACE VIEW `{dataset}.vw_daily_performance` AS
SELECT
    date_start AS report_date,
    'meta' AS platform,
    campaign_id,
    campaign_name,
    CAST(NULL AS STRING) AS campaign_type,
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
UNION ALL
SELECT
    date_start AS report_date,
    'google_ads' AS platform,
    CAST(campaign_id AS STRING) AS campaign_id,
    campaign_name,
    campaign_type,
    CAST(ad_group_id AS STRING) AS adset_id,
    ad_group_name AS adset_name,
    CAST(NULL AS STRING) AS ad_id,
    CAST(NULL AS STRING) AS ad_name,
    impressions,
    clicks,
    CAST(NULL AS INT64) AS link_clicks,
    spend,
    cpc,
    CAST(NULL AS FLOAT64) AS cpm,
    ctr,
    CAST(NULL AS INT64) AS reach,
    CAST(NULL AS FLOAT64) AS frequency,
    conversions,
    conversion_value,
    CAST(NULL AS INT64) AS add_to_cart,
    CAST(NULL AS FLOAT64) AS add_to_cart_value,
    CAST(NULL AS INT64) AS initiate_checkout,
    CAST(NULL AS FLOAT64) AS initiate_checkout_value,
    CAST(NULL AS INT64) AS landing_page_views
FROM `{dataset}.google_ads_daily_insights`
;

-- 2. vw_true_roas
-- Cross-platform spend vs actual Shopify revenue (UTM-attributed)
-- Includes Meta + Google Ads spend with blended ROAS
-- Backward compat: true_roas = blended_true_roas (referenced by vw_trends, weekly_strategy, daily_report, query_check)
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
google_daily AS (
    SELECT
        date_start AS report_date,
        SUM(spend) AS google_spend,
        SUM(impressions) AS google_impressions,
        SUM(clicks) AS google_clicks,
        SUM(conversions) AS google_conversions,
        SUM(conversion_value) AS google_conversion_value
    FROM `{dataset}.google_ads_daily_insights`
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
),
shopify_google AS (
    SELECT
        order_date AS report_date,
        COUNT(*) AS shopify_google_orders,
        SUM(total_price) AS shopify_google_revenue
    FROM `{dataset}.shopify_orders`
    WHERE LOWER(COALESCE(utm_source, '')) = 'google'
        AND LOWER(COALESCE(utm_medium, '')) IN ('cpc', 'ppc', 'paid')
        AND financial_status NOT IN ('refunded', 'voided')
    GROUP BY order_date
)
SELECT
    COALESCE(m.report_date, g.report_date, sm.report_date, sg.report_date) AS report_date,
    -- Meta
    m.meta_spend,
    m.meta_impressions,
    m.meta_clicks,
    m.meta_reported_purchases,
    m.meta_reported_revenue,
    sm.shopify_orders AS shopify_meta_orders,
    sm.shopify_meta_revenue,
    SAFE_DIVIDE(sm.shopify_meta_revenue, m.meta_spend) AS meta_true_roas,
    SAFE_DIVIDE(m.meta_reported_revenue, m.meta_spend) AS meta_reported_roas,
    COALESCE(m.meta_reported_revenue, 0) - COALESCE(sm.shopify_meta_revenue, 0) AS revenue_gap,
    -- Google Ads
    g.google_spend,
    g.google_impressions,
    g.google_clicks,
    g.google_conversions,
    g.google_conversion_value,
    sg.shopify_google_orders,
    sg.shopify_google_revenue,
    SAFE_DIVIDE(sg.shopify_google_revenue, g.google_spend) AS google_true_roas,
    -- Blended
    SAFE_DIVIDE(
        COALESCE(sm.shopify_meta_revenue, 0) + COALESCE(sg.shopify_google_revenue, 0),
        COALESCE(m.meta_spend, 0) + COALESCE(g.google_spend, 0)
    ) AS blended_true_roas,
    -- Backward compat: true_roas = blended_true_roas
    SAFE_DIVIDE(
        COALESCE(sm.shopify_meta_revenue, 0) + COALESCE(sg.shopify_google_revenue, 0),
        COALESCE(m.meta_spend, 0) + COALESCE(g.google_spend, 0)
    ) AS true_roas
FROM meta_daily m
FULL OUTER JOIN google_daily g ON m.report_date = g.report_date
FULL OUTER JOIN shopify_meta sm ON COALESCE(m.report_date, g.report_date) = sm.report_date
FULL OUTER JOIN shopify_google sg ON COALESCE(m.report_date, g.report_date) = sg.report_date
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
-- Then joins with Meta + Google Ads spend for true ROAS by channel
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
),
google_spend AS (
    SELECT
        date_start AS report_date,
        SUM(spend) AS ad_spend
    FROM `{dataset}.google_ads_daily_insights`
    GROUP BY date_start
)
SELECT
    c.report_date,
    c.channel,
    c.ga4_attributed_revenue,
    c.ga4_attributed_orders,
    CASE
        WHEN c.channel = 'meta' THEN m.ad_spend
        WHEN c.channel = 'google_ads' THEN g.ad_spend
        ELSE NULL
    END AS ad_spend,
    CASE
        WHEN c.channel = 'meta' THEN SAFE_DIVIDE(c.ga4_attributed_revenue, m.ad_spend)
        WHEN c.channel = 'google_ads' THEN SAFE_DIVIDE(c.ga4_attributed_revenue, g.ad_spend)
        ELSE NULL
    END AS enhanced_roas
FROM channel_daily c
LEFT JOIN meta_spend m ON c.report_date = m.report_date AND c.channel = 'meta'
LEFT JOIN google_spend g ON c.report_date = g.report_date AND c.channel = 'google_ads'
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

-- ============================================================
-- Google Ads Views
-- ============================================================

-- 11. vw_google_ads_keywords
-- Keyword performance with quality scores and performance tiers
-- Aggregates search term data per keyword with ROAS, CPA, CTR
CREATE OR REPLACE VIEW `{dataset}.vw_google_ads_keywords` AS
WITH keyword_perf AS (
    SELECT
        st.keyword_text,
        st.campaign_id,
        st.campaign_name,
        st.ad_group_id,
        st.ad_group_name,
        SUM(st.impressions) AS total_impressions,
        SUM(st.clicks) AS total_clicks,
        SUM(st.spend) AS total_spend,
        SUM(st.conversions) AS total_conversions,
        SUM(st.conversion_value) AS total_conversion_value,
        SAFE_DIVIDE(SUM(st.clicks), SUM(st.impressions)) AS avg_ctr,
        SAFE_DIVIDE(SUM(st.spend), SUM(st.conversions)) AS avg_cpa,
        SAFE_DIVIDE(SUM(st.conversion_value), SUM(st.spend)) AS roas,
        COUNT(DISTINCT st.date_start) AS days_active
    FROM `{dataset}.google_ads_search_terms` st
    GROUP BY st.keyword_text, st.campaign_id, st.campaign_name, st.ad_group_id, st.ad_group_name
)
SELECT
    kp.keyword_text,
    kp.campaign_id,
    kp.campaign_name,
    kp.ad_group_id,
    kp.ad_group_name,
    kp.total_impressions,
    kp.total_clicks,
    kp.total_spend,
    kp.total_conversions,
    kp.total_conversion_value,
    kp.avg_ctr,
    kp.avg_cpa,
    kp.roas,
    kp.days_active,
    k.quality_score,
    k.expected_ctr,
    k.ad_relevance,
    k.landing_page_experience,
    CASE
        WHEN kp.roas >= 3.0 AND kp.total_conversions >= 3 THEN 'top_performer'
        WHEN kp.roas >= 2.0 AND kp.total_conversions >= 1 THEN 'good'
        WHEN kp.roas >= 1.0 THEN 'marginal'
        WHEN kp.total_spend > 10 AND kp.total_conversions = 0 THEN 'wasted_spend'
        ELSE 'underperformer'
    END AS performance_tier
FROM keyword_perf kp
LEFT JOIN `{dataset}.google_ads_keywords` k
    ON kp.keyword_text = k.keyword_text
    AND kp.ad_group_id = k.ad_group_id
;

-- 12. vw_search_terms_waste
-- Search terms with spend but zero conversions — negative keyword candidates
-- Immediately actionable: add as negative keywords to stop budget waste
CREATE OR REPLACE VIEW `{dataset}.vw_search_terms_waste` AS
SELECT
    search_term,
    SUM(impressions) AS total_impressions,
    SUM(clicks) AS total_clicks,
    SUM(spend) AS total_spend,
    SUM(conversions) AS total_conversions,
    SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS avg_ctr,
    COUNT(DISTINCT date_start) AS days_seen,
    ARRAY_AGG(DISTINCT campaign_name IGNORE NULLS) AS campaigns,
    ARRAY_AGG(DISTINCT keyword_text IGNORE NULLS) AS matched_keywords
FROM `{dataset}.google_ads_search_terms`
WHERE spend > 0
GROUP BY search_term
HAVING SUM(conversions) = 0
    AND SUM(spend) >= 5
ORDER BY total_spend DESC
;

-- ============================================================
-- Phase 4: SEO Views (Search Console)
-- ============================================================

-- 13. vw_seo_opportunities
-- "Striking distance" keywords: position 5-20 with enough impressions
-- These are keywords where we already rank but could
-- reach page 1 with content improvements (supports multiple sites)
CREATE OR REPLACE VIEW `{dataset}.vw_seo_opportunities` AS
WITH recent AS (
    SELECT
        site,
        query,
        page,
        AVG(position) AS avg_position,
        SUM(impressions) AS impressions_30d,
        SUM(clicks) AS clicks_30d,
        SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr
    FROM `{dataset}.search_console_performance`
    WHERE query_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY site, query, page
)
SELECT
    site,
    query,
    page,
    avg_position,
    impressions_30d,
    clicks_30d,
    ctr,
    -- Opportunity score: high impressions + close to page 1 = high score
    ROUND(impressions_30d * (1.0 / avg_position) * 10, 2) AS opportunity_score
FROM recent
WHERE avg_position BETWEEN 5 AND 20
    AND impressions_30d >= 100
ORDER BY opportunity_score DESC
;

-- 14. vw_seo_content_gaps
-- Pages with high impressions but low CTR — ranking well but not getting clicks
-- Indicates bad title/meta description needing optimization
CREATE OR REPLACE VIEW `{dataset}.vw_seo_content_gaps` AS
WITH page_summary AS (
    SELECT
        site,
        page,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS avg_ctr,
        AVG(position) AS avg_position
    FROM `{dataset}.search_console_performance`
    WHERE query_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY site, page
    HAVING SUM(impressions) >= 200
)
SELECT
    site,
    page,
    total_impressions,
    total_clicks,
    avg_ctr,
    avg_position,
    CASE
        WHEN avg_position <= 5 AND avg_ctr < 0.05
            THEN 'optimize_title'
        WHEN avg_position BETWEEN 5 AND 10 AND avg_ctr < 0.03
            THEN 'optimize_meta'
        WHEN avg_position > 10 AND avg_ctr < 0.02
            THEN 'review_content'
        ELSE 'monitor'
    END AS suggested_action
FROM page_summary
WHERE avg_ctr < 0.05
ORDER BY total_impressions DESC
;

-- 15. vw_seo_trends
-- Week-over-week ranking changes to detect drops early
CREATE OR REPLACE VIEW `{dataset}.vw_seo_trends` AS
WITH weekly AS (
    SELECT
        site,
        query,
        page,
        DATE_TRUNC(query_date, WEEK(MONDAY)) AS week_start,
        AVG(position) AS avg_position,
        SUM(clicks) AS total_clicks,
        SUM(impressions) AS total_impressions
    FROM `{dataset}.search_console_performance`
    WHERE query_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
    GROUP BY site, query, page, week_start
),
with_prior AS (
    SELECT
        site,
        query,
        page,
        week_start,
        avg_position AS current_week_position,
        LAG(avg_position) OVER (
            PARTITION BY site, query, page ORDER BY week_start
        ) AS prior_week_position,
        total_clicks AS current_week_clicks,
        total_impressions AS current_week_impressions
    FROM weekly
)
SELECT
    site,
    query,
    page,
    week_start,
    current_week_position,
    prior_week_position,
    ROUND(current_week_position - prior_week_position, 2) AS position_change,
    current_week_clicks,
    current_week_impressions,
    CASE
        WHEN prior_week_position IS NULL THEN 'new'
        WHEN current_week_position - prior_week_position <= -1 THEN 'improving'
        WHEN current_week_position - prior_week_position >= 1 THEN 'declining'
        ELSE 'stable'
    END AS trend
FROM with_prior
WHERE prior_week_position IS NOT NULL
ORDER BY ABS(current_week_position - prior_week_position) DESC
;

-- ============================================================
-- Phase 4D: Cross-Channel Summary
-- ============================================================

-- 16. vw_channel_summary
-- Daily rollup by channel: spend, revenue, ROAS, CPA, orders
-- Single source of truth for cross-channel comparison
CREATE OR REPLACE VIEW `{dataset}.vw_channel_summary` AS
WITH meta_daily AS (
    SELECT
        date_start AS report_date,
        'meta' AS channel,
        SUM(spend) AS spend,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(purchases) AS conversions,
        SUM(purchase_value) AS conversion_value
    FROM `{dataset}.meta_daily_insights`
    GROUP BY date_start
),
google_daily AS (
    SELECT
        date_start AS report_date,
        'google_ads' AS channel,
        SUM(spend) AS spend,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(conversions) AS conversions,
        SUM(conversion_value) AS conversion_value
    FROM `{dataset}.google_ads_daily_insights`
    GROUP BY date_start
),
ga4_organic AS (
    SELECT
        order_date AS report_date,
        'organic' AS channel,
        CAST(NULL AS FLOAT64) AS spend,
        CAST(NULL AS INT64) AS impressions,
        CAST(NULL AS INT64) AS clicks,
        CAST(COUNT(DISTINCT order_id) AS FLOAT64) AS conversions,
        SUM(shopify_revenue) AS conversion_value
    FROM `{dataset}.vw_ga4_attribution`
    WHERE LOWER(COALESCE(ga4_medium, '')) = 'organic'
        AND shopify_revenue IS NOT NULL
    GROUP BY order_date
),
ga4_direct AS (
    SELECT
        order_date AS report_date,
        'direct' AS channel,
        CAST(NULL AS FLOAT64) AS spend,
        CAST(NULL AS INT64) AS impressions,
        CAST(NULL AS INT64) AS clicks,
        CAST(COUNT(DISTINCT order_id) AS FLOAT64) AS conversions,
        SUM(shopify_revenue) AS conversion_value
    FROM `{dataset}.vw_ga4_attribution`
    WHERE (LOWER(COALESCE(ga4_source, '')) = '(direct)' OR ga4_source IS NULL)
        AND shopify_revenue IS NOT NULL
    GROUP BY order_date
),
ga4_referral AS (
    SELECT
        order_date AS report_date,
        'referral' AS channel,
        CAST(NULL AS FLOAT64) AS spend,
        CAST(NULL AS INT64) AS impressions,
        CAST(NULL AS INT64) AS clicks,
        CAST(COUNT(DISTINCT order_id) AS FLOAT64) AS conversions,
        SUM(shopify_revenue) AS conversion_value
    FROM `{dataset}.vw_ga4_attribution`
    WHERE LOWER(COALESCE(ga4_medium, '')) = 'referral'
        AND shopify_revenue IS NOT NULL
    GROUP BY order_date
),
all_channels AS (
    SELECT * FROM meta_daily
    UNION ALL SELECT * FROM google_daily
    UNION ALL SELECT * FROM ga4_organic
    UNION ALL SELECT * FROM ga4_direct
    UNION ALL SELECT * FROM ga4_referral
)
SELECT
    report_date,
    channel,
    spend,
    impressions,
    clicks,
    conversions,
    conversion_value AS revenue,
    SAFE_DIVIDE(conversion_value, spend) AS roas,
    SAFE_DIVIDE(spend, conversions) AS cpa,
    CAST(conversions AS INT64) AS orders
FROM all_channels
;

-- ============================================================
-- Phase 5F: Content Performance Tracking
-- ============================================================

-- 17. vw_content_performance
-- Joins content_posts with Search Console and GA4 data for feedback loop
CREATE OR REPLACE VIEW `{dataset}.vw_content_performance` AS
WITH sc_by_url AS (
    SELECT
        page,
        AVG(position) AS current_position,
        SUM(CASE WHEN query_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN impressions ELSE 0 END) AS impressions_7d,
        SUM(CASE WHEN query_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN clicks ELSE 0 END) AS clicks_7d,
        SAFE_DIVIDE(
            SUM(CASE WHEN query_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN clicks ELSE 0 END),
            SUM(CASE WHEN query_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN impressions ELSE 0 END)
        ) AS ctr
    FROM `{dataset}.search_console_performance`
    WHERE query_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY page
),
ga4_by_page AS (
    SELECT
        CONCAT('https://', (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')) AS page_url,
        COUNT(DISTINCT CASE WHEN event_name = 'session_start' THEN user_pseudo_id END) AS ga4_sessions,
        COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_pseudo_id END) AS ga4_conversions,
        SUM(CASE WHEN event_name = 'purchase' THEN ecommerce.purchase_revenue ELSE 0 END) AS ga4_revenue
    FROM `{ga4_dataset}.events_*`
    WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
        AND event_name IN ('session_start', 'purchase')
    GROUP BY page_url
)
SELECT
    cp.post_id,
    cp.title,
    cp.target_keyword,
    cp.content_type,
    cp.platform,
    cp.status,
    cp.url,
    cp.word_count,
    cp.publish_date,
    DATE_DIFF(CURRENT_DATE(), cp.publish_date, DAY) AS days_since_publish,
    sc.current_position,
    sc.impressions_7d,
    sc.clicks_7d,
    sc.ctr,
    g.ga4_sessions,
    g.ga4_conversions,
    g.ga4_revenue,
    CASE
        WHEN sc.clicks_7d >= 50 AND sc.current_position <= 5 THEN 'top_performer'
        WHEN sc.clicks_7d >= 20 AND sc.current_position <= 10 THEN 'good'
        WHEN sc.impressions_7d >= 100 AND sc.clicks_7d < 5 THEN 'underperforming'
        WHEN DATE_DIFF(CURRENT_DATE(), cp.publish_date, DAY) < 14 THEN 'too_early'
        ELSE 'monitor'
    END AS performance_tier
FROM `{dataset}.content_posts` cp
LEFT JOIN sc_by_url sc ON cp.url = sc.page
LEFT JOIN ga4_by_page g ON cp.url = g.page_url
;

-- ============================================================
-- Phase 6A: Google Ads Copy Performance
-- ============================================================

-- 18. vw_google_ads_copy_performance
-- Asset-level performance for Google RSA headlines and descriptions
CREATE OR REPLACE VIEW `{dataset}.vw_google_ads_copy_performance` AS
SELECT
    ac.ad_id,
    ac.campaign_id,
    ac.campaign_name,
    ac.ad_group_id,
    ac.ad_group_name,
    ac.ad_type,
    ac.asset_type,
    ac.asset_text,
    ac.performance_label,
    -- Aggregate ad-group level performance as proxy
    SUM(gi.spend) AS ad_group_spend,
    SUM(gi.impressions) AS ad_group_impressions,
    SUM(gi.clicks) AS ad_group_clicks,
    SUM(gi.conversions) AS ad_group_conversions,
    SUM(gi.conversion_value) AS ad_group_conversion_value,
    SAFE_DIVIDE(SUM(gi.conversion_value), SUM(gi.spend)) AS ad_group_roas,
    SAFE_DIVIDE(SUM(gi.clicks), SUM(gi.impressions)) AS ad_group_ctr
FROM `{dataset}.google_ads_ad_copy` ac
LEFT JOIN `{dataset}.google_ads_daily_insights` gi
    ON ac.ad_group_id = gi.ad_group_id
    AND gi.date_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY ac.ad_id, ac.campaign_id, ac.campaign_name, ac.ad_group_id,
         ac.ad_group_name, ac.ad_type, ac.asset_type, ac.asset_text,
         ac.performance_label
;

-- ============================================================
-- Product Profitability
-- ============================================================

-- 19. vw_product_profitability
-- Product-level profitability: joins Shopify line items to product COGS data.
-- Includes all items (with or without SKU) so totals match Shopify Analytics.
-- Tuning Clamps are a partnership deal — partner ships & pays back 80% of product price.
-- We keep 40% of product price (20% product share + shipping pass-through), zero COGS.
-- Uses 40% of net revenue as estimated COGS for unmapped/SKU-less items.
-- {product_dataset} is replaced at deploy time.
CREATE OR REPLACE VIEW `{dataset}.vw_product_profitability` AS
WITH line_items AS (
    SELECT
        COALESCE(NULLIF(li.sku, ''), CONCAT('NO_SKU_', REPLACE(li.title, ' ', '_'))) AS sku,
        li.title,
        DATE(o.created_at) AS order_date,
        li.title = 'Tuning Clamps' AS is_partnership,
        SUM(li.quantity) AS units_sold,
        SUM(CAST(li.price AS FLOAT64) * li.quantity) AS gross_revenue,
        SUM(li.total_discount) AS total_discounts,
        -- Partnership items: we only keep 20% of the sale price
        SUM(
            CASE WHEN li.title = 'Tuning Clamps'
                 THEN (CAST(li.price AS FLOAT64) * li.quantity - li.total_discount) * 0.40
                 ELSE CAST(li.price AS FLOAT64) * li.quantity - li.total_discount
            END
        ) AS net_revenue
    FROM `{dataset}.shopify_order_line_items` li
    JOIN `{dataset}.shopify_orders` o ON li.order_id = o.order_id
    GROUP BY sku, li.title, DATE(o.created_at)
),
cogs_lookup AS (
    SELECT
        LOWER(TRIM(sku)) AS sku_key,
        AVG(cogs) AS unit_cost
    FROM `{product_dataset}.amazon_product_map`
    WHERE sku IS NOT NULL AND cogs IS NOT NULL
    GROUP BY sku_key
)
SELECT
    li.sku,
    li.title,
    li.order_date,
    li.units_sold,
    li.gross_revenue,
    li.total_discounts,
    li.net_revenue,
    li.is_partnership,
    -- Partnership = zero COGS; actual COGS from product map; else 40% estimate
    CASE
        WHEN li.is_partnership THEN 0
        WHEN c.unit_cost IS NOT NULL THEN c.unit_cost * li.units_sold
        ELSE li.net_revenue * 0.40
    END AS total_cogs,
    li.net_revenue - CASE
        WHEN li.is_partnership THEN 0
        WHEN c.unit_cost IS NOT NULL THEN c.unit_cost * li.units_sold
        ELSE li.net_revenue * 0.40
    END AS gross_profit,
    SAFE_DIVIDE(
        li.net_revenue - CASE
            WHEN li.is_partnership THEN 0
            WHEN c.unit_cost IS NOT NULL THEN c.unit_cost * li.units_sold
            ELSE li.net_revenue * 0.40
        END,
        li.net_revenue
    ) AS gross_margin,
    CASE
        WHEN li.is_partnership THEN TRUE
        WHEN c.unit_cost IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS has_actual_cogs
FROM line_items li
LEFT JOIN cogs_lookup c ON LOWER(TRIM(li.sku)) = c.sku_key
;
