-- =============================================================
-- 05_load_facts.sql
-- E-Commerce Data Pipeline Project
-- Loads the fact_orders table by joining staging tables
-- and looking up surrogate keys from dimension tables
-- Run AFTER 04_load_dimensions.sql
-- =============================================================

-- Truncate fact table before reload
TRUNCATE TABLE warehouse.fact_orders;

-- ─── Load fact_orders ─────────────────────────────────────────
-- Join staging orders with:
--   - staging order items (for product + seller info)
--   - staging reviews (for review score)
--   - dimension tables (to get surrogate keys)
INSERT INTO warehouse.fact_orders (
    order_id,
    customer_key,
    product_key,
    seller_key,
    purchase_date_key,
    delivery_date_key,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    payment_type,
    payment_installments,
    total_payment_value,
    total_order_value,
    total_freight_value,
    item_count,
    delivery_days,
    estimated_delivery_days,
    is_late,
    review_score
)
SELECT
    o.order_id,

    -- Surrogate keys from dimensions
    dc.customer_key,
    dp.product_key,
    ds.seller_key,

    -- Date keys (cast purchase/delivery dates to YYYYMMDD integer)
    TO_CHAR(o.order_purchase_timestamp, 'YYYYMMDD')::INTEGER    AS purchase_date_key,
    TO_CHAR(o.order_delivered_customer_date, 'YYYYMMDD')::INTEGER AS delivery_date_key,

    -- Order info
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- Payment info
    o.payment_type,
    o.payment_installments::SMALLINT,
    o.total_payment_value,

    -- Financial metrics
    o.total_order_value,
    o.total_freight_value,
    o.item_count::SMALLINT,

    -- Delivery metrics
    o.delivery_days,
    o.estimated_delivery_days,
    o.is_late::BOOLEAN,

    -- Review score (from latest review)
    r.review_score

FROM staging.stg_orders o

-- Join to get customer surrogate key
LEFT JOIN warehouse.dim_customer dc
    ON o.customer_id = dc.customer_id

-- Join to get most common product per order (first item)
LEFT JOIN (
    SELECT DISTINCT ON (order_id)
        order_id,
        product_id,
        seller_id
    FROM staging.stg_order_items
    ORDER BY order_id, order_item_id ASC
) oi ON o.order_id = oi.order_id

LEFT JOIN warehouse.dim_product dp
    ON oi.product_id = dp.product_id

LEFT JOIN warehouse.dim_seller ds
    ON oi.seller_id = ds.seller_id

-- Join to get review score
LEFT JOIN staging.stg_reviews r
    ON o.order_id = r.order_id

ON CONFLICT (order_id) DO UPDATE SET
    order_status                    = EXCLUDED.order_status,
    delivery_days                   = EXCLUDED.delivery_days,
    is_late                         = EXCLUDED.is_late,
    review_score                    = EXCLUDED.review_score,
    total_order_value               = EXCLUDED.total_order_value;

-- ─── Row count check ──────────────────────────────────────────
SELECT
    'fact_orders' AS table_name,
    COUNT(*)      AS rows_loaded
FROM warehouse.fact_orders;

-- ─── Sanity check: orphan keys ────────────────────────────────
-- Orders with no matching customer (should be 0)
SELECT 'orphan_customer_keys' AS check_name, COUNT(*) AS count
FROM warehouse.fact_orders
WHERE customer_key IS NULL;

-- Orders with no matching product (some expected — multi-item orders)
SELECT 'null_product_keys' AS check_name, COUNT(*) AS count
FROM warehouse.fact_orders
WHERE product_key IS NULL;

-- Orders with no purchase date key match in dim_date (should be 0)
SELECT 'missing_purchase_date_keys' AS check_name, COUNT(*) AS count
FROM warehouse.fact_orders fo
LEFT JOIN warehouse.dim_date dd ON fo.purchase_date_key = dd.date_key
WHERE fo.purchase_date_key IS NOT NULL
  AND dd.date_key IS NULL;