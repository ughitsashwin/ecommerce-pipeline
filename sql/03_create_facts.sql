-- =============================================================
-- 03_create_facts.sql
-- E-Commerce Data Pipeline Project
-- Creates the central fact table in the warehouse schema
-- Run AFTER 02_create_dimensions.sql
-- =============================================================

-- ─── fact_orders ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.fact_orders (

    -- Surrogate key
    order_key               SERIAL          PRIMARY KEY,

    -- Natural key (from source)
    order_id                VARCHAR(50)     NOT NULL UNIQUE,

    -- Foreign keys to dimensions
    customer_key            INTEGER         REFERENCES warehouse.dim_customer(customer_key),
    product_key             INTEGER         REFERENCES warehouse.dim_product(product_key),
    seller_key              INTEGER         REFERENCES warehouse.dim_seller(seller_key),
    purchase_date_key       INTEGER         REFERENCES warehouse.dim_date(date_key),
    delivery_date_key       INTEGER         REFERENCES warehouse.dim_date(date_key),

    -- Order status
    order_status            VARCHAR(30),

    -- Timestamps
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP,

    -- Payment info
    payment_type            VARCHAR(30),
    payment_installments    SMALLINT,
    total_payment_value     NUMERIC(10,2),

    -- Financial metrics
    total_order_value       NUMERIC(10,2),
    total_freight_value     NUMERIC(10,2),
    item_count              SMALLINT,

    -- Delivery metrics
    delivery_days           NUMERIC(6,1),
    estimated_delivery_days NUMERIC(6,1),
    is_late                 BOOLEAN         DEFAULT FALSE,

    -- Review metrics
    review_score            NUMERIC(2,1),

    -- Metadata
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP

);

-- ─── Indexes ──────────────────────────────────────────────────

-- FK indexes (important for JOIN performance)
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer_key     ON warehouse.fact_orders (customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product_key      ON warehouse.fact_orders (product_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_seller_key       ON warehouse.fact_orders (seller_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_purchase_date    ON warehouse.fact_orders (purchase_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_delivery_date    ON warehouse.fact_orders (delivery_date_key);

-- Analytical indexes (common filter/group by columns)
CREATE INDEX IF NOT EXISTS idx_fact_orders_status           ON warehouse.fact_orders (order_status);
CREATE INDEX IF NOT EXISTS idx_fact_orders_is_late          ON warehouse.fact_orders (is_late);
CREATE INDEX IF NOT EXISTS idx_fact_orders_review_score     ON warehouse.fact_orders (review_score);
CREATE INDEX IF NOT EXISTS idx_fact_orders_payment_type     ON warehouse.fact_orders (payment_type);

-- Confirm
SELECT
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns c
     WHERE c.table_schema = t.table_schema
     AND c.table_name = t.table_name) AS column_count
FROM information_schema.tables t
WHERE table_schema = 'warehouse'
ORDER BY table_name;