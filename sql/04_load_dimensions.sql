-- =============================================================
-- 04_load_dimensions.sql
-- E-Commerce Data Pipeline Project
-- Loads dimension tables from staging schema
-- Run AFTER 03_create_facts.sql and after Python pipeline
-- has loaded data into staging tables
-- =============================================================

-- ─── Load dim_date ────────────────────────────────────────────
-- Truncate and reload (date dim is fully regenerated each run)
TRUNCATE TABLE warehouse.dim_date;

INSERT INTO warehouse.dim_date (
    date_key,
    full_date,
    year,
    quarter,
    month,
    month_name,
    day,
    day_of_week,
    day_name,
    week_of_year,
    is_weekend
)
SELECT
    date_key,
    full_date::DATE,
    year::SMALLINT,
    quarter::SMALLINT,
    month::SMALLINT,
    month_name,
    day::SMALLINT,
    day_of_week::SMALLINT,
    day_name,
    week_of_year::SMALLINT,
    is_weekend::BOOLEAN
FROM staging.stg_dim_date
ON CONFLICT (date_key) DO NOTHING;

SELECT 'dim_date' AS table_name, COUNT(*) AS rows_loaded FROM warehouse.dim_date;

-- ─── Load dim_customer ────────────────────────────────────────
TRUNCATE TABLE warehouse.dim_customer CASCADE;

INSERT INTO warehouse.dim_customer (
    customer_id,
    customer_unique_id,
    city,
    state,
    zip_prefix
)
SELECT
    customer_id,
    customer_unique_id,
    customer_city       AS city,
    customer_state      AS state,
    customer_zip_code_prefix AS zip_prefix
FROM staging.stg_customers
ON CONFLICT (customer_id) DO UPDATE SET
    customer_unique_id  = EXCLUDED.customer_unique_id,
    city                = EXCLUDED.city,
    state               = EXCLUDED.state,
    zip_prefix          = EXCLUDED.zip_prefix;

SELECT 'dim_customer' AS table_name, COUNT(*) AS rows_loaded FROM warehouse.dim_customer;

-- ─── Load dim_product ─────────────────────────────────────────
TRUNCATE TABLE warehouse.dim_product CASCADE;

INSERT INTO warehouse.dim_product (
    product_id,
    category_portuguese,
    category_english,
    weight_g,
    length_cm,
    height_cm,
    width_cm
)
SELECT
    product_id,
    product_category_name           AS category_portuguese,
    product_category_name_english   AS category_english,
    product_weight_g                AS weight_g,
    product_length_cm               AS length_cm,
    product_height_cm               AS height_cm,
    product_width_cm                AS width_cm
FROM staging.stg_products
ON CONFLICT (product_id) DO UPDATE SET
    category_portuguese = EXCLUDED.category_portuguese,
    category_english    = EXCLUDED.category_english,
    weight_g            = EXCLUDED.weight_g;

SELECT 'dim_product' AS table_name, COUNT(*) AS rows_loaded FROM warehouse.dim_product;

-- ─── Load dim_seller ──────────────────────────────────────────
TRUNCATE TABLE warehouse.dim_seller CASCADE;

INSERT INTO warehouse.dim_seller (
    seller_id,
    city,
    state,
    zip_prefix
)
SELECT
    seller_id,
    seller_city         AS city,
    seller_state        AS state,
    seller_zip_code_prefix AS zip_prefix
FROM staging.stg_sellers
ON CONFLICT (seller_id) DO UPDATE SET
    city        = EXCLUDED.city,
    state       = EXCLUDED.state,
    zip_prefix  = EXCLUDED.zip_prefix;

SELECT 'dim_seller' AS table_name, COUNT(*) AS rows_loaded FROM warehouse.dim_seller;

-- ─── Final row count summary ──────────────────────────────────
SELECT 'DIMENSION LOAD COMPLETE' AS status;

SELECT table_name, row_count
FROM (
    SELECT 'dim_date'       AS table_name, COUNT(*) AS row_count FROM warehouse.dim_date
    UNION ALL
    SELECT 'dim_customer',  COUNT(*) FROM warehouse.dim_customer
    UNION ALL
    SELECT 'dim_product',   COUNT(*) FROM warehouse.dim_product
    UNION ALL
    SELECT 'dim_seller',    COUNT(*) FROM warehouse.dim_seller
) summary
ORDER BY table_name;