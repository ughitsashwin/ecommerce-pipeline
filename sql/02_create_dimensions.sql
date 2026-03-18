-- =============================================================
-- 02_create_dimensions.sql
-- E-Commerce Data Pipeline Project
-- Creates all dimension tables in the warehouse schema
-- =============================================================

-- ─── dim_date ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key        INTEGER         PRIMARY KEY,  -- YYYYMMDD e.g. 20180101
    full_date       DATE            NOT NULL,
    year            SMALLINT        NOT NULL,
    quarter         SMALLINT        NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month           SMALLINT        NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_name      VARCHAR(10)     NOT NULL,
    day             SMALLINT        NOT NULL CHECK (day BETWEEN 1 AND 31),
    day_of_week     SMALLINT        NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),
    day_name        VARCHAR(10)     NOT NULL,
    week_of_year    SMALLINT        NOT NULL,
    is_weekend      BOOLEAN         NOT NULL DEFAULT FALSE
);

-- ─── dim_customer ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_key        SERIAL          PRIMARY KEY,
    customer_id         VARCHAR(50)     NOT NULL UNIQUE,
    customer_unique_id  VARCHAR(50),
    city                VARCHAR(100),
    state               VARCHAR(5),
    zip_prefix          VARCHAR(10),
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- ─── dim_product ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_key             SERIAL          PRIMARY KEY,
    product_id              VARCHAR(50)     NOT NULL UNIQUE,
    category_portuguese     VARCHAR(100),
    category_english        VARCHAR(100),
    weight_g                INTEGER,
    length_cm               NUMERIC(8,2),
    height_cm               NUMERIC(8,2),
    width_cm                NUMERIC(8,2),
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- ─── dim_seller ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_seller (
    seller_key      SERIAL          PRIMARY KEY,
    seller_id       VARCHAR(50)     NOT NULL UNIQUE,
    city            VARCHAR(100),
    state           VARCHAR(5),
    zip_prefix      VARCHAR(10),
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- ─── Indexes ──────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_dim_date_full_date       ON warehouse.dim_date (full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month      ON warehouse.dim_date (year, month);
CREATE INDEX IF NOT EXISTS idx_dim_customer_id          ON warehouse.dim_customer (customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_id           ON warehouse.dim_product (product_id);
CREATE INDEX IF NOT EXISTS idx_dim_seller_id            ON warehouse.dim_seller (seller_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_state       ON warehouse.dim_customer (state);
CREATE INDEX IF NOT EXISTS idx_dim_seller_state         ON warehouse.dim_seller (state);

-- Confirm tables created
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'warehouse'
ORDER BY table_name;