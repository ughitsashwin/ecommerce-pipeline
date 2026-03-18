-- =============================================================
-- 06_create_views.sql
-- E-Commerce Data Pipeline Project
-- Creates analytical views for Power BI / Tableau consumption
-- These are the views your BI tool will connect to directly
-- =============================================================

-- ─── View 1: Monthly Revenue ──────────────────────────────────
-- Answers: "What is the monthly revenue trend?"
CREATE OR REPLACE VIEW warehouse.vw_monthly_revenue AS
SELECT
    dd.year,
    dd.month,
    dd.month_name,
    dd.year || '-' || LPAD(dd.month::TEXT, 2, '0') AS year_month,
    COUNT(DISTINCT fo.order_id)                     AS total_orders,
    SUM(fo.total_order_value)                       AS total_revenue,
    SUM(fo.total_freight_value)                     AS total_freight,
    ROUND(AVG(fo.total_order_value)::NUMERIC, 2)    AS avg_order_value,
    COUNT(DISTINCT fo.customer_key)                 AS unique_customers
FROM warehouse.fact_orders fo
JOIN warehouse.dim_date dd
    ON fo.purchase_date_key = dd.date_key
WHERE fo.order_status = 'delivered'
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;


-- ─── View 2: Category Performance ────────────────────────────
-- Answers: "Which product categories generate the most revenue?"
CREATE OR REPLACE VIEW warehouse.vw_category_performance AS
SELECT
    dp.category_english,
    dp.category_portuguese,
    COUNT(DISTINCT fo.order_id)                     AS total_orders,
    SUM(fo.total_order_value)                       AS total_revenue,
    ROUND(AVG(fo.total_order_value)::NUMERIC, 2)    AS avg_order_value,
    ROUND(AVG(fo.review_score)::NUMERIC, 2)         AS avg_review_score,
    SUM(fo.item_count)                              AS total_items_sold,
    ROUND(
        100.0 * SUM(fo.total_order_value) /
        NULLIF(SUM(SUM(fo.total_order_value)) OVER (), 0),
        2
    ) AS revenue_pct_of_total
FROM warehouse.fact_orders fo
JOIN warehouse.dim_product dp
    ON fo.product_key = dp.product_key
WHERE fo.order_status = 'delivered'
  AND dp.category_english IS NOT NULL
GROUP BY dp.category_english, dp.category_portuguese
ORDER BY total_revenue DESC;


-- ─── View 3: Delivery Performance by State ────────────────────
-- Answers: "Which regions experience the most delivery delays?"
CREATE OR REPLACE VIEW warehouse.vw_delivery_by_state AS
SELECT
    dc.state,
    COUNT(DISTINCT fo.order_id)                         AS total_orders,
    ROUND(AVG(fo.delivery_days)::NUMERIC, 1)            AS avg_delivery_days,
    ROUND(AVG(fo.estimated_delivery_days)::NUMERIC, 1)  AS avg_estimated_days,
    SUM(fo.is_late::INT)                                AS late_orders,
    ROUND(
        100.0 * SUM(fo.is_late::INT) / NULLIF(COUNT(*), 0),
        1
    ) AS late_delivery_pct,
    ROUND(AVG(fo.review_score)::NUMERIC, 2)             AS avg_review_score
FROM warehouse.fact_orders fo
JOIN warehouse.dim_customer dc
    ON fo.customer_key = dc.customer_key
WHERE fo.order_status = 'delivered'
  AND fo.delivery_days IS NOT NULL
GROUP BY dc.state
ORDER BY late_delivery_pct DESC;


-- ─── View 4: Seller Performance ──────────────────────────────
-- Answers: "Which sellers have the highest volume and best ratings?"
CREATE OR REPLACE VIEW warehouse.vw_seller_performance AS
SELECT
    ds.seller_id,
    ds.city                                             AS seller_city,
    ds.state                                            AS seller_state,
    COUNT(DISTINCT fo.order_id)                         AS total_orders,
    SUM(fo.total_order_value)                           AS total_revenue,
    ROUND(AVG(fo.review_score)::NUMERIC, 2)             AS avg_review_score,
    ROUND(AVG(fo.delivery_days)::NUMERIC, 1)            AS avg_delivery_days,
    SUM(fo.is_late::INT)                                AS late_orders,
    ROUND(
        100.0 * SUM(fo.is_late::INT) / NULLIF(COUNT(*), 0),
        1
    ) AS late_delivery_pct
FROM warehouse.fact_orders fo
JOIN warehouse.dim_seller ds
    ON fo.seller_key = ds.seller_key
WHERE fo.order_status = 'delivered'
GROUP BY ds.seller_id, ds.city, ds.state
ORDER BY total_orders DESC;


-- ─── View 5: Customer Review Summary ─────────────────────────
-- Answers: "What is the review score distribution?"
CREATE OR REPLACE VIEW warehouse.vw_review_summary AS
SELECT
    fo.review_score,
    COUNT(*)                                            AS order_count,
    ROUND(
        100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0),
        1
    ) AS pct_of_total,
    ROUND(AVG(fo.delivery_days)::NUMERIC, 1)            AS avg_delivery_days,
    ROUND(AVG(fo.total_order_value)::NUMERIC, 2)        AS avg_order_value
FROM warehouse.fact_orders fo
WHERE fo.review_score IS NOT NULL
  AND fo.order_status = 'delivered'
GROUP BY fo.review_score
ORDER BY fo.review_score DESC;


-- ─── View 6: Payment Method Breakdown ────────────────────────
-- Answers: "What is the payment method mix and installment usage?"
CREATE OR REPLACE VIEW warehouse.vw_payment_breakdown AS
SELECT
    fo.payment_type,
    COUNT(DISTINCT fo.order_id)                         AS total_orders,
    SUM(fo.total_payment_value)                         AS total_value,
    ROUND(AVG(fo.total_payment_value)::NUMERIC, 2)      AS avg_order_value,
    ROUND(AVG(fo.payment_installments)::NUMERIC, 1)     AS avg_installments,
    ROUND(
        100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0),
        1
    ) AS pct_of_orders
FROM warehouse.fact_orders fo
WHERE fo.payment_type IS NOT NULL
GROUP BY fo.payment_type
ORDER BY total_orders DESC;


-- ─── View 7: Executive KPI Summary ───────────────────────────
-- One-row summary for KPI cards on the executive dashboard
CREATE OR REPLACE VIEW warehouse.vw_executive_kpis AS
SELECT
    COUNT(DISTINCT order_id)                            AS total_orders,
    ROUND(SUM(total_order_value)::NUMERIC, 2)           AS total_revenue,
    ROUND(AVG(total_order_value)::NUMERIC, 2)           AS avg_order_value,
    ROUND(AVG(delivery_days)::NUMERIC, 1)               AS avg_delivery_days,
    ROUND(AVG(review_score)::NUMERIC, 2)                AS avg_review_score,
    ROUND(
        100.0 * SUM(is_late::INT) / NULLIF(COUNT(*), 0),
        1
    ) AS overall_late_pct,
    COUNT(DISTINCT customer_key)                        AS total_customers
FROM warehouse.fact_orders
WHERE order_status = 'delivered';


-- ─── Confirm all views created ───────────────────────────────
SELECT table_name AS view_name
FROM information_schema.views
WHERE table_schema = 'warehouse'
ORDER BY table_name;