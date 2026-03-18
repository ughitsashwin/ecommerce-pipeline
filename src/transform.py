"""
transform.py — Data Transformation Module
E-Commerce Data Pipeline Project

Contains individual transformation functions for each dataset.
Each function takes a DataFrame in and returns a cleaned DataFrame out.
No side effects — easy to test in isolation.
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Portuguese -> English category name mapping (top categories)
CATEGORY_TRANSLATION = {
    "cama_mesa_banho": "bed_bath_table",
    "beleza_saude": "health_beauty",
    "esporte_lazer": "sports_leisure",
    "informatica_acessorios": "computers_accessories",
    "moveis_decoracao": "furniture_decor",
    "utilidades_domesticas": "housewares",
    "relogios_presentes": "watches_gifts",
    "telefonia": "telephony",
    "automotivo": "auto",
    "brinquedos": "toys",
    "cool_stuff": "cool_stuff",
    "ferramentas_jardim": "garden_tools",
    "perfumaria": "perfumery",
    "bebes": "baby",
    "eletronicos": "electronics",
    "eletrodomesticos": "appliances",
    "livros_tecnicos": "books_technical",
    "fashion_bolsas_e_acessorios": "fashion_bags_accessories",
    "papelaria": "stationery",
    "construcao_ferramentas_seguranca": "construction_tools_safety",
}


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the orders dataset.
    - Drop rows with null order_id
    - Standardize order_status values
    - Parse and validate date columns
    """
    logger.info("Cleaning orders...")
    original_count = len(df)

    # Drop rows without order_id
    df = df.dropna(subset=["order_id"])

    # Standardize status to lowercase stripped strings
    df["order_status"] = df["order_status"].str.strip().str.lower()

    # Valid statuses
    valid_statuses = {
        "delivered", "shipped", "canceled", "unavailable",
        "invoiced", "processing", "created", "approved"
    }
    invalid_mask = ~df["order_status"].isin(valid_statuses)
    if invalid_mask.sum() > 0:
        logger.warning(f"  Found {invalid_mask.sum()} rows with unexpected order_status values")

    logger.info(f"  Orders: {original_count:,} -> {len(df):,} rows after cleaning")
    return df.reset_index(drop=True)


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the customers dataset.
    - Normalize city names (strip whitespace, title case)
    - Remove duplicate customer records
    """
    logger.info("Cleaning customers...")
    original_count = len(df)

    # Normalize city names
    df["customer_city"] = (
        df["customer_city"]
        .str.strip()
        .str.title()
    )

    # Normalize state to uppercase
    df["customer_state"] = df["customer_state"].str.strip().str.upper()

    # Remove duplicates on customer_id
    df = df.drop_duplicates(subset=["customer_id"])

    logger.info(f"  Customers: {original_count:,} -> {len(df):,} rows after cleaning")
    return df.reset_index(drop=True)


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the products dataset.
    - Fill missing category names with 'unknown'
    - Translate Portuguese category names to English
    """
    logger.info("Cleaning products...")

    # Fill missing category names
    df["product_category_name"] = df["product_category_name"].fillna("unknown")

    # Translate category names
    df["product_category_name_english"] = (
        df["product_category_name"]
        .map(CATEGORY_TRANSLATION)
        .fillna(df["product_category_name"])  # keep original if no translation found
    )

    logger.info(f"  Products: {len(df):,} rows cleaned")
    return df.reset_index(drop=True)


def clean_sellers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the sellers dataset.
    - Normalize city and state values
    """
    logger.info("Cleaning sellers...")

    df["seller_city"] = df["seller_city"].str.strip().str.title()
    df["seller_state"] = df["seller_state"].str.strip().str.upper()

    # Remove duplicate seller_ids
    df = df.drop_duplicates(subset=["seller_id"])

    logger.info(f"  Sellers: {len(df):,} rows cleaned")
    return df.reset_index(drop=True)


def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the payments dataset.
    - Remove zero-value payments
    - Validate payment_type enum values
    """
    logger.info("Cleaning payments...")
    original_count = len(df)

    # Remove zero or negative payment values
    df = df[df["payment_value"] > 0]

    # Validate payment type
    valid_types = {"credit_card", "boleto", "voucher", "debit_card", "not_defined"}
    invalid_mask = ~df["payment_type"].isin(valid_types)
    if invalid_mask.sum() > 0:
        logger.warning(f"  Found {invalid_mask.sum()} rows with unexpected payment_type values")

    logger.info(f"  Payments: {original_count:,} -> {len(df):,} rows after cleaning")
    return df.reset_index(drop=True)


def clean_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the reviews dataset.
    - Keep only valid review scores (1-5)
    - Drop duplicates
    """
    logger.info("Cleaning reviews...")
    original_count = len(df)

    # Filter to valid review scores
    df = df[df["review_score"].between(1, 5)]

    # Keep only the latest review per order
    df = df.sort_values("review_answer_timestamp").drop_duplicates(
        subset=["order_id"], keep="last"
    )

    logger.info(f"  Reviews: {original_count:,} -> {len(df):,} rows after cleaning")
    return df.reset_index(drop=True)


def enrich_orders(
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
    payments: pd.DataFrame,
) -> pd.DataFrame:
    """
    Enrich orders with calculated fields:
    - delivery_days: actual days from purchase to delivery
    - estimated_delivery_days: days from purchase to estimated delivery
    - is_late: whether actual delivery was after estimated date
    - total_order_value: sum of item prices + freight
    - total_freight_value: sum of freight costs
    """
    logger.info("Enriching orders...")

    df = orders.copy()

    # Calculate delivery times
    df["delivery_days"] = (
        (df["order_delivered_customer_date"] - df["order_purchase_timestamp"])
        .dt.total_seconds() / 86400
    ).round(1)

    df["estimated_delivery_days"] = (
        (df["order_estimated_delivery_date"] - df["order_purchase_timestamp"])
        .dt.total_seconds() / 86400
    ).round(1)

    # Is late flag (only for delivered orders)
    delivered_mask = df["order_status"] == "delivered"
    df["is_late"] = False
    df.loc[delivered_mask, "is_late"] = (
        df.loc[delivered_mask, "order_delivered_customer_date"]
        > df.loc[delivered_mask, "order_estimated_delivery_date"]
    )

    # Aggregate item values per order
    order_values = order_items.groupby("order_id").agg(
        total_order_value=("price", "sum"),
        total_freight_value=("freight_value", "sum"),
        item_count=("order_item_id", "count"),
    ).reset_index()

    df = df.merge(order_values, on="order_id", how="left")

    # Aggregate payment totals
    payment_totals = payments.groupby("order_id").agg(
        total_payment_value=("payment_value", "sum"),
        payment_installments=("payment_installments", "max"),
        payment_type=("payment_type", "first"),
    ).reset_index()

    df = df.merge(payment_totals, on="order_id", how="left")

    logger.info(f"  Enriched orders: {len(df):,} rows")
    return df.reset_index(drop=True)


def build_date_dimension(start_date: str = "2016-01-01", end_date: str = "2019-12-31") -> pd.DataFrame:
    """
    Generate a complete date dimension table.
    Covers the full range of the Olist dataset.
    """
    logger.info("Building date dimension...")

    dates = pd.date_range(start=start_date, end=end_date, freq="D")

    df = pd.DataFrame({
        "full_date": dates,
        "year": dates.year,
        "quarter": dates.quarter,
        "month": dates.month,
        "month_name": dates.strftime("%B"),
        "day": dates.day,
        "day_of_week": dates.dayofweek,          # 0=Monday, 6=Sunday
        "day_name": dates.strftime("%A"),
        "week_of_year": dates.isocalendar().week.values,
        "is_weekend": dates.dayofweek >= 5,
        "date_key": dates.strftime("%Y%m%d").astype(int),
    })

    logger.info(f"  Date dimension: {len(df):,} rows ({start_date} to {end_date})")
    return df


def transform(dataframes: dict) -> dict:
    """
    Main transformation function.
    Applies all cleaning and enrichment functions.
    Returns a dictionary of transformed DataFrames.
    """
    logger.info("=" * 50)
    logger.info("TRANSFORM PHASE STARTING")
    logger.info("=" * 50)

    transformed = {}

    # Clean individual datasets
    transformed["orders"] = clean_orders(dataframes["orders"])
    transformed["customers"] = clean_customers(dataframes["customers"])
    transformed["products"] = clean_products(dataframes["products"])
    transformed["sellers"] = clean_sellers(dataframes["sellers"])
    transformed["payments"] = clean_payments(dataframes["payments"])
    transformed["reviews"] = clean_reviews(dataframes["reviews"])
    transformed["order_items"] = dataframes["order_items"].copy()

    # Enrich orders with calculated fields
    transformed["orders_enriched"] = enrich_orders(
        transformed["orders"],
        transformed["order_items"],
        transformed["payments"],
    )

    # Build date dimension
    transformed["dim_date"] = build_date_dimension()

    logger.info(f"\nTransformation complete. {len(transformed)} datasets ready.")
    return transformed


if __name__ == "__main__":
    from extract import extract
    raw = extract()
    clean = transform(raw)
    for name, df in clean.items():
        print(f"{name}: {df.shape}")
