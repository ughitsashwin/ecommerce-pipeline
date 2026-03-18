"""
transform.py
------------
All cleaning and enrichment logic for the e-commerce pipeline.
Each function takes a DataFrame in and returns a DataFrame out — no side effects.
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

CATEGORY_TRANSLATION = {
    "cama_mesa_banho":            "bed_bath_table",
    "beleza_saude":               "health_beauty",
    "esporte_lazer":              "sports_leisure",
    "moveis_decoracao":           "furniture_decor",
    "informatica_acessorios":     "computers_accessories",
    "utilidades_domesticas":      "housewares",
    "relogios_presentes":         "watches_gifts",
    "telefonia":                  "telephony",
    "ferramentas_jardim":         "garden_tools",
    "automotivo":                 "auto",
    "brinquedos":                 "toys",
    "perfumaria":                 "perfumery",
    "bebes":                      "baby",
    "eletronicos":                "electronics",
    "eletrodomesticos":           "appliances",
    "livros_tecnicos":            "technical_books",
    "musica":                     "music",
    "papelaria":                  "stationery",
}


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean orders:
    - Drop rows with null order_id
    - Standardise order_status to lowercase
    - Ensure all timestamp columns are datetime
    """
    logger.info("[TRANSFORM] clean_orders start — rows: {:,}".format(len(df)))
    df = df.copy()

    before = len(df)
    df = df.dropna(subset=["order_id"])
    logger.info(f"[TRANSFORM] clean_orders: dropped {before - len(df):,} null order_id rows")

    df["order_status"] = df["order_status"].str.strip().str.lower()

    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    logger.info("[TRANSFORM] clean_orders done — rows: {:,}".format(len(df)))
    return df


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean customers:
    - Normalise city names (strip, title case)
    - Remove duplicate customer_ids
    """
    logger.info("[TRANSFORM] clean_customers start — rows: {:,}".format(len(df)))
    df = df.copy()

    if "customer_city" in df.columns:
        df["customer_city"] = df["customer_city"].str.strip().str.title()
    if "customer_state" in df.columns:
        df["customer_state"] = df["customer_state"].str.strip().str.upper()

    before = len(df)
    df = df.drop_duplicates(subset=["customer_id"])
    logger.info(f"[TRANSFORM] clean_customers: removed {before - len(df):,} duplicates")

    logger.info("[TRANSFORM] clean_customers done — rows: {:,}".format(len(df)))
    return df


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean products:
    - Fill missing category names with 'unknown'
    - Translate Portuguese category names to English
    """
    logger.info("[TRANSFORM] clean_products start — rows: {:,}".format(len(df)))
    df = df.copy()

    if "product_category_name" in df.columns:
        df["product_category_name"] = df["product_category_name"].fillna("unknown")
        df["product_category_name_english"] = (
            df["product_category_name"]
            .map(CATEGORY_TRANSLATION)
            .fillna(df["product_category_name"])
        )

    logger.info("[TRANSFORM] clean_products done — rows: {:,}".format(len(df)))
    return df


def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean payments:
    - Remove zero-value rows
    - Warn on unexpected payment_type values
    """
    logger.info("[TRANSFORM] clean_payments start — rows: {:,}".format(len(df)))
    df = df.copy()

    before = len(df)
    df = df[df["payment_value"] > 0]
    logger.info(f"[TRANSFORM] clean_payments: removed {before - len(df):,} zero-value rows")

    valid_types = {"credit_card", "boleto", "voucher", "debit_card", "not_defined"}
    invalid_mask = ~df["payment_type"].isin(valid_types)
    if invalid_mask.any():
        logger.warning(
            f"[TRANSFORM] {invalid_mask.sum():,} rows with unexpected payment_type: "
            f"{df.loc[invalid_mask, 'payment_type'].unique()}"
        )

    logger.info("[TRANSFORM] clean_payments done — rows: {:,}".format(len(df)))
    return df


def enrich_orders(orders: pd.DataFrame, order_items: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich orders with:
    - delivery_days: days from purchase to delivery
    - is_late: 1 if delivered after estimated date
    - total_order_value and total_freight from order_items
    """
    logger.info("[TRANSFORM] enrich_orders start")
    orders = orders.copy()

    orders["delivery_days"] = (
        orders["order_delivered_customer_date"] - orders["order_purchase_timestamp"]
    ).dt.days

    orders["is_late"] = (
        orders["order_delivered_customer_date"] > orders["order_estimated_delivery_date"]
    ).astype(int)

    if order_items is not None and not order_items.empty:
        order_value = (
            order_items
            .groupby("order_id")
            .agg(
                total_order_value=("price", "sum"),
                total_freight=("freight_value", "sum")
            )
            .reset_index()
        )
        orders = orders.merge(order_value, on="order_id", how="left")
    else:
        orders["total_order_value"] = np.nan
        orders["total_freight"] = np.nan

    logger.info("[TRANSFORM] enrich_orders done — rows: {:,}".format(len(orders)))
    return orders


def build_date_dimension(start: str = "2016-01-01", end: str = "2019-12-31") -> pd.DataFrame:
    """
    Generate a dim_date table for the full dataset date range.
    Returns columns: date_key, full_date, year, quarter, month,
                     month_name, day_of_week, day_name, is_weekend
    """
    logger.info(f"[TRANSFORM] build_date_dimension: {start} to {end}")
    dates = pd.date_range(start=start, end=end, freq="D")
    df = pd.DataFrame({"full_date": dates})

    df["date_key"]    = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["year"]        = df["full_date"].dt.year
    df["quarter"]     = df["full_date"].dt.quarter
    df["month"]       = df["full_date"].dt.month
    df["month_name"]  = df["full_date"].dt.strftime("%B")
    df["day_of_week"] = df["full_date"].dt.dayofweek
    df["day_name"]    = df["full_date"].dt.strftime("%A")
    df["is_weekend"]  = (df["day_of_week"] >= 5).astype(int)

    logger.info(f"[TRANSFORM] build_date_dimension done — {len(df):,} rows")
    return df