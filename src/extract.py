"""
extract.py
----------
Handles extraction of raw CSV files from the data/raw/ directory.
Returns a dictionary of DataFrames ready for transformation.
"""

import os
import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# File map: logical name -> filename in data/raw/
RAW_FILES = {
    "orders":         "olist_orders_dataset.csv",
    "order_items":    "olist_order_items_dataset.csv",
    "customers":      "olist_customers_dataset.csv",
    "products":       "olist_products_dataset.csv",
    "sellers":        "olist_sellers_dataset.csv",
    "payments":       "olist_order_payments_dataset.csv",
    "reviews":        "olist_order_reviews_dataset.csv",
    "geolocation":    "olist_geolocation_dataset.csv",
}

# Expected minimum row counts for validation warnings
MIN_ROW_COUNTS = {
    "orders":       90_000,
    "order_items":  100_000,
    "customers":    90_000,
    "products":     30_000,
    "sellers":      3_000,
    "payments":     100_000,
    "reviews":      90_000,
    "geolocation":  1_000_000,
}


def extract(raw_dir: str = "data/raw") -> dict:
    """
    Read all raw CSV files and return a dictionary of DataFrames.

    Args:
        raw_dir: Path to the folder containing raw CSV files.

    Returns:
        dict mapping logical name to DataFrame, e.g. {"orders": df, ...}
    """
    ingestion_ts = datetime.utcnow().isoformat()
    dataframes = {}

    for name, filename in RAW_FILES.items():
        filepath = os.path.join(raw_dir, filename)

        if not os.path.exists(filepath):
            logger.warning(f"[EXTRACT] File not found, skipping: {filepath}")
            continue

        logger.info(f"[EXTRACT] Reading {filename} ...")
        df = _read_csv(name, filepath)

        # Add metadata columns
        df["_source_file"] = filename
        df["_ingestion_timestamp"] = ingestion_ts

        _validate_row_count(name, df)

        dataframes[name] = df
        logger.info(f"[EXTRACT] {name}: {len(df):,} rows loaded")

    logger.info(f"[EXTRACT] Done — {len(dataframes)} files extracted")
    return dataframes


def _read_csv(name: str, filepath: str) -> pd.DataFrame:
    """Read a CSV with date parsing applied per dataset."""
    date_cols = {
        "orders": [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        "reviews": ["review_creation_date", "review_answer_timestamp"],
    }

    kwargs = {"low_memory": False}
    if name in date_cols:
        kwargs["parse_dates"] = date_cols[name]

    return pd.read_csv(filepath, **kwargs)


def _validate_row_count(name: str, df: pd.DataFrame) -> None:
    """Warn if row count is below the expected minimum."""
    minimum = MIN_ROW_COUNTS.get(name, 0)
    if len(df) < minimum:
        logger.warning(
            f"[EXTRACT] {name} has {len(df):,} rows — "
            f"expected at least {minimum:,}. Check the source file."
        )