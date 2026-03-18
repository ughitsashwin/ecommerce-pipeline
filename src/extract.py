"""
extract.py — Data Extraction Module
E-Commerce Data Pipeline Project

Reads raw CSVs from data/raw/, validates them, adds metadata,
and returns a dictionary of DataFrames ready for transformation.
"""

import os
import logging
import pandas as pd
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Expected row counts for validation (update after first run)
EXPECTED_ROW_COUNTS = {
    "orders": 99441,
    "order_items": 112650,
    "customers": 99441,
    "products": 32951,
    "sellers": 3095,
    "payments": 103886,
    "reviews": 100000,
    "geolocation": 1000163,
}

RAW_DATA_PATH = "data/raw"

FILE_MAP = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
}

DTYPE_MAP = {
    "orders": {
        "order_id": str,
        "customer_id": str,
        "order_status": str,
    },
    "order_items": {
        "order_id": str,
        "product_id": str,
        "seller_id": str,
        "order_item_id": int,
        "price": float,
        "freight_value": float,
    },
    "customers": {
        "customer_id": str,
        "customer_unique_id": str,
        "customer_zip_code_prefix": str,
        "customer_city": str,
        "customer_state": str,
    },
    "products": {
        "product_id": str,
        "product_category_name": str,
    },
    "sellers": {
        "seller_id": str,
        "seller_zip_code_prefix": str,
        "seller_city": str,
        "seller_state": str,
    },
    "payments": {
        "order_id": str,
        "payment_sequential": int,
        "payment_type": str,
        "payment_installments": int,
        "payment_value": float,
    },
    "reviews": {
        "review_id": str,
        "order_id": str,
        "review_score": float,
    },
    "geolocation": {
        "geolocation_zip_code_prefix": str,
        "geolocation_city": str,
        "geolocation_state": str,
        "geolocation_lat": float,
        "geolocation_lng": float,
    },
}

DATE_COLUMNS = {
    "orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "reviews": ["review_creation_date", "review_answer_timestamp"],
    "order_items": ["shipping_limit_date"],
}


def read_csv(name: str, filename: str) -> pd.DataFrame:
    """Read a single CSV file with enforced dtypes and date parsing."""
    filepath = os.path.join(RAW_DATA_PATH, filename)

    if not os.path.exists(filepath):
        logger.error(f"File not found: {filepath}")
        raise FileNotFoundError(f"Missing file: {filepath}")

    logger.info(f"Reading {name} from {filepath}...")

    df = pd.read_csv(
        filepath,
        dtype=DTYPE_MAP.get(name, {}),
        parse_dates=DATE_COLUMNS.get(name, []),
        low_memory=False,
    )

    # Add metadata columns
    df["_source_file"] = filename
    df["_ingestion_timestamp"] = datetime.utcnow()

    logger.info(f"  -> {len(df):,} rows loaded")
    return df


def validate_row_counts(dataframes: dict) -> None:
    """Warn if row counts differ significantly from expected values."""
    for name, expected in EXPECTED_ROW_COUNTS.items():
        if name not in dataframes:
            continue
        actual = len(dataframes[name])
        if actual != expected:
            logger.warning(
                f"Row count mismatch for '{name}': "
                f"expected {expected:,}, got {actual:,}"
            )
        else:
            logger.info(f"  OK {name}: {actual:,} rows (as expected)")


def extract() -> dict:
    """
    Main extraction function.
    Returns a dictionary of DataFrames keyed by dataset name.
    """
    logger.info("=" * 50)
    logger.info("EXTRACT PHASE STARTING")
    logger.info("=" * 50)

    dataframes = {}

    for name, filename in FILE_MAP.items():
        try:
            dataframes[name] = read_csv(name, filename)
        except FileNotFoundError:
            logger.warning(f"Skipping '{name}' - file not found.")

    logger.info(f"\nExtraction complete. {len(dataframes)} datasets loaded.")
    validate_row_counts(dataframes)

    return dataframes


if __name__ == "__main__":
    dfs = extract()
    for name, df in dfs.items():
        print(f"{name}: {df.shape}")
