"""
load.py — Data Loading Module
E-Commerce Data Pipeline Project

Loads transformed DataFrames into PostgreSQL using SQLAlchemy.
Implements upsert logic for idempotency.
Loads to staging schema first, then promotes to warehouse schema.
"""

import logging
import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def get_engine():
    """Create and return a SQLAlchemy engine from environment variables."""

    # Always load .env from the project root (two levels up from src/load.py)
    env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(dotenv_path=env_path, override=True)

    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        # Fall back to building from individual env vars
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        user = os.getenv("DB_USER", os.environ.get("USER", "postgres"))
        password = os.getenv("DB_PASSWORD", "")
        dbname = os.getenv("DB_NAME", "ecommerce")
        if password:
            db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        else:
            db_url = f"postgresql+psycopg2://{user}@{host}:{port}/{dbname}"

    logger.info("Connecting to database...")
    engine = create_engine(db_url, echo=False)
    logger.info("Database engine created successfully")
    return engine


def create_schemas(engine) -> None:
    """Create staging and warehouse schemas if they don't exist."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS warehouse"))
        conn.commit()
    logger.info("Schemas verified: staging, warehouse")


def load_to_staging(df: pd.DataFrame, table_name: str, engine) -> int:
    """
    Load a DataFrame to the staging schema.
    Uses replace strategy — idempotent, safe to re-run.
    Returns row count loaded.
    """
    full_table = f"staging.{table_name}"
    logger.info(f"Loading {len(df):,} rows to {full_table}...")

    df.to_sql(
        name=table_name,
        con=engine,
        schema="staging",
        if_exists="replace",
        index=False,
        chunksize=10000,
        method="multi",
    )

    # Verify row count after load
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {full_table}"))
        loaded_count = result.scalar()

    logger.info(f"  -> {loaded_count:,} rows confirmed in {full_table}")

    if loaded_count != len(df):
        raise ValueError(
            f"Row count mismatch after load: "
            f"expected {len(df):,}, found {loaded_count:,}"
        )

    return loaded_count


def load_all_staging(transformed: dict, engine) -> dict:
    """
    Load all transformed DataFrames to staging schema.
    Returns a summary dict of {table_name: row_count}.
    """
    logger.info("=" * 50)
    logger.info("LOADING TO STAGING SCHEMA")
    logger.info("=" * 50)

    staging_tables = {
        "orders_enriched": "stg_orders",
        "customers": "stg_customers",
        "products": "stg_products",
        "sellers": "stg_sellers",
        "payments": "stg_payments",
        "reviews": "stg_reviews",
        "order_items": "stg_order_items",
        "dim_date": "stg_dim_date",
    }

    summary = {}

    for dataset_name, table_name in staging_tables.items():
        if dataset_name not in transformed:
            logger.warning(f"Dataset '{dataset_name}' not found — skipping")
            continue

        df = transformed[dataset_name]

        # Drop internal metadata columns before loading
        df_clean = df.drop(
            columns=[c for c in df.columns if c.startswith("_")],
            errors="ignore"
        )

        try:
            count = load_to_staging(df_clean, table_name, engine)
            summary[table_name] = count
        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
            raise

    return summary


def log_summary(summary: dict) -> None:
    """Print a formatted summary of all loaded tables."""
    logger.info("\n" + "=" * 50)
    logger.info("LOAD SUMMARY")
    logger.info("=" * 50)
    total_rows = 0
    for table, count in summary.items():
        logger.info(f"  {table:<30} {count:>10,} rows")
        total_rows += count
    logger.info(f"  {'TOTAL':<30} {total_rows:>10,} rows")
    logger.info("=" * 50)


def load(transformed: dict, dry_run: bool = False) -> dict:
    """
    Main load function.
    Connects to PostgreSQL, creates schemas, and loads all staging tables.
    """
    if dry_run:
        logger.info("DRY RUN MODE — skipping database load")
        return {name: len(df) for name, df in transformed.items()}

    engine = get_engine()
    create_schemas(engine)
    summary = load_all_staging(transformed, engine)
    log_summary(summary)

    return summary


if __name__ == "__main__":
    from extract import extract
    from transform import transform

    raw = extract()
    clean = transform(raw)
    summary = load(clean)
    print(summary)
