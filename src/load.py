"""
load.py
-------
Loads transformed DataFrames into PostgreSQL using SQLAlchemy.
Idempotent — safe to re-run multiple times.
"""

import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()  # reads DB_URL from .env file

logger = logging.getLogger(__name__)


def get_engine() -> Engine:
    """
    Create a SQLAlchemy engine from the DB_URL environment variable.

    Add this to your .env file:
        DB_URL=postgresql://postgres:secret@localhost:5432/ecommerce
    """
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise EnvironmentError(
            "DB_URL not set. Add it to your .env file:\n"
            "DB_URL=postgresql://postgres:secret@localhost:5432/ecommerce"
        )
    return create_engine(db_url)


def load_dataframe(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    schema: str = "staging",
    if_exists: str = "replace",
) -> None:
    """
    Load a DataFrame into a PostgreSQL table.

    Args:
        df:         DataFrame to load
        table_name: Target table name (without schema prefix)
        engine:     SQLAlchemy engine
        schema:     Target schema (default: "staging")
        if_exists:  "replace" drops & recreates; "append" adds rows
    """
    before_count = len(df)
    logger.info(f"[LOAD] Loading {before_count:,} rows -> {schema}.{table_name}")

    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        chunksize=10_000,
        method="multi",
    )

    # Verify row count after load
    with engine.connect() as conn:
        result = conn.execute(
            text(f'SELECT COUNT(*) FROM {schema}."{table_name}"')
        )
        after_count = result.scalar()

    logger.info(f"[LOAD] {schema}.{table_name}: {after_count:,} rows confirmed in DB")

    if after_count != before_count:
        logger.warning(
            f"[LOAD] Row count mismatch for {table_name}: "
            f"sent {before_count:,}, found {after_count:,}"
        )


def load_all(dataframes: dict, engine: Engine) -> None:
    """
    Load all transformed DataFrames into the staging schema.

    Args:
        dataframes: dict of {table_name: DataFrame}
        engine:     SQLAlchemy engine
    """
    # Ensure staging schema exists
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
        conn.commit()
    logger.info("[LOAD] Staging schema ready")

    for table_name, df in dataframes.items():
        if df is None or df.empty:
            logger.warning(f"[LOAD] Skipping {table_name} — DataFrame is empty or None")
            continue
        load_dataframe(df, table_name, engine)

    logger.info(f"[LOAD] All done — {len(dataframes)} tables loaded into staging")