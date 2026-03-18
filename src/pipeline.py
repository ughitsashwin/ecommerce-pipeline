"""
pipeline.py
-----------
Orchestrator — runs the full Extract -> Transform -> Load pipeline.

Usage:
    python src/pipeline.py              # full run
    python src/pipeline.py --dry-run    # extract + transform only, skip DB load
"""

import argparse
import logging
import sys
import time

from extract import extract
from transform import (
    clean_orders,
    clean_customers,
    clean_products,
    clean_payments,
    enrich_orders,
    build_date_dimension,
)
from load import get_engine, load_all

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log"),
    ],
)
logger = logging.getLogger(__name__)


def run(dry_run: bool = False) -> None:
    """
    Execute the full pipeline.

    Args:
        dry_run: If True, skip the database load step.
    """
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("PIPELINE START" + (" [DRY RUN]" if dry_run else ""))
    logger.info("=" * 60)

    # ── EXTRACT ──────────────────────────────────────────────────────────────
    logger.info("--- PHASE 1: EXTRACT ---")
    try:
        raw = extract(raw_dir="data/raw")
    except Exception as e:
        logger.error(f"[EXTRACT] Failed: {e}")
        raise

    # ── TRANSFORM ────────────────────────────────────────────────────────────
    logger.info("--- PHASE 2: TRANSFORM ---")
    try:
        transformed = {}

        transformed["stg_orders"] = enrich_orders(
            clean_orders(raw.get("orders")),
            raw.get("order_items"),
        )
        transformed["stg_customers"]   = clean_customers(raw.get("customers"))
        transformed["stg_products"]    = clean_products(raw.get("products"))
        transformed["stg_payments"]    = clean_payments(raw.get("payments"))
        transformed["stg_sellers"]     = raw.get("sellers")       # no cleaning needed
        transformed["stg_reviews"]     = raw.get("reviews")       # used as-is
        transformed["stg_order_items"] = raw.get("order_items")   # used as-is
        transformed["stg_dim_date"]    = build_date_dimension()

    except Exception as e:
        logger.error(f"[TRANSFORM] Failed: {e}")
        # Extraction artifacts preserved — only transform failed
        raise

    # Log summary
    for name, df in transformed.items():
        if df is not None:
            logger.info(f"[TRANSFORM] {name}: {len(df):,} rows ready")

    # ── LOAD ─────────────────────────────────────────────────────────────────
    if dry_run:
        logger.info("--- PHASE 3: LOAD (SKIPPED — dry run) ---")
    else:
        logger.info("--- PHASE 3: LOAD ---")
        try:
            engine = get_engine()
            load_all(transformed, engine)
        except Exception as e:
            logger.error(f"[LOAD] Failed: {e}")
            raise

    # ── DONE ─────────────────────────────────────────────────────────────────
    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE in {elapsed:.1f}s" + (" [DRY RUN]" if dry_run else ""))
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-Commerce Data Pipeline")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run extract + transform only. Skip database load.",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)