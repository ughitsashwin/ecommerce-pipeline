"""
pipeline.py — Pipeline Orchestrator
E-Commerce Data Pipeline Project

Runs the full ETL pipeline: Extract -> Transform -> Load
Supports --dry-run flag to skip the database load step.

Usage:
    python src/pipeline.py             # Full pipeline
    python src/pipeline.py --dry-run   # Extract + Transform only
"""

import argparse
import logging
import sys
import time
from datetime import datetime

# Configure logging before imports
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    ],
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="E-Commerce Data Pipeline"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run extract and transform but skip database load",
    )
    parser.add_argument(
        "--phase",
        choices=["extract", "transform", "load", "all"],
        default="all",
        help="Which phase to run (default: all)",
    )
    return parser.parse_args()


def run_pipeline(dry_run: bool = False, phase: str = "all"):
    """
    Main pipeline orchestrator.
    Runs each phase with error handling and timing.
    """
    start_time = time.time()

    logger.info("=" * 60)
    logger.info("E-COMMERCE DATA PIPELINE STARTING")
    logger.info(f"Mode: {'DRY RUN' if dry_run else 'FULL RUN'}")
    logger.info(f"Phase: {phase.upper()}")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # Import here to keep startup fast even if imports are slow
    from extract import extract
    from transform import transform
    from load import load

    raw_data = None
    transformed_data = None
    load_summary = None

    # --- EXTRACT ---
    if phase in ("extract", "all"):
        logger.info("\n--- PHASE 1: EXTRACT ---")
        extract_start = time.time()
        try:
            raw_data = extract()
            extract_time = time.time() - extract_start
            logger.info(f"Extract completed in {extract_time:.1f}s")
        except Exception as e:
            logger.error(f"EXTRACT FAILED: {e}")
            logger.info("Aborting pipeline — extraction artifacts preserved")
            sys.exit(1)

    # --- TRANSFORM ---
    if phase in ("transform", "all") and raw_data is not None:
        logger.info("\n--- PHASE 2: TRANSFORM ---")
        transform_start = time.time()
        try:
            transformed_data = transform(raw_data)
            transform_time = time.time() - transform_start
            logger.info(f"Transform completed in {transform_time:.1f}s")
        except Exception as e:
            logger.error(f"TRANSFORM FAILED: {e}")
            logger.info("Aborting pipeline — raw data preserved in staging")
            sys.exit(1)

    # --- LOAD ---
    if phase in ("load", "all") and transformed_data is not None:
        logger.info("\n--- PHASE 3: LOAD ---")
        load_start = time.time()
        try:
            load_summary = load(transformed_data, dry_run=dry_run)
            load_time = time.time() - load_start
            if dry_run:
                logger.info(f"Dry run completed in {load_time:.1f}s (no data written)")
            else:
                logger.info(f"Load completed in {load_time:.1f}s")
        except Exception as e:
            logger.error(f"LOAD FAILED: {e}")
            logger.info("Transform artifacts preserved — fix load issue and retry")
            sys.exit(1)

    # --- SUMMARY ---
    total_time = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"Total runtime: {total_time:.1f}s ({total_time/60:.1f} min)")
    logger.info("=" * 60)

    return load_summary


if __name__ == "__main__":
    import os
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)

    args = parse_args()
    run_pipeline(dry_run=args.dry_run, phase=args.phase)
