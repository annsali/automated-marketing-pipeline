#!/usr/bin/env python3
"""
Marketing Data Pipeline - Main Entry Point
Runs the complete ETL pipeline end-to-end.

Usage:
    python run_pipeline.py
    python run_pipeline.py --start-date 2024-01-01 --end-date 2024-06-30
"""

import argparse
import sys
from datetime import datetime

from src.logger import setup_logging, get_logger
from src.orchestrator import PipelineOrchestrator
from config import get_default_date_range


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run the Marketing Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python run_pipeline.py                    # Run with default date range (last 18 months)
    python run_pipeline.py --start-date 2024-01-01 --end-date 2024-06-30
        """
    )
    
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for extraction (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for extraction (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    # Setup logging
    setup_logging()
    logger = get_logger("main")
    
    logger.info("=" * 60)
    logger.info("MARKETING DATA PIPELINE")
    logger.info("=" * 60)
    
    # Parse dates
    if args.start_date and args.end_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    else:
        start_date, end_date = get_default_date_range()
        logger.info(f"Using default date range: {start_date.date()} to {end_date.date()}")
    
    # Run pipeline
    orchestrator = PipelineOrchestrator()
    manifest = orchestrator.run_pipeline(start_date, end_date)
    
    # Output summary
    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Run ID: {manifest['run_id']}")
    logger.info(f"Status: {manifest['status']}")
    logger.info(f"Duration: {manifest.get('duration_seconds', 0):.1f} seconds")
    
    if manifest['status'] == "SUCCESS":
        logger.info(f"DQ Score: {manifest.get('dq_score', 'N/A')}")
        logger.info(f"Anomalies Detected: {manifest.get('anomalies_detected', 0)}")
        logger.info(f"Reports Generated: {manifest.get('reports_generated', 0)}")
        logger.info("=" * 60)
        logger.info("Pipeline completed successfully!")
        return 0
    else:
        logger.error(f"Error: {manifest.get('error', 'Unknown error')}")
        logger.info("=" * 60)
        logger.info("Pipeline failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
