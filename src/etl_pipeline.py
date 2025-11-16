#!/usr/bin/env python3
"""
Main ETL Pipeline for Irish Property Price Register.

This script orchestrates the complete ETL process including:
- Initial bulk download of historical data
- Incremental monthly updates
- Data extraction, cleaning, and deduplication
- Metadata tracking

Usage:
    python etl_pipeline.py                    # Auto mode
    python etl_pipeline.py --mode initial     # Force initial load
    python etl_pipeline.py --mode incremental # Force incremental update
    python etl_pipeline.py --force-full       # Re-download everything
    python etl_pipeline.py --verbose          # Debug logging
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

# Add src to path if running from project root
sys.path.insert(0, str(Path(__file__).parent))

from config import config
from downloader import download_all_data, download_monthly_data
from extractor import extract_and_validate_all, get_all_csv_files
from logger_config import cleanup_old_logs, get_logger
from merger import (
    generate_data_summary,
    load_existing_data,
    merge_datasets,
    save_consolidated_data,
)
from metadata import (
    get_months_to_update,
    get_pipeline_status,
    load_metadata,
    save_metadata,
    update_metadata_after_run,
)

logger = get_logger(__name__)


def run_initial_load() -> Dict:
    """
    Perform initial bulk download and processing of all historical data.
    
    Returns:
        Dictionary with run summary
    """
    logger.info("=" * 80)
    logger.info("STARTING INITIAL DATA LOAD")
    logger.info("=" * 80)
    
    start_time = time.time()
    run_info = {
        'mode': 'initial',
        'status': 'failed',
        'files_processed': [],
        'errors': []
    }
    
    try:
        # Step 1: Download PPR-ALL.zip
        logger.info("\n[STEP 1/4] Downloading complete historical dataset...")
        zip_file = download_all_data()
        
        if not zip_file:
            error_msg = "Failed to download PPR-ALL.zip"
            logger.error(error_msg)
            run_info['errors'].append(error_msg)
            return run_info
        
        # Step 2: Extract all CSV files
        logger.info("\n[STEP 2/4] Extracting CSV files from archive...")
        csv_files = extract_and_validate_all(zip_file, config.INITIAL_DIR)
        
        if not csv_files:
            error_msg = "No valid CSV files found in archive"
            logger.error(error_msg)
            run_info['errors'].append(error_msg)
            return run_info
        
        logger.info(f"Extracted and validated {len(csv_files)} CSV files")
        
        # Step 3: Merge all data
        logger.info("\n[STEP 3/4] Merging all CSV files...")
        merged_data = merge_datasets(existing=None, new_files=csv_files)
        
        # Step 4: Save consolidated dataset
        logger.info("\n[STEP 4/4] Saving consolidated dataset...")
        save_consolidated_data(merged_data, backup=False)
        
        # Generate summary
        summary = generate_data_summary(merged_data)
        
        # Update run info
        run_info['status'] = 'success'
        run_info['total_records'] = len(merged_data)
        run_info['data_summary'] = summary
        run_info['files_processed'] = [
            {'filename': f.name, 'record_count': 0} for f in csv_files
        ]
        
        # Determine latest month from data
        if summary['latest_date']:
            latest_dt = datetime.strptime(summary['latest_date'], '%Y-%m-%d')
            run_info['latest_month'] = f"{latest_dt.year}-{latest_dt.month:02d}"
        
        elapsed_time = time.time() - start_time
        
        logger.info("\n" + "=" * 80)
        logger.info("INITIAL LOAD COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Total records: {summary['total_records']:,}")
        logger.info(f"Date range: {summary['earliest_date']} to {summary['latest_date']}")
        logger.info(f"Average price: €{summary['average_price_eur']:,.2f}")
        logger.info(f"Counties: {summary['counties']}")
        logger.info(f"Elapsed time: {elapsed_time:.1f}s")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Initial load failed: {e}", exc_info=True)
        run_info['errors'].append(str(e))
    
    return run_info


def run_incremental_update(months_to_download: Optional[list] = None) -> Dict:
    """
    Perform incremental update with latest monthly data.
    
    Args:
        months_to_download: List of (year, month) tuples (auto-detected if None)
    
    Returns:
        Dictionary with run summary
    """
    logger.info("=" * 80)
    logger.info("STARTING INCREMENTAL UPDATE")
    logger.info("=" * 80)
    
    start_time = time.time()
    run_info = {
        'mode': 'incremental',
        'status': 'failed',
        'files_processed': [],
        'errors': []
    }
    
    try:
        # Load metadata to determine what to download
        metadata = load_metadata()
        
        if months_to_download is None:
            months_to_download = get_months_to_update(metadata)
        
        if not months_to_download:
            logger.info("No new months to download")
            run_info['status'] = 'success'
            run_info['message'] = 'Already up to date'
            return run_info
        
        # Step 1: Download monthly files
        logger.info(f"\n[STEP 1/3] Downloading {len(months_to_download)} monthly files...")
        downloaded_files = []
        
        for year, month in months_to_download:
            logger.info(f"Downloading {year}-{month:02d}...")
            file_path = download_monthly_data(year, month)
            
            if file_path:
                downloaded_files.append(file_path)
                run_info['files_processed'].append({
                    'filename': file_path.name,
                    'record_count': 0
                })
            else:
                logger.warning(f"Could not download {year}-{month:02d} (may not be available yet)")
        
        if not downloaded_files:
            error_msg = "No monthly files could be downloaded"
            logger.warning(error_msg)
            run_info['errors'].append(error_msg)
            run_info['status'] = 'partial'
            return run_info
        
        logger.info(f"Successfully downloaded {len(downloaded_files)} files")
        
        # Step 2: Load existing data and merge
        logger.info("\n[STEP 2/3] Merging with existing dataset...")
        existing_data = load_existing_data()
        
        if existing_data is None:
            logger.warning("No existing data found, treating as initial load")
            merged_data = merge_datasets(existing=None, new_files=downloaded_files)
        else:
            merged_data = merge_datasets(existing=existing_data, new_files=downloaded_files)
        
        # Step 3: Save updated dataset
        logger.info("\n[STEP 3/3] Saving updated dataset...")
        save_consolidated_data(merged_data, backup=True)
        
        # Generate summary
        summary = generate_data_summary(merged_data)
        
        # Update run info
        run_info['status'] = 'success'
        run_info['total_records'] = len(merged_data)
        run_info['data_summary'] = summary
        
        # Determine latest month
        if months_to_download:
            latest_year, latest_month = max(months_to_download)
            run_info['latest_month'] = f"{latest_year}-{latest_month:02d}"
        
        elapsed_time = time.time() - start_time
        
        logger.info("\n" + "=" * 80)
        logger.info("INCREMENTAL UPDATE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Files downloaded: {len(downloaded_files)}")
        logger.info(f"Total records: {summary['total_records']:,}")
        logger.info(f"Date range: {summary['earliest_date']} to {summary['latest_date']}")
        logger.info(f"Elapsed time: {elapsed_time:.1f}s")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Incremental update failed: {e}", exc_info=True)
        run_info['errors'].append(str(e))
    
    return run_info


def determine_execution_mode() -> str:
    """
    Determine whether to run initial load or incremental update.
    
    Returns:
        'initial' or 'incremental'
    """
    metadata = load_metadata()
    status = get_pipeline_status(metadata)
    
    if status['needs_initial_load']:
        logger.info("No existing data found - will perform initial load")
        return 'initial'
    else:
        logger.info("Existing data found - will perform incremental update")
        return 'incremental'


def run_pipeline(
    mode: str = 'auto',
    force_full: bool = False,
    verbose: bool = False
) -> Dict:
    """
    Main entry point for ETL pipeline.
    
    Args:
        mode: 'auto', 'initial', or 'incremental'
        force_full: Force full re-download even if data exists
        verbose: Enable debug logging
    
    Returns:
        Dictionary with execution summary
    """
    # Setup logging
    if verbose:
        logger.setLevel('DEBUG')
    
    # Ensure directories exist
    config.ensure_directories()
    
    # Clean up old logs
    cleanup_old_logs()
    
    logger.info("\n" + "=" * 80)
    logger.info("IRISH PROPERTY PRICE REGISTER - ETL PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Mode: {mode}")
    logger.info(f"Force full reload: {force_full}")
    logger.info("=" * 80 + "\n")
    
    # Determine execution mode
    if mode == 'auto':
        if force_full:
            mode = 'initial'
        else:
            mode = determine_execution_mode()
    
    # Execute pipeline
    if mode == 'initial' or force_full:
        run_info = run_initial_load()
    elif mode == 'incremental':
        run_info = run_incremental_update()
    else:
        logger.error(f"Invalid mode: {mode}")
        return {'status': 'failed', 'error': f'Invalid mode: {mode}'}
    
    # Update metadata
    if run_info['status'] in ['success', 'partial']:
        logger.info("\nUpdating metadata...")
        metadata = load_metadata()
        metadata = update_metadata_after_run(metadata, run_info)
        save_metadata(metadata)
    
    # Final summary
    logger.info("\n" + "=" * 80)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Status: {run_info['status'].upper()}")
    logger.info(f"Mode: {run_info['mode']}")
    
    if run_info.get('total_records'):
        logger.info(f"Total records: {run_info['total_records']:,}")
    
    if run_info.get('errors'):
        logger.info(f"Errors: {len(run_info['errors'])}")
        for error in run_info['errors']:
            logger.info(f"  - {error}")
    
    logger.info(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80 + "\n")
    
    return run_info


def main():
    """Command-line interface for the ETL pipeline."""
    parser = argparse.ArgumentParser(
        description='Irish Property Price Register ETL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python etl_pipeline.py                    # Auto mode
  python etl_pipeline.py --mode initial     # Force initial load
  python etl_pipeline.py --mode incremental # Force incremental update
  python etl_pipeline.py --force-full       # Re-download everything
  python etl_pipeline.py --verbose          # Debug logging
        """
    )
    
    parser.add_argument(
        '--mode',
        choices=['auto', 'initial', 'incremental'],
        default='auto',
        help='Execution mode (default: auto)'
    )
    
    parser.add_argument(
        '--force-full',
        action='store_true',
        help='Force full re-download even if data exists'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    # Run pipeline
    result = run_pipeline(
        mode=args.mode,
        force_full=args.force_full,
        verbose=args.verbose
    )
    
    # Exit with appropriate code
    if result['status'] == 'success':
        sys.exit(0)
    elif result['status'] == 'partial':
        sys.exit(1)
    else:
        sys.exit(2)


if __name__ == "__main__":
    main()
