"""
Metadata management module for Irish Property Price Register ETL Pipeline.

Handles pipeline state tracking, update detection, and run history.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config import config
from logger_config import get_logger

logger = get_logger(__name__)


def initialize_metadata() -> Dict:
    """
    Create a new metadata structure.
    
    Returns:
        Empty metadata dictionary
    """
    return {
        'last_full_download': None,
        'last_incremental_update': None,
        'last_processed_month': None,
        'total_records': 0,
        'data_coverage': {
            'earliest_date': None,
            'latest_date': None
        },
        'files_processed': [],
        'register_last_updated': '12/11/2025 17:48:21',  # From website
        'pipeline_version': '1.0',
        'created_at': datetime.now().isoformat()
    }


def load_metadata(metadata_path: Optional[Path] = None) -> Dict:
    """
    Load metadata from JSON file.
    
    Args:
        metadata_path: Path to metadata file (default: config.METADATA_FILE)
    
    Returns:
        Metadata dictionary (or new one if file doesn't exist)
    """
    if metadata_path is None:
        metadata_path = config.METADATA_FILE
    
    if not metadata_path.exists():
        logger.info("No existing metadata found, initializing new metadata")
        return initialize_metadata()
    
    try:
        with open(metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        logger.info("Loaded existing metadata")
        logger.debug(f"Last full download: {metadata.get('last_full_download')}")
        logger.debug(f"Last processed month: {metadata.get('last_processed_month')}")
        return metadata
        
    except Exception as e:
        logger.error(f"Failed to load metadata: {e}")
        logger.info("Initializing new metadata")
        return initialize_metadata()


def save_metadata(metadata: Dict, metadata_path: Optional[Path] = None) -> None:
    """
    Save metadata to JSON file.
    
    Args:
        metadata: Metadata dictionary
        metadata_path: Path to metadata file (default: config.METADATA_FILE)
    """
    if metadata_path is None:
        metadata_path = config.METADATA_FILE
    
    # Ensure directory exists
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        logger.info(f"Metadata saved to {metadata_path}")
        
    except Exception as e:
        logger.error(f"Failed to save metadata: {e}")


def get_months_to_update(
    metadata: Dict,
    current_date: Optional[datetime] = None
) -> List[Tuple[int, int]]:
    """
    Calculate which months need to be downloaded.
    
    Args:
        metadata: Current metadata
        current_date: Reference date (default: now)
    
    Returns:
        List of (year, month) tuples that need downloading
    """
    if current_date is None:
        current_date = datetime.now()
    
    months_to_download = []
    
    # Get last processed month
    last_processed = metadata.get('last_processed_month')
    
    if last_processed is None:
        # Never processed, need to download from beginning
        # But since we have PPR-ALL.zip, we'll just download current and previous month
        logger.info("No previous updates found")
        start_date = current_date - timedelta(days=60)  # Go back 2 months to be safe
    else:
        try:
            # Parse last processed month (format: YYYY-MM)
            year, month = map(int, last_processed.split('-'))
            start_date = datetime(year, month, 1)
            logger.info(f"Last processed month: {last_processed}")
        except Exception as e:
            logger.warning(f"Invalid last_processed_month format: {last_processed}, {e}")
            start_date = current_date - timedelta(days=60)
    
    # Generate list of months to download
    # Start from month after last processed
    check_date = start_date + timedelta(days=32)  # Move to next month
    check_date = check_date.replace(day=1)  # First day of month
    
    while check_date <= current_date:
        year = check_date.year
        month = check_date.month
        months_to_download.append((year, month))
        
        # Move to next month
        check_date = (check_date + timedelta(days=32)).replace(day=1)
    
    logger.info(f"Identified {len(months_to_download)} months to download")
    for year, month in months_to_download:
        logger.debug(f"  - {year}-{month:02d}")
    
    return months_to_download


def update_metadata_after_run(
    metadata: Dict,
    run_info: Dict
) -> Dict:
    """
    Update metadata after a successful pipeline run.
    
    Args:
        metadata: Current metadata
        run_info: Information about the run (mode, files processed, summary, etc.)
    
    Returns:
        Updated metadata dictionary
    """
    logger.info("Updating metadata after run...")
    
    # Update timestamps
    now = datetime.now().isoformat()
    
    if run_info.get('mode') == 'initial':
        metadata['last_full_download'] = now
    elif run_info.get('mode') == 'incremental':
        metadata['last_incremental_update'] = now
    
    # Update last processed month
    if run_info.get('latest_month'):
        metadata['last_processed_month'] = run_info['latest_month']
    
    # Update record count
    if run_info.get('total_records') is not None:
        metadata['total_records'] = run_info['total_records']
    
    # Update data coverage
    if run_info.get('data_summary'):
        summary = run_info['data_summary']
        metadata['data_coverage']['earliest_date'] = summary.get('earliest_date')
        metadata['data_coverage']['latest_date'] = summary.get('latest_date')
    
    # Add to files processed (keep last 100)
    if run_info.get('files_processed'):
        for file_info in run_info['files_processed']:
            file_entry = {
                'filename': file_info['filename'],
                'processed_at': now,
                'record_count': file_info.get('record_count', 0)
            }
            metadata['files_processed'].append(file_entry)
        
        # Keep only last 100 files
        metadata['files_processed'] = metadata['files_processed'][-100:]
    
    logger.info("Metadata updated successfully")
    
    return metadata


def get_pipeline_status(metadata: Dict) -> Dict:
    """
    Get a summary of pipeline status.
    
    Args:
        metadata: Current metadata
    
    Returns:
        Status summary dictionary
    """
    status = {
        'has_data': metadata['total_records'] > 0,
        'total_records': metadata['total_records'],
        'data_coverage': metadata['data_coverage'],
        'last_update': metadata.get('last_incremental_update') or metadata.get('last_full_download'),
        'files_processed_count': len(metadata['files_processed']),
        'needs_initial_load': metadata.get('last_full_download') is None
    }
    
    # Check if data is stale (more than 30 days old)
    if status['last_update']:
        try:
            last_update_dt = datetime.fromisoformat(status['last_update'])
            days_since_update = (datetime.now() - last_update_dt).days
            status['days_since_update'] = days_since_update
            status['is_stale'] = days_since_update > 30
        except:
            status['days_since_update'] = None
            status['is_stale'] = False
    else:
        status['days_since_update'] = None
        status['is_stale'] = False
    
    return status


if __name__ == "__main__":
    # Simple test
    logger.info("Testing metadata module...")
    
    # Load or create metadata
    metadata = load_metadata()
    logger.info(f"Metadata: {json.dumps(metadata, indent=2)}")
    
    # Get status
    status = get_pipeline_status(metadata)
    logger.info(f"Pipeline status: {json.dumps(status, indent=2)}")
    
    # Test month calculation
    months = get_months_to_update(metadata)
    logger.info(f"Months to update: {months}")
