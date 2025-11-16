"""
Extraction module for Irish Property Price Register ETL Pipeline.

Handles ZIP file extraction, CSV validation, and file metadata extraction.
"""

import zipfile
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from config import config
from logger_config import get_logger

logger = get_logger(__name__)


class ExtractionError(Exception):
    """Raised when extraction fails."""
    pass


def extract_zip(zip_path: Path, extract_to: Optional[Path] = None) -> List[Path]:
    """
    Extract all CSV files from a ZIP archive.
    
    Args:
        zip_path: Path to ZIP file
        extract_to: Directory to extract to (default: same as zip file)
    
    Returns:
        List of paths to extracted CSV files
    
    Raises:
        ExtractionError: If extraction fails
    """
    if extract_to is None:
        extract_to = zip_path.parent
    
    extract_to.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Extracting {zip_path.name}...")
    
    extracted_files = []
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Get list of files in zip
            file_list = zip_ref.namelist()
            csv_files = [f for f in file_list if f.lower().endswith('.csv')]
            
            logger.info(f"Found {len(csv_files)} CSV files in archive")
            
            # Extract each CSV file
            for csv_file in csv_files:
                try:
                    # Prevent zip slip vulnerability
                    file_path = (extract_to / csv_file).resolve()
                    if not file_path.is_relative_to(extract_to.resolve()):
                        logger.warning(f"Skipping potentially unsafe path: {csv_file}")
                        continue
                    
                    # Extract file
                    zip_ref.extract(csv_file, extract_to)
                    extracted_files.append(file_path)
                    logger.debug(f"Extracted: {csv_file}")
                    
                except Exception as e:
                    logger.warning(f"Failed to extract {csv_file}: {e}")
                    continue
            
            logger.info(f"Successfully extracted {len(extracted_files)} files to {extract_to}")
            
    except zipfile.BadZipFile as e:
        logger.error(f"Invalid or corrupt ZIP file: {zip_path}")
        raise ExtractionError(f"Bad ZIP file: {e}")
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise ExtractionError(f"Extraction error: {e}")
    
    return extracted_files


def validate_csv_structure(csv_path: Path) -> bool:
    """
    Validate that a CSV file has the expected structure.
    
    Args:
        csv_path: Path to CSV file
    
    Returns:
        True if valid, False otherwise
    """
    try:
        # Read just the header - try different encodings
        encodings = ['windows-1252', 'iso-8859-1', 'utf-8']
        df = None
        for encoding in encodings:
            try:
                df = pd.read_csv(csv_path, nrows=0, encoding=encoding)
                break
            except UnicodeDecodeError:
                continue
        
        if df is None:
            logger.warning(f"Could not decode {csv_path.name} with any standard encoding")
            return False
        
        columns = df.columns.tolist()
        
        # Check if all expected columns are present
        missing_columns = set(config.EXPECTED_COLUMNS) - set(columns)
        
        if missing_columns:
            logger.warning(
                f"CSV {csv_path.name} is missing columns: {missing_columns}"
            )
            return False
        
        # Check for extra columns (just log, don't fail)
        extra_columns = set(columns) - set(config.EXPECTED_COLUMNS)
        if extra_columns:
            logger.info(
                f"CSV {csv_path.name} has additional columns: {extra_columns}"
            )
        
        logger.debug(f"CSV structure validated: {csv_path.name}")
        return True
        
    except pd.errors.EmptyDataError:
        logger.warning(f"CSV file is empty: {csv_path.name}")
        return False
    except pd.errors.ParserError as e:
        logger.warning(f"CSV parsing error in {csv_path.name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to validate {csv_path.name}: {e}")
        return False


def get_csv_info(csv_path: Path) -> Optional[Dict]:
    """
    Extract metadata information from a CSV file.
    
    Args:
        csv_path: Path to CSV file
    
    Returns:
        Dictionary with metadata (row_count, file_size, etc.) or None if error
    """
    try:
        # Get file size
        file_size = csv_path.stat().st_size
        
        # Read CSV to get row count and date range
        # Use chunks for large files
        row_count = 0
        earliest_date = None
        latest_date = None
        
        # Try different encodings
        encodings = ['windows-1252', 'iso-8859-1', 'utf-8']
        csv_encoding = None
        for encoding in encodings:
            try:
                test_chunk = pd.read_csv(csv_path, encoding=encoding, nrows=1)
                csv_encoding = encoding
                break
            except UnicodeDecodeError:
                continue
        
        if csv_encoding is None:
            logger.error(f"Could not determine encoding for {csv_path.name}")
            return None
        
        for chunk in pd.read_csv(
            csv_path,
            encoding=csv_encoding,
            chunksize=config.CSV_CHUNK_SIZE,
            usecols=['Date of Sale (dd/mm/yyyy)'],
            parse_dates=False  # We'll parse manually for safety
        ):
            row_count += len(chunk)
            
            # Try to find date range
            try:
                # Convert dates
                dates = pd.to_datetime(
                    chunk['Date of Sale (dd/mm/yyyy)'],
                    format='%d/%m/%Y',
                    errors='coerce'
                )
                
                chunk_earliest = dates.min()
                chunk_latest = dates.max()
                
                if pd.notna(chunk_earliest):
                    if earliest_date is None or chunk_earliest < earliest_date:
                        earliest_date = chunk_earliest
                
                if pd.notna(chunk_latest):
                    if latest_date is None or chunk_latest > latest_date:
                        latest_date = chunk_latest
                        
            except Exception as e:
                logger.debug(f"Could not parse dates in {csv_path.name}: {e}")
                continue
        
        metadata = {
            'filename': csv_path.name,
            'file_path': str(csv_path),
            'file_size_bytes': file_size,
            'file_size_mb': round(file_size / (1024 * 1024), 2),
            'row_count': row_count,
            'earliest_date': earliest_date.strftime('%Y-%m-%d') if earliest_date else None,
            'latest_date': latest_date.strftime('%Y-%m-%d') if latest_date else None,
        }
        
        logger.debug(
            f"CSV info: {csv_path.name} - {row_count} rows, "
            f"{metadata['file_size_mb']} MB"
        )
        
        return metadata
        
    except Exception as e:
        logger.error(f"Failed to get info for {csv_path.name}: {e}")
        return None


def extract_and_validate_all(zip_path: Path, extract_to: Optional[Path] = None) -> List[Path]:
    """
    Extract ZIP and validate all CSV files.
    
    Args:
        zip_path: Path to ZIP file
        extract_to: Directory to extract to (default: same as zip file)
    
    Returns:
        List of valid CSV file paths
    """
    logger.info(f"Extracting and validating {zip_path.name}...")
    
    # Extract all files
    extracted_files = extract_zip(zip_path, extract_to)
    
    if not extracted_files:
        logger.warning("No files were extracted")
        return []
    
    # Validate each file
    valid_files = []
    for csv_file in extracted_files:
        if validate_csv_structure(csv_file):
            valid_files.append(csv_file)
        else:
            logger.warning(f"Skipping invalid CSV: {csv_file.name}")
    
    logger.info(f"Validated {len(valid_files)} out of {len(extracted_files)} files")
    
    return valid_files


def get_all_csv_files(directory: Path) -> List[Path]:
    """
    Get all CSV files in a directory (non-recursive).
    
    Args:
        directory: Directory to search
    
    Returns:
        List of CSV file paths
    """
    if not directory.exists():
        logger.warning(f"Directory does not exist: {directory}")
        return []
    
    csv_files = list(directory.glob("*.csv"))
    logger.debug(f"Found {len(csv_files)} CSV files in {directory}")
    
    return sorted(csv_files)


if __name__ == "__main__":
    # Simple test
    logger.info("Testing extractor module...")
    
    # Test getting CSV files from monthly directory
    csv_files = get_all_csv_files(config.MONTHLY_DIR)
    logger.info(f"Found {len(csv_files)} CSV files in monthly directory")
    
    if csv_files:
        # Test validation on first file
        test_file = csv_files[0]
        logger.info(f"Testing validation on: {test_file.name}")
        
        is_valid = validate_csv_structure(test_file)
        logger.info(f"Validation result: {'PASS' if is_valid else 'FAIL'}")
        
        if is_valid:
            info = get_csv_info(test_file)
            logger.info(f"File info: {info}")
