"""
Download module for Irish Property Price Register ETL Pipeline.

Handles downloading files from the PPR website with retry logic,
progress tracking, and error handling.
"""

import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
import warnings

import requests
from tqdm import tqdm

# Suppress SSL warnings since we're disabling verification for the government site
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

from config import config
from logger_config import get_logger

logger = get_logger(__name__)


class DownloadError(Exception):
    """Raised when download fails after all retries."""
    pass


def download_file(
    url: str,
    destination: Path,
    max_retries: Optional[int] = None,
    show_progress: bool = True
) -> bool:
    """
    Download a file with retry logic and progress indication.
    
    Args:
        url: URL to download from
        destination: Path where file should be saved
        max_retries: Maximum retry attempts (default from config)
        show_progress: Whether to show progress bar
    
    Returns:
        True if download successful, False otherwise
    
    Raises:
        DownloadError: If download fails after all retries
    """
    if max_retries is None:
        max_retries = config.MAX_RETRIES
    
    # Ensure destination directory exists
    destination.parent.mkdir(parents=True, exist_ok=True)
    
    # Extract filename from URL for logging
    filename = Path(urlparse(url).path).name
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Downloading {filename} (attempt {attempt}/{max_retries})...")
            logger.debug(f"URL: {url}")
            logger.debug(f"Destination: {destination}")
            
            # Make request with streaming
            response = requests.get(
                url,
                stream=True,
                timeout=config.TIMEOUT,
                headers={'User-Agent': 'Mozilla/5.0 (PropertyPriceRegister ETL Pipeline)'},
                verify=False  # Disable SSL verification for this government site
            )
            response.raise_for_status()
            
            # Get total file size
            total_size = int(response.headers.get('content-length', 0))
            
            # Download with progress bar
            downloaded_size = 0
            start_time = time.time()
            
            with open(destination, 'wb') as f:
                if show_progress and total_size > 0:
                    with tqdm(
                        total=total_size,
                        unit='B',
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=filename
                    ) as pbar:
                        for chunk in response.iter_content(chunk_size=config.CHUNK_SIZE):
                            if chunk:
                                f.write(chunk)
                                downloaded_size += len(chunk)
                                pbar.update(len(chunk))
                else:
                    for chunk in response.iter_content(chunk_size=config.CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
            
            elapsed_time = time.time() - start_time
            speed_mbps = (downloaded_size / (1024 * 1024)) / elapsed_time if elapsed_time > 0 else 0
            
            logger.info(
                f"Download completed: {downloaded_size / (1024 * 1024):.1f} MB "
                f"in {elapsed_time:.1f}s ({speed_mbps:.1f} MB/s)"
            )
            
            # Verify download
            if verify_download(destination):
                return True
            else:
                logger.warning(f"Downloaded file failed verification: {destination}")
                destination.unlink(missing_ok=True)
                
        except requests.exceptions.Timeout:
            logger.warning(f"Download timeout on attempt {attempt}/{max_retries}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code}: {e}")
            if e.response.status_code == 404:
                logger.error(f"File not found: {url}")
                return False  # Don't retry on 404
            if e.response.status_code >= 500:
                logger.warning(f"Server error, will retry...")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Download failed on attempt {attempt}/{max_retries}: {e}")
        except IOError as e:
            logger.error(f"File I/O error: {e}")
            raise DownloadError(f"Cannot write to {destination}: {e}")
        
        # Wait before retry (with exponential backoff)
        if attempt < max_retries:
            wait_time = config.RETRY_DELAY * (config.RETRY_BACKOFF ** (attempt - 1))
            logger.info(f"Waiting {wait_time:.1f}s before retry...")
            time.sleep(wait_time)
    
    # All retries failed
    logger.error(f"Download failed after {max_retries} attempts: {url}")
    return False


def verify_download(file_path: Path, min_size: Optional[int] = None) -> bool:
    """
    Verify that downloaded file is valid.
    
    Args:
        file_path: Path to downloaded file
        min_size: Minimum expected file size in bytes (default from config)
    
    Returns:
        True if file is valid, False otherwise
    """
    if min_size is None:
        min_size = config.MIN_FILE_SIZE
    
    if not file_path.exists():
        logger.error(f"Downloaded file does not exist: {file_path}")
        return False
    
    file_size = file_path.stat().st_size
    
    if file_size < min_size:
        logger.error(f"Downloaded file too small: {file_size} bytes (min: {min_size})")
        return False
    
    logger.debug(f"Download verified: {file_path.name} ({file_size} bytes)")
    return True


def download_all_data(destination_dir: Optional[Path] = None) -> Optional[Path]:
    """
    Download the complete historical dataset (PPR-ALL.zip).
    
    Args:
        destination_dir: Directory to save file (default: config.INITIAL_DIR)
    
    Returns:
        Path to downloaded file if successful, None otherwise
    """
    if destination_dir is None:
        destination_dir = config.INITIAL_DIR
    
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = destination_dir / "PPR-ALL.zip"
    
    logger.info("Starting download of complete historical dataset (PPR-ALL.zip)")
    logger.info(f"This may take several minutes depending on your connection...")
    
    success = download_file(config.ALL_DATA_URL, destination)
    
    if success:
        logger.info(f"Successfully downloaded PPR-ALL.zip to {destination}")
        return destination
    else:
        logger.error("Failed to download complete dataset")
        return None


def download_monthly_data(
    year: int,
    month: int,
    destination_dir: Optional[Path] = None
) -> Optional[Path]:
    """
    Download data for a specific month.
    
    Args:
        year: Year (e.g., 2025)
        month: Month (1-12)
        destination_dir: Directory to save file (default: config.MONTHLY_DIR)
    
    Returns:
        Path to downloaded file if successful, None otherwise
    """
    if destination_dir is None:
        destination_dir = config.MONTHLY_DIR
    
    destination_dir.mkdir(parents=True, exist_ok=True)
    
    # Format filename
    filename = f"PPR-{year}-{month:02d}.csv"
    destination = destination_dir / filename
    
    # Format URL
    url = config.MONTHLY_URL_PATTERN.format(year=year, month=month)
    
    logger.info(f"Downloading monthly data: {year}-{month:02d}")
    
    success = download_file(url, destination)
    
    if success:
        logger.info(f"Successfully downloaded {filename}")
        return destination
    else:
        logger.warning(f"Failed to download {filename} (may not exist yet)")
        return None


def download_county_data(
    year: int,
    month: int,
    county: str,
    destination_dir: Optional[Path] = None
) -> Optional[Path]:
    """
    Download data for a specific county and month.
    
    Args:
        year: Year (e.g., 2025)
        month: Month (1-12)
        county: County name (e.g., "Dublin")
        destination_dir: Directory to save file (default: config.MONTHLY_DIR)
    
    Returns:
        Path to downloaded file if successful, None otherwise
    """
    if destination_dir is None:
        destination_dir = config.MONTHLY_DIR
    
    destination_dir.mkdir(parents=True, exist_ok=True)
    
    # Format filename
    filename = f"PPR-{year}-{month:02d}-{county}.csv"
    destination = destination_dir / filename
    
    # Format URL
    url = config.COUNTY_URL_PATTERN.format(year=year, month=month, county=county)
    
    logger.info(f"Downloading county data: {county}, {year}-{month:02d}")
    
    success = download_file(url, destination)
    
    if success:
        logger.info(f"Successfully downloaded {filename}")
        return destination
    else:
        logger.warning(f"Failed to download {filename}")
        return None


if __name__ == "__main__":
    # Simple test
    logger.info("Testing downloader module...")
    
    # Test monthly download (current month minus 1)
    from datetime import datetime
    now = datetime.now()
    year = now.year if now.month > 1 else now.year - 1
    month = now.month - 1 if now.month > 1 else 12
    
    test_file = download_monthly_data(year, month)
    if test_file:
        logger.info(f"Test successful! Downloaded: {test_file}")
    else:
        logger.error("Test failed!")
