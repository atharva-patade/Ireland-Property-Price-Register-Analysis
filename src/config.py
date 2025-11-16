"""
Configuration settings for Irish Property Price Register ETL Pipeline.

This module centralizes all configuration constants including URLs, paths,
and operational settings.
"""

from pathlib import Path
from typing import Final


class Config:
    """Central configuration for the ETL pipeline."""
    
    # URLs
    BASE_URL: Final[str] = "https://www.propertypriceregister.ie/website/npsra/ppr/npsra-ppr.nsf/Downloads"
    ALL_DATA_URL: Final[str] = f"{BASE_URL}/PPR-ALL.zip/$FILE/PPR-ALL.zip"
    MONTHLY_URL_PATTERN: Final[str] = f"{BASE_URL}/PPR-{{year}}-{{month:02d}}.csv/$FILE/PPR-{{year}}-{{month:02d}}.csv"
    COUNTY_URL_PATTERN: Final[str] = f"{BASE_URL}/PPR-{{year}}-{{month:02d}}-{{county}}.csv/$FILE/PPR-{{year}}-{{month:02d}}-{{county}}.csv"
    
    # Paths
    BASE_DIR: Final[Path] = Path(__file__).parent.parent
    DATA_DIR: Final[Path] = BASE_DIR / "data"
    RAW_DIR: Final[Path] = DATA_DIR / "raw"
    INITIAL_DIR: Final[Path] = RAW_DIR / "initial"
    MONTHLY_DIR: Final[Path] = RAW_DIR / "monthly"
    PROCESSED_DIR: Final[Path] = DATA_DIR / "processed"
    METADATA_DIR: Final[Path] = DATA_DIR / "metadata"
    LOGS_DIR: Final[Path] = BASE_DIR / "logs"
    
    # Files
    CONSOLIDATED_FILE: Final[Path] = PROCESSED_DIR / "ppr_consolidated.parquet"
    CONSOLIDATED_BACKUP: Final[Path] = PROCESSED_DIR / "ppr_consolidated.parquet.backup"
    METADATA_FILE: Final[Path] = METADATA_DIR / "last_update.json"
    
    # Download settings
    MAX_RETRIES: Final[int] = 3
    RETRY_DELAY: Final[int] = 5  # seconds
    RETRY_BACKOFF: Final[float] = 2.0  # exponential backoff multiplier
    CHUNK_SIZE: Final[int] = 8192  # bytes for streaming downloads
    TIMEOUT: Final[int] = 30  # seconds
    MIN_FILE_SIZE: Final[int] = 1024  # minimum valid file size in bytes
    
    # Data processing settings
    CSV_CHUNK_SIZE: Final[int] = 100000  # rows per chunk for large file processing
    PARQUET_COMPRESSION: Final[str] = "snappy"
    
    # Expected CSV columns (from PPR website)
    EXPECTED_COLUMNS: Final[list] = [
        "Date of Sale (dd/mm/yyyy)",
        "Address",
        "County",
        "Eircode",
        "Price (€)",
        "Not Full Market Price",
        "VAT Exclusive",
        "Description of Property",
        "Property Size Description"
    ]
    
    # Logging settings
    LOG_FORMAT: Final[str] = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
    LOG_DATE_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"
    LOG_FILE_PATTERN: Final[str] = "etl_pipeline_{date}.log"
    LOG_RETENTION_DAYS: Final[int] = 30
    
    @classmethod
    def ensure_directories(cls) -> None:
        """Create all required directories if they don't exist."""
        directories = [
            cls.INITIAL_DIR,
            cls.MONTHLY_DIR,
            cls.PROCESSED_DIR,
            cls.METADATA_DIR,
            cls.LOGS_DIR,
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)


# Create a singleton instance for easy import
config = Config()
