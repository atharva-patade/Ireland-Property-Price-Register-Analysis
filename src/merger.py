"""
Merger module for Irish Property Price Register ETL Pipeline.

Handles loading, merging, deduplicating, and saving property price data.
"""

import shutil
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from config import config
from logger_config import get_logger

logger = get_logger(__name__)


class MergerError(Exception):
    """Raised when merge operation fails."""
    pass


def load_existing_data(file_path: Optional[Path] = None) -> Optional[pd.DataFrame]:
    """
    Load existing consolidated dataset.
    
    Args:
        file_path: Path to consolidated file (default: config.CONSOLIDATED_FILE)
    
    Returns:
        DataFrame if file exists, None otherwise
    """
    if file_path is None:
        file_path = config.CONSOLIDATED_FILE
    
    if not file_path.exists():
        logger.info("No existing consolidated data found")
        return None
    
    try:
        logger.info(f"Loading existing data from {file_path.name}...")
        df = pd.read_parquet(file_path)
        logger.info(f"Loaded {len(df):,} records from existing dataset")
        return df
        
    except Exception as e:
        logger.error(f"Failed to load existing data: {e}")
        return None


def load_csv_files(csv_files: List[Path]) -> pd.DataFrame:
    """
    Load multiple CSV files and concatenate them.
    
    Args:
        csv_files: List of CSV file paths
    
    Returns:
        Concatenated DataFrame
    
    Raises:
        MergerError: If loading fails
    """
    if not csv_files:
        raise MergerError("No CSV files provided")
    
    logger.info(f"Loading {len(csv_files)} CSV files...")
    
    dfs = []
    total_rows = 0
    
    for csv_file in csv_files:
        try:
            # Try different encodings (Irish government files often use Windows-1252)
            encodings = ['windows-1252', 'iso-8859-1', 'utf-8']
            df = None
            for encoding in encodings:
                try:
                    df = pd.read_csv(
                        csv_file,
                        encoding=encoding,
                        dtype=str  # Read all as strings initially
                    )
                    logger.debug(f"Successfully read {csv_file.name} with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                logger.error(f"Could not decode {csv_file.name} with any standard encoding")
                continue
            
            rows = len(df)
            total_rows += rows
            dfs.append(df)
            
            logger.debug(f"Loaded {csv_file.name}: {rows:,} rows")
            
        except Exception as e:
            logger.error(f"Failed to load {csv_file.name}: {e}")
            # Continue with other files instead of failing completely
            continue
    
    if not dfs:
        raise MergerError("No CSV files could be loaded successfully")
    
    # Concatenate all dataframes
    logger.info(f"Concatenating {len(dfs)} dataframes ({total_rows:,} total rows)...")
    combined_df = pd.concat(dfs, ignore_index=True)
    
    logger.info(f"Successfully loaded {len(combined_df):,} records")
    
    return combined_df


def clean_and_convert_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and convert data types for analysis.
    
    Args:
        df: Raw DataFrame
    
    Returns:
        Cleaned DataFrame with proper types
    """
    logger.info("Cleaning and converting data types...")
    
    df_clean = df.copy()
    
    # Convert date
    try:
        df_clean['sale_date'] = pd.to_datetime(
            df_clean['Date of Sale (dd/mm/yyyy)'],
            format='%d/%m/%Y',
            errors='coerce'
        )
    except Exception as e:
        logger.warning(f"Date conversion issues: {e}")
    
    # Clean and convert price
    try:
        # Remove currency symbol and commas, convert to float
        df_clean['price_eur'] = (
            df_clean['Price (€)']
            .str.replace('€', '', regex=False)
            .str.replace(',', '', regex=False)
            .str.strip()
        )
        df_clean['price_eur'] = pd.to_numeric(df_clean['price_eur'], errors='coerce')
    except Exception as e:
        logger.warning(f"Price conversion issues: {e}")
    
    # Convert boolean fields
    df_clean['not_full_market_price'] = (
        df_clean['Not Full Market Price'].str.strip().str.upper() == 'YES'
    )
    df_clean['vat_exclusive'] = (
        df_clean['VAT Exclusive'].str.strip().str.upper() == 'YES'
    )
    
    # Clean text fields
    df_clean['address'] = df_clean['Address'].str.strip()
    df_clean['county'] = df_clean['County'].str.strip()
    df_clean['eircode'] = df_clean['Eircode'].str.strip()
    df_clean['property_description'] = df_clean['Description of Property'].str.strip()
    df_clean['property_size_category'] = df_clean['Property Size Description'].str.strip()
    
    # Add derived fields
    df_clean['year'] = df_clean['sale_date'].dt.year
    df_clean['month'] = df_clean['sale_date'].dt.month
    df_clean['quarter'] = df_clean['sale_date'].dt.quarter
    
    # Select final columns
    final_columns = [
        'sale_date', 'address', 'county', 'eircode', 'price_eur',
        'not_full_market_price', 'vat_exclusive', 'property_description',
        'property_size_category', 'year', 'month', 'quarter'
    ]
    
    df_final = df_clean[final_columns].copy()
    
    logger.info("Data cleaning completed")
    
    return df_final


def deduplicate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate records using composite key.
    
    Args:
        df: DataFrame to deduplicate
    
    Returns:
        Deduplicated DataFrame
    """
    logger.info("Deduplicating records...")
    
    initial_count = len(df)
    
    # Create composite key for deduplication
    # Using address, date, price, and county
    df['_unique_key'] = (
        df['address'].fillna('').str.upper() + '|' +
        df['sale_date'].astype(str) + '|' +
        df['price_eur'].astype(str) + '|' +
        df['county'].fillna('').str.upper()
    )
    
    # Remove duplicates, keeping first occurrence
    df_dedup = df.drop_duplicates(subset=['_unique_key'], keep='first')
    
    # Remove the temporary key column
    df_dedup = df_dedup.drop(columns=['_unique_key'])
    
    duplicates_removed = initial_count - len(df_dedup)
    
    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed:,} duplicate records ({(duplicates_removed/initial_count)*100:.2f}%)")
    else:
        logger.info("No duplicates found")
    
    return df_dedup


def merge_datasets(
    existing: Optional[pd.DataFrame],
    new_files: List[Path]
) -> pd.DataFrame:
    """
    Merge new data with existing dataset.
    
    Args:
        existing: Existing DataFrame (can be None)
        new_files: List of new CSV files to merge
    
    Returns:
        Merged and deduplicated DataFrame
    
    Raises:
        MergerError: If merge fails
    """
    logger.info("Starting merge operation...")
    
    # Load new data
    new_data = load_csv_files(new_files)
    
    # Clean and convert new data
    new_data_clean = clean_and_convert_data(new_data)
    
    # Merge with existing if present
    if existing is not None:
        logger.info("Merging with existing dataset...")
        combined = pd.concat([existing, new_data_clean], ignore_index=True)
    else:
        logger.info("No existing dataset, using new data only")
        combined = new_data_clean
    
    # Deduplicate
    final_data = deduplicate_data(combined)
    
    logger.info(f"Merge completed: {len(final_data):,} total records")
    
    return final_data


def save_consolidated_data(
    df: pd.DataFrame,
    output_path: Optional[Path] = None,
    backup: bool = True
) -> None:
    """
    Save consolidated dataset to Parquet format.
    
    Args:
        df: DataFrame to save
        output_path: Output file path (default: config.CONSOLIDATED_FILE)
        backup: Whether to backup existing file before overwriting
    
    Raises:
        MergerError: If save fails
    """
    if output_path is None:
        output_path = config.CONSOLIDATED_FILE
    
    # Ensure directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Backup existing file if requested
    if backup and output_path.exists():
        backup_path = config.CONSOLIDATED_BACKUP
        logger.info(f"Backing up existing file to {backup_path.name}")
        try:
            shutil.copy2(output_path, backup_path)
        except Exception as e:
            logger.warning(f"Failed to create backup: {e}")
    
    # Save to Parquet
    try:
        logger.info(f"Saving {len(df):,} records to {output_path.name}...")
        
        df.to_parquet(
            output_path,
            compression=config.PARQUET_COMPRESSION,
            index=False
        )
        
        file_size = output_path.stat().st_size / (1024 * 1024)
        logger.info(f"Successfully saved to {output_path} ({file_size:.1f} MB)")
        
    except Exception as e:
        logger.error(f"Failed to save consolidated data: {e}")
        raise MergerError(f"Save failed: {e}")


def generate_data_summary(df: pd.DataFrame) -> Dict:
    """
    Generate summary statistics for the dataset.
    
    Args:
        df: DataFrame to summarize
    
    Returns:
        Dictionary with summary statistics
    """
    logger.info("Generating data summary...")
    
    summary = {
        'total_records': len(df),
        'earliest_date': df['sale_date'].min().strftime('%Y-%m-%d') if pd.notna(df['sale_date'].min()) else None,
        'latest_date': df['sale_date'].max().strftime('%Y-%m-%d') if pd.notna(df['sale_date'].max()) else None,
        'total_value_eur': float(df['price_eur'].sum()) if 'price_eur' in df else 0,
        'average_price_eur': float(df['price_eur'].mean()) if 'price_eur' in df else 0,
        'median_price_eur': float(df['price_eur'].median()) if 'price_eur' in df else 0,
        'counties': df['county'].nunique() if 'county' in df else 0,
        'non_market_price_count': int(df['not_full_market_price'].sum()) if 'not_full_market_price' in df else 0,
        'vat_exclusive_count': int(df['vat_exclusive'].sum()) if 'vat_exclusive' in df else 0,
        'missing_prices': int(df['price_eur'].isna().sum()) if 'price_eur' in df else 0,
        'missing_dates': int(df['sale_date'].isna().sum()) if 'sale_date' in df else 0,
    }
    
    logger.info(f"Summary: {summary['total_records']:,} records from {summary['earliest_date']} to {summary['latest_date']}")
    logger.info(f"Average price: €{summary['average_price_eur']:,.2f}")
    logger.info(f"Counties covered: {summary['counties']}")
    
    return summary


if __name__ == "__main__":
    # Simple test
    logger.info("Testing merger module...")
    
    # Test loading existing data if available
    existing = load_existing_data()
    if existing is not None:
        summary = generate_data_summary(existing)
        logger.info(f"Test successful! Summary: {summary}")
