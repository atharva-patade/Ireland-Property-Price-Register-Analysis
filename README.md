# Irish Property Price Register - ETL Pipeline

Automated ETL pipeline for downloading and processing data from the [Irish Property Price Register](https://www.propertypriceregister.ie/).

## Overview

This project downloads and processes all residential property sales in Ireland since January 1, 2010, making the data ready for analysis.

## Features

- Download complete historical dataset (~750K+ records)
- Incremental monthly updates
- Automatic deduplication
- Parquet storage for efficient analysis
- Comprehensive logging
## Quick Start

### Prerequisites

- Python 3.8 or higher

### Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/atharva-patade/Ireland-Property-Price-Register-Analysis.git
   cd Ireland-Property-Price-Register-Analysis
   ```

2. **Create and activate virtual environment**:
   ```bash
   # On Linux/Mac
   python3 -m venv .venv
   source .venv/bin/activate
   
   # On Windows
   python -m venv .venv
   .venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### Get the Data

Run the pipeline to download and process all property data:

```bash
python src/etl_pipeline.py --mode initial
```

**What happens:**
- Downloads PPR-ALL.zip (~150-200 MB) from the official Property Price Register
- Extracts and validates all CSV files
- Merges into a single consolidated dataset
- Removes duplicates
- Saves as compressed Parquet format
- **Time**: 5-15 minutes (depending on internet speed)
- **Output**: `data/processed/ppr_consolidated.parquet` (~750K+ property records)

**Success looks like:**
```
[INFO] Starting ETL pipeline in initial mode
[INFO] Downloading PPR-ALL.zip...
[INFO] Download completed: 156.2 MB in 84s
[INFO] Extracted 147 CSV files
[INFO] Merged 752,907 records (removed 1,124 duplicates)
[INFO] Saved to data/processed/ppr_consolidated.parquet
[INFO] Pipeline completed successfully
```

## Usage

### Updating Data (Monthly)

Keep your dataset current with the latest property sales:

Update with latest monthly data:

```bash
python src/etl_pipeline.py --mode incremental
```

Or use automatic mode (detects what's needed):

```bash
python src/etl_pipeline.py
```

### Additional Options

```bash
# Force full refresh
python src/etl_pipeline.py --force-full

# Verbose logging
python src/etl_pipeline.py --verbose

# Convenience script
./run_pipeline.sh
```

## Data Output

After running the pipeline:

- **Main dataset**: `data/processed/ppr_consolidated.parquet`
- **Metadata**: `data/metadata/last_update.json`
- **Logs**: `logs/etl_pipeline_YYYY-MM-DD.log`

## Data Schema

Key columns in the processed dataset:

| Column | Description |
|--------|-------------|
| sale_date | Transaction date |
| address | Property address |
| county | Irish county |
| eircode | Irish postcode |
| price_eur | Sale price in euros |
| property_description | Property type |
| not_full_market_price | Market price indicator |
| vat_exclusive | VAT exclusion flag |

## Analyzing the Data

### Quick Analysis

Load and explore the data in Python:

```python
import pandas as pd

# Load the data
df = pd.read_parquet('data/processed/ppr_consolidated.parquet')

# Basic exploration
print(f"Total records: {len(df):,}")
print(f"\nDate range: {df['sale_date'].min()} to {df['sale_date'].max()}")
print(f"\nAverage price: €{df['price_eur'].mean():,.2f}")
print(f"\nCounties covered: {df['county'].nunique()}")

# Top 5 most expensive sales
print("\nTop 5 most expensive sales:")
print(df.nlargest(5, 'price_eur')[['address', 'county', 'price_eur', 'sale_date']])
```

### Jupyter Notebooks

Explore the data with pre-built analysis notebooks:

```bash
# Make sure virtual environment is activated
jupyter notebook notebooks/01_data_exploration.ipynb
```

**Available notebooks:**
- `01_data_exploration.ipynb` - Basic data exploration and statistics
- `02_new_vs_secondhand_analysis.ipynb` - New build vs second-hand property analysis

## Project Structure

```
IrishPropertyPriceRegister/
├── data/                 # Data storage (gitignored)
│   ├── raw/             # Downloaded CSV files
│   ├── processed/       # Consolidated Parquet dataset
│   └── metadata/        # Pipeline state tracking
├── logs/                # Execution logs (gitignored)
├── src/                 # Source code
│   ├── etl_pipeline.py  # Main entry point
│   ├── downloader.py    # Download module
│   ├── extractor.py     # ZIP/CSV extraction
│   ├── merger.py        # Data merging
│   ├── metadata.py      # State management
│   ├── config.py        # Configuration
│   └── logger_config.py # Logging setup
├── tests/               # Unit tests
├── notebooks/           # Jupyter notebooks
└── requirements.txt     # Python dependencies
```

## Complete Example Workflow

Here's a complete workflow from clone to analysis:

```bash
# 1. Clone and setup
git clone https://github.com/atharva-patade/Ireland-Property-Price-Register-Analysis.git
cd Ireland-Property-Price-Register-Analysis
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Download and process data
python src/etl_pipeline.py --mode initial

# 4. Verify data
python -c "import pandas as pd; df = pd.read_parquet('data/processed/ppr_consolidated.parquet'); print(f'Loaded {len(df):,} records from {df.sale_date.min()} to {df.sale_date.max()}')"

# 5. Start analyzing
jupyter notebook notebooks/01_data_exploration.ipynb
```

## Troubleshooting

### Download Fails

The pipeline automatically retries (3 attempts). If download still fails:
- Check internet connection
- Verify the source website is accessible: https://www.propertypriceregister.ie/
- Try again later if server is down

### Out of Memory

If processing fails due to memory:
- Close other applications
- The pipeline uses chunked processing for large files
- Consider processing on a machine with more RAM

### Import Errors

If you get `ModuleNotFoundError`:
```bash
# Make sure virtual environment is activated
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows

# Reinstall dependencies
pip install -r requirements.txt
```

### Logs

Check logs for detailed information:
```bash
# Linux/Mac
tail -f logs/etl_pipeline_$(date +%Y-%m-%d).log

# Or view the latest log
ls -t logs/*.log | head -1 | xargs cat
```

## Data Source

- **Website**: https://www.propertypriceregister.ie/
- **Provider**: Property Services Regulatory Authority (PSRA)
- **Coverage**: All residential property sales since January 1, 2010
- **Update Frequency**: Monthly

## License

This project is for educational/personal use. The Property Price Register data is public sector information provided by the PSRA.

## Contact

For issues with this pipeline, check the logs directory.  
For issues with source data: [info@psr.ie](mailto:info@psr.ie)
