# Irish Property Price Register - ETL Pipeline

Automated ETL pipeline for downloading and processing data from the [Irish Property Price Register](https://www.propertypriceregister.ie/).

## Overview

This project downloads and processes all residential property sales in Ireland since January 1, 2010, making the data ready for analysis.

## Features

- ✅ Download complete historical dataset (~750K+ records)
- ✅ Incremental monthly updates
- ✅ Automatic deduplication
- ✅ Parquet storage for efficient analysis
- ✅ Comprehensive logging

## Installation

1. **Clone the repository**:
   ```bash
   cd IrishPropertyPriceRegister
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Initial Data Download

Download the complete historical dataset:

```bash
python src/etl_pipeline.py --mode initial
```

This will:
- Download PPR-ALL.zip (~150-200 MB)
- Extract and process all CSV files
- Create consolidated dataset in Parquet format
- Takes approximately 5-15 minutes

### Incremental Updates

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

## Exploratory Analysis

Start exploring the data:

```bash
jupyter notebook notebooks/01_data_exploration.ipynb
```

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

## Troubleshooting

### Download Fails

The pipeline automatically retries (3 attempts). If download still fails:
- Check internet connection
- Try again later if server is down

### Out of Memory

If processing fails due to memory:
- Close other applications
- The pipeline uses chunked processing for large files

### Logs

Check logs for detailed information:
```bash
tail -f logs/etl_pipeline_$(date +%Y-%m-%d).log
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
