#!/bin/bash
# Convenience script to run the ETL pipeline with the virtual environment

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Activate virtual environment
source .venv/bin/activate

# Run the pipeline with all arguments passed through
python src/etl_pipeline.py "$@"

# Deactivate virtual environment
deactivate
