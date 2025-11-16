#!/bin/bash
# Quick status check for Irish Property Price Register ETL Pipeline

echo "======================================================================"
echo "Irish Property Price Register - Pipeline Status"
echo "======================================================================"
echo ""

# Check if virtual environment exists
if [ -d ".venv" ]; then
    echo "✅ Virtual environment: EXISTS"
else
    echo "❌ Virtual environment: NOT FOUND"
    echo "   Run: python -m venv .venv"
fi

# Check if data exists
if [ -f "data/processed/ppr_consolidated.parquet" ]; then
    echo "✅ Consolidated data: EXISTS"
    size=$(du -h data/processed/ppr_consolidated.parquet | cut -f1)
    echo "   Size: $size"
else
    echo "❌ Consolidated data: NOT FOUND"
    echo "   Run: python src/etl_pipeline.py --mode initial"
fi

# Check metadata
if [ -f "data/metadata/last_update.json" ]; then
    echo "✅ Metadata: EXISTS"
    echo ""
    echo "Metadata contents:"
    cat data/metadata/last_update.json | python -m json.tool
else
    echo "❌ Metadata: NOT FOUND"
fi

echo ""
echo "======================================================================"
echo "Quick Commands:"
echo "======================================================================"
echo "  ./run_pipeline.sh                 # Run pipeline (auto mode)"
echo "  ./run_pipeline.sh --mode initial  # Force initial load"
echo "  source .venv/bin/activate         # Activate virtual environment"
echo "  jupyter notebook                  # Start Jupyter for EDA"
echo "======================================================================"
