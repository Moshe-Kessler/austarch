#!/bin/bash
# =============================================================================
# AustArch Database Setup Script
# =============================================================================

set -e

# Configuration
DB_NAME="${AUSTARCH_DB_NAME:-austarch}"
DB_USER="${AUSTARCH_DB_USER:-postgres}"
DB_HOST="${AUSTARCH_DB_HOST:-localhost}"
DB_PORT="${AUSTARCH_DB_PORT:-5432}"

echo "=============================================="
echo "AustArch Database Setup"
echo "=============================================="
echo "Database: $DB_NAME"
echo "Host: $DB_HOST:$DB_PORT"
echo "User: $DB_USER"
echo ""

# Check for psql
if ! command -v psql &> /dev/null; then
    echo "Error: psql command not found. Please install PostgreSQL client."
    exit 1
fi

# Create database if it doesn't exist
echo "Creating database..."
createdb -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" "$DB_NAME" 2>/dev/null || echo "Database already exists or creation skipped."

# Execute schema
echo "Creating schema..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f schema.sql

# Load reference data
echo "Loading reference data..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f reference_data.sql

# Load validation functions
echo "Loading validation functions..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f validate.sql

echo ""
echo "=============================================="
echo "Setup Complete!"
echo "=============================================="
echo ""
echo "Next steps:"
echo "1. Download data from: https://doi.org/10.5284/1027216"
echo "2. Place CSV files in: ./data/"
echo "3. Run ingestion: python ingest.py --all"
echo ""
echo "Optional: Load IBRA bioregion shapefile for spatial analysis"
echo ""
