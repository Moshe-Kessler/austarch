# AustArch Database

Australian Archaeological Dating Database - A PostgreSQL/PostGIS database containing radiocarbon and luminescence ages from archaeological sites across Australia.

## Overview

AustArch is a comprehensive database of archaeological dating results from Australia, based on the dataset compiled by Williams et al. (2014) and published through the Archaeology Data Service.

**Dataset Statistics:**
- ~5,200 age determinations (radiocarbon and luminescence dating)
- ~1,100 archaeological sites with geographic coordinates
- Coverage across Australian states and territories
- Temporal span: 0 to ~270,000 years BP (Before Present)
- Dating methods: Radiocarbon (C14, AMS, Conventional), OSL, TL, ESR, U-Th, AAR

**Source:**
- Publication: https://intarch.ac.uk/journal/issue36/6/williams.html
- Data: Archaeology Data Service (doi: [10.5284/1027216](https://doi.org/10.5284/1027216))

## Technology Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| PostgreSQL | 17+ | Relational database with ACID compliance |
| PostGIS | 3.x | Geospatial support with Australian datum (GDA94) |
| pg_trgm | - | Fuzzy text search for site names |
| Python | 3.10+ | Data ingestion and visualization |
| psycopg2 | 2.9+ | PostgreSQL adapter for Python |
| Jupyter | 1.0+ | Interactive notebook environment |
| pandas | 2.0+ | Data analysis and manipulation |
| matplotlib | 3.7+ | Publication-quality charts |
| seaborn | 0.12+ | Statistical visualization |
| folium | 0.14+ | Interactive maps |
| SQLAlchemy | 2.0+ | Database toolkit and ORM |

## Database Schema

```
BIOREGION (89 IBRA regions)
    │
    │ 1:N
    ▼
SITE (archaeological locations)
    │
    │ 1:N
    ▼
SAMPLE (material + stratigraphic context)
    │
    │ 1:N
    ▼
AGE_DETERMINATION (dating results)
    │
    ├── DATING_METHOD (radiocarbon, OSL, TL, etc.)
    └── DATA_SOURCE (publication references)
```

**Key Features:**
- PostGIS geometry types for spatial queries
- PostgreSQL range types for calibrated date uncertainty
- Quality rating scale (1-5) with rejection flags
- Automatic bioregion assignment via spatial joins
- Audit logging for data provenance

## Setup

### Prerequisites

- macOS with Homebrew (or Linux with apt/yum)
- PostgreSQL 17+ with PostGIS extension
- Python 3.10+

### 1. Install PostgreSQL and PostGIS

```bash
# macOS
brew install postgresql@17 postgis
brew services start postgresql@17

# Add to PATH (add to ~/.zshrc for persistence)
export PATH="/usr/local/opt/postgresql@17/bin:$PATH"
```

### 2. Create Database

```bash
createdb austarch
psql -d austarch -c "CREATE EXTENSION postgis; CREATE EXTENSION pg_trgm;"
```

### 3. Load Schema

```bash
psql -d austarch -f schema.sql
psql -d austarch -f reference_data.sql
psql -d austarch -f validate.sql
```

### 4. Set Up Python Environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 5. Download and Ingest Data

The data is automatically downloaded from the Archaeology Data Service:

```bash
# Set your database user (default is 'postgres')
export AUSTARCH_DB_USER=your_username

# Run full ingestion pipeline
python ingest.py --all
```

This will:
- Load CSV data files from `./data/`
- Create sites, samples, and age determinations
- Assign bioregions via spatial joins
- Run validation checks
- Refresh materialized views

## Configuration

The ingestion script uses environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `AUSTARCH_DB_HOST` | localhost | Database host |
| `AUSTARCH_DB_PORT` | 5432 | Database port |
| `AUSTARCH_DB_NAME` | austarch | Database name |
| `AUSTARCH_DB_USER` | postgres | Database user |
| `AUSTARCH_DB_PASSWORD` | (empty) | Database password |
| `AUSTARCH_DATA_DIR` | ./data | Directory containing CSV files |

## Testing & Validation

### Run Validation Checks

```bash
source venv/bin/activate
export AUSTARCH_DB_USER=your_username
python ingest.py --validate
```

### Verify Record Counts

```bash
psql -d austarch -c "SELECT * FROM verify_record_counts();"
```

Expected output:
```
       metric        | expected | actual | status
---------------------+----------+--------+--------
 Total sites         |     1748 |   1114 | CHECK
 Radiocarbon ages    |     5044 |   4925 | PASS
 Non-radiocarbon ages|      478 |    258 | CHECK
 Bioregions with data|       75 |      0 | CHECK
```

Note: Some variance is expected due to deduplication and data cleaning.

### Run Data Quality Report

```bash
psql -d austarch -c "SELECT * FROM generate_validation_report();"
```

### Sample Queries

```bash
# Run all example queries
psql -d austarch -f queries.sql

# Interactive session
psql -d austarch
```

**Temporal Distribution:**
```sql
SELECT
    CASE
        WHEN COALESCE(age_bp, c14_age) < 11700 THEN 'Holocene'
        ELSE 'Pleistocene'
    END AS epoch,
    COUNT(*) as count
FROM age_determination
WHERE COALESCE(age_bp, c14_age) IS NOT NULL
GROUP BY 1;
```

**Sites Near a Location (within 100km):**
```sql
SELECT site_name,
       ST_Distance(geom::geography,
                   ST_SetSRID(ST_MakePoint(151.2, -33.9), 4283)::geography) / 1000 AS dist_km
FROM site
WHERE ST_DWithin(geom::geography,
                 ST_SetSRID(ST_MakePoint(151.2, -33.9), 4283)::geography,
                 100000)
ORDER BY dist_km;
```

**Dating Method Summary:**
```sql
SELECT dm.name, COUNT(*) as count
FROM age_determination ad
JOIN dating_method dm ON ad.method_id = dm.id
GROUP BY dm.name
ORDER BY count DESC;
```

## Interactive Visualizations

The repository includes a comprehensive Jupyter notebook (`austarch_visualization.ipynb`) for exploring and visualizing the database.

### Features

The notebook provides:

- **Temporal Distribution Charts**: Stacked bar charts showing age determinations across 1,000-year brackets, color-coded by dating method, with Holocene/Pleistocene boundary markers
- **Dating Method Breakdown**: Horizontal bar charts and pie charts analyzing the distribution of radiocarbon (C14, AMS, Conventional), OSL, TL, and other dating techniques
- **State/Territory Coverage**: Grouped bar charts comparing site counts and age determinations across Australian states
- **Material Type Distribution**: Analysis of sample materials (charcoal, shell, quartz, etc.) used for dating
- **Interactive Map**: Folium-based clustered marker map with color-coded site types and popup details including age ranges
- **Summary Statistics**: Database-wide metrics including site counts, date ranges, and method distributions

### Running the Notebook

```bash
# Install visualization dependencies
pip install -r requirements.txt

# Set database connection (optional - defaults to localhost)
export AUSTARCH_DB_USER=your_username

# Launch Jupyter
jupyter notebook austarch_visualization.ipynb
```

The notebook connects to your local PostgreSQL database and generates publication-quality charts saved as PNG files, plus an interactive HTML map (`austarch_map.html`).

## Project Structure

```
austarch/
├── README.md                        # This file
├── LICENSE                          # MIT License
├── schema.sql                       # Database DDL (tables, indexes, views, triggers)
├── reference_data.sql               # Dating methods, materials, bioregions
├── validate.sql                     # Data quality validation functions
├── queries.sql                      # Example analytical queries
├── ingest.py                        # Python data ingestion pipeline
├── requirements.txt                 # Python dependencies (database + visualization)
├── setup.sh                         # Quick setup script
├── austarch_visualization.ipynb     # Jupyter notebook with interactive visualizations
├── .env.example                     # Example environment variable configuration
├── data/                            # Downloaded CSV data files
│   ├── Austarch_1-3_and_IDASQ_28Nov13-1.csv
│   └── Austarch_Citation.csv
└── venv/                            # Python virtual environment
```

### Generated Output Files

When you run the visualization notebook, it creates:

- `temporal_distribution.png` - Temporal distribution chart
- `method_breakdown.png` - Dating method analysis
- `state_coverage.png` - Geographic coverage by state
- `material_distribution.png` - Sample material types
- `austarch_map.html` - Interactive site map

## Key Views

| View | Description |
|------|-------------|
| `v_age_determinations` | Comprehensive join of all tables |
| `v_temporal_distribution` | Ages grouped by 1000-year brackets |
| `v_site_summary` | Site statistics with date ranges |
| `v_bioregion_coverage` | Dating coverage by bioregion |
| `v_quality_issues` | Ages with data quality problems |

## Citation

If you use this database, please cite:

> Williams, A.N., Ulm, S., Cook, A.R., Langley, M.C. and Collard, M. (2014)
> 'Human refugia in Australia during the Last Glacial Maximum and Terminal Pleistocene:
> A geospatial analysis of the 25-12 ka Australian archaeological record',
> *Journal of Archaeological Science*, 52, pp. 507-521.

Data available from Archaeology Data Service: https://doi.org/10.5284/1027216

## License

The original dataset is provided by the Archaeology Data Service under their terms of use.
This database implementation code is provided as-is for research and educational purposes.
