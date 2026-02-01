#!/usr/bin/env python3
"""
AustArch Data Ingestion Pipeline
=================================
Ingests archaeological dating data from the Archaeology Data Service
into the AustArch PostgreSQL database.

Source: https://doi.org/10.5284/1027216
"""

import csv
import logging
import os
import re
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.request import urlretrieve

import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_batch, execute_values

# Register UUID adapter for psycopg2
psycopg2.extras.register_uuid()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

@dataclass
class Config:
    """Database and ingestion configuration."""
    # Database connection
    db_host: str = os.getenv('AUSTARCH_DB_HOST', 'localhost')
    db_port: int = int(os.getenv('AUSTARCH_DB_PORT', '5432'))
    db_name: str = os.getenv('AUSTARCH_DB_NAME', 'austarch')
    db_user: str = os.getenv('AUSTARCH_DB_USER', 'postgres')
    db_password: str = os.getenv('AUSTARCH_DB_PASSWORD', '')

    # Data source URLs (from Archaeology Data Service)
    # Note: Actual URLs should be obtained from https://doi.org/10.5284/1027216
    data_dir: Path = Path(os.getenv('AUSTARCH_DATA_DIR', './data'))

    # Processing options
    batch_size: int = 500
    skip_existing: bool = True
    validate_coordinates: bool = True

    # Australian coordinate bounds
    lat_min: float = -43.7
    lat_max: float = -10.0
    lon_min: float = 112.0
    lon_max: float = 154.0


# =============================================================================
# Data Mappings
# =============================================================================

# Map source material descriptions to our material codes
MATERIAL_MAPPING = {
    'charcoal': 'CHARCOAL',
    'wood': 'WOOD',
    'bone': 'BONE',
    'burnt bone': 'BONE_BURNT',
    'calcined bone': 'BONE_BURNT',
    'shell': 'SHELL_UNSPEC',
    'marine shell': 'SHELL_MARINE',
    'freshwater shell': 'SHELL_FRESHWATER',
    'land snail': 'SHELL_TERRESTRIAL',
    'seed': 'SEED',
    'seeds': 'SEED',
    'plant': 'SEED',
    'peat': 'PEAT',
    'soil': 'SOIL_ORG',
    'sediment': 'SEDIMENT',
    'organic': 'SOIL_ORG',
    'hair': 'HAIR',
    'eggshell': 'EGGSHELL',
    'emu eggshell': 'EGGSHELL',
    'resin': 'RESIN',
    'fibre': 'FIBER',
    'fiber': 'FIBER',
    'dung': 'DUNG',
    'quartz': 'QUARTZ',
    'feldspar': 'FELDSPAR',
    'sand': 'SAND',
    'calcite': 'CALCITE',
    'tooth': 'TOOTH_ENAMEL',
    'enamel': 'TOOTH_ENAMEL',
    'hearth': 'HEARTH',
    'ceramic': 'CERAMIC',
    'pottery': 'CERAMIC',
}

# Map source method descriptions to our method codes
METHOD_MAPPING = {
    'radiocarbon': 'C14',
    'c14': 'C14',
    'ams': 'AMS',
    'accelerator': 'AMS',
    'conventional': 'CONV',
    'osl': 'OSL',
    'optically stimulated': 'OSL',
    'tl': 'TL',
    'thermoluminescence': 'TL',
    'irsl': 'IRSL',
    'u-th': 'U-TH',
    'uranium': 'U-TH',
    'u-series': 'U-TH',
    'esr': 'ESR',
    'electron spin': 'ESR',
    'aar': 'AAR',
    'amino acid': 'AAR',
    'cosmogenic': 'COSMO',
}

# State name normalization
STATE_MAPPING = {
    'nsw': 'NSW',
    'new south wales': 'NSW',
    'vic': 'VIC',
    'victoria': 'VIC',
    'qld': 'QLD',
    'queensland': 'QLD',
    'sa': 'SA',
    'south australia': 'SA',
    'wa': 'WA',
    'western australia': 'WA',
    'nt': 'NT',
    'northern territory': 'NT',
    'tas': 'TAS',
    'tasmania': 'TAS',
    'act': 'ACT',
}


# =============================================================================
# Database Connection
# =============================================================================

def get_connection(config: Config):
    """Create database connection."""
    return psycopg2.connect(
        host=config.db_host,
        port=config.db_port,
        dbname=config.db_name,
        user=config.db_user,
        password=config.db_password
    )


# =============================================================================
# Data Parsing Utilities
# =============================================================================

def parse_float(value: str) -> Optional[float]:
    """Parse a float value, returning None for empty/invalid."""
    if not value or value.strip() in ('', 'NA', 'N/A', '-', 'null', 'NULL'):
        return None
    try:
        return float(value.strip().replace(',', ''))
    except ValueError:
        return None


def parse_int(value: str) -> Optional[int]:
    """Parse an integer value, returning None for empty/invalid."""
    f = parse_float(value)
    return int(f) if f is not None else None


def normalize_material(description: str) -> str:
    """Normalize material description to standard code."""
    if not description:
        return 'UNKNOWN'

    desc_lower = description.lower().strip()

    # Check for exact matches first
    for pattern, code in MATERIAL_MAPPING.items():
        if pattern in desc_lower:
            return code

    return 'OTHER'


def normalize_method(description: str) -> str:
    """Normalize dating method description to standard code."""
    if not description:
        return 'C14'  # Default assumption

    desc_lower = description.lower().strip()

    for pattern, code in METHOD_MAPPING.items():
        if pattern in desc_lower:
            return code

    # Check lab code prefixes for method hints
    return 'C14'  # Default


def normalize_state(state: str) -> Optional[str]:
    """Normalize state name to standard abbreviation."""
    if not state:
        return None

    state_lower = state.lower().strip()
    return STATE_MAPPING.get(state_lower, state.upper()[:3])


# IBRA region to state mapping (major regions)
IBRA_STATE_HINTS = {
    'sydney': 'NSW', 'riverina': 'NSW', 'darling': 'NSW', 'murray': 'NSW',
    'south east corner': 'NSW', 'nandewar': 'NSW', 'new england': 'NSW',
    'brigalow': 'QLD', 'cape york': 'QLD', 'wet tropics': 'QLD', 'einasleigh': 'QLD',
    'mulga': 'QLD', 'mitchell': 'QLD', 'gulf': 'QLD',
    'gippsland': 'VIC', 'victorian': 'VIC', 'mallee': 'VIC',
    'nullarbor': 'SA', 'flinders': 'SA', 'eyre': 'SA', 'gawler': 'SA',
    'simpson': 'SA', 'stony plains': 'SA', 'naracoorte': 'SA',
    'pilbara': 'WA', 'kimberley': 'WA', 'carnarvon': 'WA', 'murchison': 'WA',
    'geraldton': 'WA', 'swan': 'WA', 'jarrah': 'WA', 'esperance': 'WA',
    'coolgardie': 'WA', 'gibson': 'WA', 'great victoria': 'WA', 'little sandy': 'WA',
    'arnhem': 'NT', 'darwin': 'NT', 'tanami': 'NT', 'macdonnell': 'NT',
    'finke': 'NT', 'barkly': 'NT', 'sturt': 'NT', 'pine creek': 'NT',
    'tasmanian': 'TAS', 'furneaux': 'TAS', 'king': 'TAS',
    'australian alps': 'ACT',
}


def derive_state_from_ibra(ibra_region: str) -> Optional[str]:
    """Derive state from IBRA bioregion name."""
    if not ibra_region:
        return None

    ibra_lower = ibra_region.lower().strip()

    for hint, state in IBRA_STATE_HINTS.items():
        if hint in ibra_lower:
            return state

    return None


def parse_depth(depth_str: str) -> Optional[float]:
    """Parse depth string which may be a range or single value."""
    if not depth_str or depth_str.strip() in ('', 'NA', 'N/A', '-', 'surface'):
        return None

    depth_str = depth_str.strip().lower()

    # Handle ranges like "10-20" or "10 - 20"
    if '-' in depth_str:
        parts = depth_str.split('-')
        if len(parts) >= 2:
            top = parse_float(parts[0])
            if top is not None:
                return top

    # Handle single value
    return parse_float(depth_str)


def determine_method(method_str: str, technique_str: str, lab_code: str) -> str:
    """Determine dating method from METHOD, TECHNIQUE columns and lab code."""
    combined = f"{method_str} {technique_str}".lower()

    # Check for luminescence methods first
    if 'osl' in combined or 'optically stimulated' in combined:
        return 'OSL'
    if 'tl' in combined or 'thermoluminescence' in combined:
        return 'TL'
    if 'irsl' in combined:
        return 'IRSL'

    # Check for other non-C14 methods
    if 'u-th' in combined or 'uranium' in combined or 'u-series' in combined:
        return 'U-TH'
    if 'esr' in combined or 'electron spin' in combined:
        return 'ESR'
    if 'aar' in combined or 'amino acid' in combined:
        return 'AAR'

    # Default to radiocarbon, but try to distinguish AMS vs conventional
    if 'ams' in combined or 'accelerator' in combined:
        return 'AMS'
    if 'conventional' in combined or 'radiometric' in combined:
        return 'CONV'

    # Infer from lab code prefixes
    if lab_code:
        lab_upper = lab_code.upper()
        # AMS labs
        if lab_upper.startswith(('OZ', 'SANU', 'ANUA', 'CAMS', 'AA-', 'BETA', 'UBA', 'UCIAMS', 'D-AMS')):
            return 'AMS'
        # Conventional labs
        if lab_upper.startswith(('I-', 'GX-', 'GAK-', 'SUA-', 'ANU-', 'NZ-', 'GRN-', 'W-')):
            return 'CONV'

    # Default to generic C14
    return 'C14'


def parse_lab_code(code: str) -> Optional[str]:
    """Clean and validate lab code format."""
    if not code or code.strip() in ('', 'NA', '-'):
        return None

    cleaned = code.strip().upper()
    # Remove common prefixes/suffixes that aren't part of the code
    cleaned = re.sub(r'\s+', '', cleaned)

    return cleaned if len(cleaned) >= 3 else None


def validate_coordinates(lat: float, lon: float, config: Config) -> bool:
    """Check if coordinates are within Australian bounds."""
    return (config.lat_min <= lat <= config.lat_max and
            config.lon_min <= lon <= config.lon_max)


def parse_citation(text: str) -> dict:
    """Parse a citation string into components."""
    result = {
        'citation': text,
        'author': None,
        'year': None,
        'title': None
    }

    if not text:
        return result

    # Try to extract year (4 digits, likely 19xx or 20xx)
    year_match = re.search(r'\b(19\d{2}|20\d{2})\b', text)
    if year_match:
        result['year'] = int(year_match.group(1))

    # Extract author (text before year or before first comma)
    if year_match:
        author_text = text[:year_match.start()].strip()
        author_text = re.sub(r'[,\.\(\)]+$', '', author_text)
        if author_text:
            result['author'] = author_text

    return result


# =============================================================================
# Site Deduplication
# =============================================================================

def normalize_site_name(name: str) -> str:
    """Normalize site name for comparison."""
    if not name:
        return ''

    # Lowercase, remove punctuation, normalize whitespace
    normalized = name.lower()
    normalized = re.sub(r'[^\w\s]', ' ', normalized)
    normalized = ' '.join(normalized.split())

    return normalized


def find_existing_site(cursor, name: str, lat: float, lon: float) -> Optional[int]:
    """Find existing site by name match or proximity.

    If a site is found by name but has no coordinates, and the current row
    has coordinates, update the site with those coordinates.
    """
    normalized = normalize_site_name(name)

    # First try exact name match
    cursor.execute("""
        SELECT id, latitude, longitude FROM site
        WHERE LOWER(REPLACE(site_name, ' ', '')) = LOWER(REPLACE(%s, ' ', ''))
        LIMIT 1
    """, (name,))

    result = cursor.fetchone()
    if result:
        site_id, existing_lat, existing_lon = result
        # Update coordinates if site has none but current row does
        if (existing_lat is None or existing_lon is None) and lat is not None and lon is not None:
            cursor.execute("""
                UPDATE site SET latitude = %s, longitude = %s
                WHERE id = %s
            """, (lat, lon, site_id))
        return site_id

    # Try spatial proximity (within ~100m)
    if lat is not None and lon is not None:
        cursor.execute("""
            SELECT id FROM site
            WHERE geom IS NOT NULL
              AND ST_DWithin(
                  geom::geography,
                  ST_SetSRID(ST_MakePoint(%s, %s), 4283)::geography,
                  100
              )
            LIMIT 1
        """, (lon, lat))

        result = cursor.fetchone()
        if result:
            return result[0]

    return None


# =============================================================================
# Reference Data Lookups
# =============================================================================

class ReferenceDataCache:
    """Cache for reference table lookups."""

    def __init__(self, cursor):
        self.cursor = cursor
        self._methods = {}
        self._materials = {}
        self._sources = {}
        self._load_reference_data()

    def _load_reference_data(self):
        """Load reference tables into memory."""
        self.cursor.execute("SELECT id, code FROM dating_method")
        self._methods = {row[1]: row[0] for row in self.cursor.fetchall()}

        self.cursor.execute("SELECT id, code FROM sample_material")
        self._materials = {row[1]: row[0] for row in self.cursor.fetchall()}

    def get_method_id(self, code: str) -> Optional[int]:
        return self._methods.get(code)

    def get_material_id(self, code: str) -> Optional[int]:
        return self._materials.get(code)

    def get_or_create_source(self, citation: str) -> Optional[int]:
        """Get or create a data source record."""
        if not citation:
            return None

        if citation in self._sources:
            return self._sources[citation]

        parsed = parse_citation(citation)

        self.cursor.execute("""
            INSERT INTO data_source (citation, author, year)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING id
        """, (parsed['citation'], parsed['author'], parsed['year']))

        result = self.cursor.fetchone()
        if result:
            self._sources[citation] = result[0]
            return result[0]

        # Might already exist
        self.cursor.execute(
            "SELECT id FROM data_source WHERE citation = %s",
            (citation,)
        )
        result = self.cursor.fetchone()
        if result:
            self._sources[citation] = result[0]
            return result[0]

        return None


# =============================================================================
# Main Ingestion Functions
# =============================================================================

@dataclass
class IngestStats:
    """Track ingestion statistics."""
    sites_created: int = 0
    sites_matched: int = 0
    samples_created: int = 0
    ages_created: int = 0
    ages_skipped: int = 0
    errors: list = field(default_factory=list)


def process_radiocarbon_row(
    row: dict,
    cursor,
    ref_cache: ReferenceDataCache,
    config: Config,
    batch_id: uuid.UUID,
    stats: IngestStats
) -> None:
    """Process a single row from AustArch data file."""

    # Extract and validate coordinates
    # Column names: LATITUDE, LONGITUDE
    lat = parse_float(row.get('LATITUDE'))
    lon = parse_float(row.get('LONGITUDE'))

    if lat is not None and lon is not None:
        if config.validate_coordinates and not validate_coordinates(lat, lon, config):
            stats.errors.append(f"Invalid coordinates: {lat}, {lon}")
            lat, lon = None, None

    # Age determination - validate lab code FIRST before creating site/sample
    # Column names: LAB_CODE, AGE, ERROR, C13_AGE, C13_ERROR, METHOD, TECHNIQUE
    lab_code = parse_lab_code(row.get('LAB_CODE'))

    if not lab_code:
        stats.ages_skipped += 1
        return

    # Check for duplicate lab code before creating any records
    if config.skip_existing:
        cursor.execute(
            "SELECT id FROM age_determination WHERE lab_code = %s",
            (lab_code,)
        )
        if cursor.fetchone():
            stats.ages_skipped += 1
            return

    # Site data
    # Column names: SITE, SITE_TYPE, IBRA_REGION
    site_name = (row.get('SITE') or 'Unknown Site').strip()
    site_type = row.get('SITE_TYPE') or ''
    ibra_region = row.get('IBRA_REGION') or ''

    # Derive state from IBRA region if possible
    state = derive_state_from_ibra(ibra_region)

    # Find or create site
    site_id = find_existing_site(cursor, site_name, lat, lon)

    if site_id:
        stats.sites_matched += 1
    else:
        cursor.execute("""
            INSERT INTO site (site_name, latitude, longitude, state, site_type, region, import_batch_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (site_name, lat, lon, state, site_type, ibra_region, batch_id))
        site_id = cursor.fetchone()[0]
        stats.sites_created += 1

    # Sample data
    # Column names: MATERIAL, MATERIAL_TOP_LEVEL, DEPTH_FROM_SURFACE_CM, CONTEXT
    material_desc = row.get('MATERIAL') or ''
    material_top = row.get('MATERIAL_TOP_LEVEL') or ''
    material_code = normalize_material(material_desc or material_top)
    material_id = ref_cache.get_material_id(material_code)

    depth_str = row.get('DEPTH_FROM_SURFACE_CM') or ''
    depth_top = parse_depth(depth_str)
    context = row.get('CONTEXT') or ''

    cursor.execute("""
        INSERT INTO sample (
            site_id, material_id, material_description,
            depth_cm_top, cultural_association
        )
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """, (site_id, material_id, material_desc, depth_top, context))
    sample_id = cursor.fetchone()[0]
    stats.samples_created += 1

    # Parse ages - AGE and ERROR are the main columns
    c14_age = parse_int(row.get('AGE'))
    c14_error = parse_int(row.get('ERROR'))

    # C13 values (isotopic correction)
    c13_age = parse_float(row.get('C13_AGE'))
    c13_error = parse_float(row.get('C13_ERROR'))

    # Determine method from METHOD and TECHNIQUE columns
    method_str = row.get('METHOD') or ''
    technique_str = row.get('TECHNIQUE') or ''
    method_code = determine_method(method_str, technique_str, lab_code)
    method_id = ref_cache.get_method_id(method_code)

    # For luminescence ages, convert to ka
    lum_age_ka = None
    lum_error_ka = None
    if method_code in ('OSL', 'TL', 'IRSL'):
        # Ages are in years, convert to ka for luminescence
        if c14_age:
            lum_age_ka = c14_age / 1000.0
        if c14_error:
            lum_error_ka = c14_error / 1000.0

    # Citation/source - SOURCE column
    citation = row.get('SOURCE') or ''
    source_id = ref_cache.get_or_create_source(citation) if citation else None

    # Quality flags - DATE_ISSUES and ADDITIONAL_DATA_ISSUES columns
    date_issues = row.get('DATE_ISSUES') or ''
    additional_issues = row.get('ADDITIONAL_DATA_ISSUES') or ''
    notes = row.get('NOTES') or ''

    # Determine if rejected based on issues
    is_rejected = bool(date_issues.strip()) or 'reject' in notes.lower()
    rejection_reason = date_issues if is_rejected else None

    # Quality issues array
    quality_issues = []
    if date_issues.strip():
        quality_issues.append(date_issues.strip())
    if additional_issues.strip():
        quality_issues.append(additional_issues.strip())

    cursor.execute("""
        INSERT INTO age_determination (
            sample_id, lab_code, method_id,
            c14_age, c14_error, delta_c13,
            lum_age_ka, lum_error_ka,
            age_bp, age_error,
            is_rejected, rejection_reason, quality_issues,
            data_source_id, notes, import_batch_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        sample_id, lab_code, method_id,
        c14_age if method_code in ('C14', 'AMS', 'CONV') else None,
        c14_error if method_code in ('C14', 'AMS', 'CONV') else None,
        c13_age,
        lum_age_ka, lum_error_ka,
        c14_age, c14_error,  # Use as general age
        is_rejected, rejection_reason,
        quality_issues if quality_issues else None,
        source_id, notes, batch_id
    ))
    stats.ages_created += 1


def ingest_csv_file(
    filepath: Path,
    processor_func,
    cursor,
    ref_cache: ReferenceDataCache,
    config: Config,
    batch_id: uuid.UUID
) -> IngestStats:
    """Ingest a CSV file using the specified processor function."""

    stats = IngestStats()

    logger.info(f"Processing file: {filepath}")

    with open(filepath, 'r', encoding='utf-8-sig') as f:
        # Try to detect delimiter
        sample = f.read(4096)
        f.seek(0)

        if '\t' in sample:
            reader = csv.DictReader(f, delimiter='\t')
        else:
            reader = csv.DictReader(f)

        for i, row in enumerate(reader):
            try:
                # Use savepoint so individual row errors don't abort transaction
                cursor.execute("SAVEPOINT row_savepoint")
                processor_func(row, cursor, ref_cache, config, batch_id, stats)
                cursor.execute("RELEASE SAVEPOINT row_savepoint")

                if (i + 1) % 500 == 0:
                    logger.info(f"  Processed {i + 1} rows...")

            except Exception as e:
                cursor.execute("ROLLBACK TO SAVEPOINT row_savepoint")
                stats.errors.append(f"Row {i + 1}: {str(e)}")
                if len(stats.errors) <= 5:  # Only log first 5 errors in detail
                    logger.warning(f"Error on row {i + 1}: {e}")

    return stats


def run_ingestion(config: Config) -> None:
    """Main ingestion pipeline."""

    logger.info("=" * 60)
    logger.info("AustArch Data Ingestion Pipeline")
    logger.info("=" * 60)

    # Verify data directory
    if not config.data_dir.exists():
        logger.warning(f"Data directory not found: {config.data_dir}")
        logger.info("Please download data from https://doi.org/10.5284/1027216")
        logger.info(f"And place CSV files in: {config.data_dir}")
        return

    # Find data files
    csv_files = list(config.data_dir.glob('*.csv')) + list(config.data_dir.glob('*.tsv'))

    if not csv_files:
        logger.warning(f"No CSV/TSV files found in {config.data_dir}")
        return

    logger.info(f"Found {len(csv_files)} data file(s)")

    conn = get_connection(config)

    try:
        with conn.cursor() as cursor:
            # Create import batch record
            batch_id = uuid.uuid4()
            cursor.execute("""
                INSERT INTO import_batch (id, source_url, status)
                VALUES (%s, %s, 'running')
            """, (batch_id, 'https://doi.org/10.5284/1027216'))

            # Load reference data cache
            ref_cache = ReferenceDataCache(cursor)

            total_stats = IngestStats()

            for filepath in csv_files:
                filename = filepath.name.lower()

                # Skip citation/reference files - they don't contain age data
                if 'citation' in filename or 'reference' in filename:
                    logger.info(f"Skipping reference file: {filename}")
                    continue

                # Use unified processor for all age data
                # (handles radiocarbon, OSL, TL, etc. based on METHOD column)
                processor = process_radiocarbon_row
                logger.info(f"Processing data file: {filename}")

                stats = ingest_csv_file(
                    filepath, processor, cursor, ref_cache, config, batch_id
                )

                # Aggregate stats
                total_stats.sites_created += stats.sites_created
                total_stats.sites_matched += stats.sites_matched
                total_stats.samples_created += stats.samples_created
                total_stats.ages_created += stats.ages_created
                total_stats.ages_skipped += stats.ages_skipped
                total_stats.errors.extend(stats.errors)

            # Update import batch record
            cursor.execute("""
                UPDATE import_batch
                SET completed_at = NOW(),
                    status = 'completed',
                    record_count = %s,
                    notes = %s
                WHERE id = %s
            """, (
                total_stats.ages_created,
                f"Sites: {total_stats.sites_created} new, {total_stats.sites_matched} matched. "
                f"Ages: {total_stats.ages_created} created, {total_stats.ages_skipped} skipped.",
                batch_id
            ))

            conn.commit()

            # Print summary
            logger.info("=" * 60)
            logger.info("Ingestion Complete")
            logger.info("=" * 60)
            logger.info(f"Sites created: {total_stats.sites_created}")
            logger.info(f"Sites matched: {total_stats.sites_matched}")
            logger.info(f"Samples created: {total_stats.samples_created}")
            logger.info(f"Ages created: {total_stats.ages_created}")
            logger.info(f"Ages skipped: {total_stats.ages_skipped}")

            if total_stats.errors:
                logger.warning(f"Errors encountered: {len(total_stats.errors)}")
                for error in total_stats.errors[:10]:
                    logger.warning(f"  - {error}")
                if len(total_stats.errors) > 10:
                    logger.warning(f"  ... and {len(total_stats.errors) - 10} more")

    except Exception as e:
        conn.rollback()
        logger.error(f"Ingestion failed: {e}")
        raise

    finally:
        conn.close()


def assign_bioregions(config: Config) -> None:
    """Post-ingestion: Assign bioregions to sites via spatial join."""

    logger.info("Assigning bioregions to sites...")

    conn = get_connection(config)

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE site s
                SET bioregion_id = b.id
                FROM bioregion b
                WHERE s.geom IS NOT NULL
                  AND s.bioregion_id IS NULL
                  AND ST_Contains(b.geom, s.geom)
            """)

            updated = cursor.rowcount
            conn.commit()

            logger.info(f"Assigned bioregions to {updated} sites")

    finally:
        conn.close()


def refresh_views(config: Config) -> None:
    """Post-ingestion: Refresh materialized views."""

    logger.info("Refreshing materialized views...")

    conn = get_connection(config)

    try:
        with conn.cursor() as cursor:
            cursor.execute("REFRESH MATERIALIZED VIEW mv_summary_stats")
            conn.commit()
            logger.info("Views refreshed successfully")

    finally:
        conn.close()


def validate_data(config: Config) -> None:
    """Post-ingestion: Run validation checks."""

    logger.info("Running validation checks...")

    conn = get_connection(config)

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM validate_data_quality()")

            logger.info("\nValidation Results:")
            logger.info("-" * 50)

            for row in cursor.fetchall():
                check_name, issue_count, severity = row
                status = "✓" if issue_count == 0 else "⚠"
                logger.info(f"  {status} {check_name}: {issue_count} ({severity})")

            cursor.execute("SELECT * FROM verify_record_counts()")

            logger.info("\nRecord Count Verification:")
            logger.info("-" * 50)

            for row in cursor.fetchall():
                metric, expected, actual, status = row
                status_icon = "✓" if status == 'PASS' else "?"
                logger.info(f"  {status_icon} {metric}: {actual} (expected ~{expected})")

    finally:
        conn.close()


# =============================================================================
# CLI Interface
# =============================================================================

def main():
    """Command-line interface."""
    import argparse

    parser = argparse.ArgumentParser(
        description='AustArch Data Ingestion Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python ingest.py --ingest           # Run full ingestion
  python ingest.py --validate         # Run validation only
  python ingest.py --assign-bioregions # Assign bioregions after ingestion
  python ingest.py --all              # Run all steps

Environment variables:
  AUSTARCH_DB_HOST      Database host (default: localhost)
  AUSTARCH_DB_PORT      Database port (default: 5432)
  AUSTARCH_DB_NAME      Database name (default: austarch)
  AUSTARCH_DB_USER      Database user (default: postgres)
  AUSTARCH_DB_PASSWORD  Database password
  AUSTARCH_DATA_DIR     Directory containing CSV data files (default: ./data)
        """
    )

    parser.add_argument('--ingest', action='store_true',
                        help='Run data ingestion')
    parser.add_argument('--assign-bioregions', action='store_true',
                        help='Assign bioregions to sites')
    parser.add_argument('--refresh-views', action='store_true',
                        help='Refresh materialized views')
    parser.add_argument('--validate', action='store_true',
                        help='Run validation checks')
    parser.add_argument('--all', action='store_true',
                        help='Run all steps')
    parser.add_argument('--data-dir', type=Path,
                        help='Directory containing data files')

    args = parser.parse_args()

    config = Config()

    if args.data_dir:
        config.data_dir = args.data_dir

    # Default to showing help if no action specified
    if not any([args.ingest, args.assign_bioregions, args.refresh_views,
                args.validate, args.all]):
        parser.print_help()
        return

    try:
        if args.all or args.ingest:
            run_ingestion(config)

        if args.all or args.assign_bioregions:
            assign_bioregions(config)

        if args.all or args.refresh_views:
            refresh_views(config)

        if args.all or args.validate:
            validate_data(config)

        logger.info("\nAll operations completed successfully!")

    except psycopg2.OperationalError as e:
        logger.error(f"Database connection failed: {e}")
        logger.info("Check your database connection settings.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Operation failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
