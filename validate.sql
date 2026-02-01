-- =============================================================================
-- AustArch Data Validation Functions
-- Quality checks and data integrity validation
-- =============================================================================

-- =============================================================================
-- VALIDATION FUNCTIONS
-- =============================================================================

-- Check if coordinates are within Australian bounds
CREATE OR REPLACE FUNCTION is_valid_australian_coords(lat NUMERIC, lon NUMERIC)
RETURNS BOOLEAN AS $$
BEGIN
    -- Australian bounding box (mainland + Tasmania)
    -- Latitude: -43.7 to -10.0 (south to north)
    -- Longitude: 112.0 to 154.0 (west to east)
    RETURN lat BETWEEN -43.7 AND -10.0
       AND lon BETWEEN 112.0 AND 154.0;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Validate radiocarbon age is reasonable
CREATE OR REPLACE FUNCTION is_valid_c14_age(age INTEGER, error_val INTEGER)
RETURNS BOOLEAN AS $$
BEGIN
    -- C14 dating range is typically 0-50,000 BP
    -- Error should be positive and reasonable proportion of age
    IF age IS NULL THEN RETURN TRUE; END IF;
    RETURN age >= 0
       AND age <= 55000
       AND (error_val IS NULL OR error_val > 0)
       AND (error_val IS NULL OR error_val < age OR age < 1000);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Validate luminescence age is reasonable
CREATE OR REPLACE FUNCTION is_valid_lum_age(age_ka NUMERIC, error_ka NUMERIC)
RETURNS BOOLEAN AS $$
BEGIN
    -- OSL/TL typically ranges from 0-300 ka for Australian contexts
    IF age_ka IS NULL THEN RETURN TRUE; END IF;
    RETURN age_ka >= 0
       AND age_ka <= 500
       AND (error_ka IS NULL OR error_ka > 0);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Validate lab code format
CREATE OR REPLACE FUNCTION is_valid_lab_code(code VARCHAR)
RETURNS BOOLEAN AS $$
BEGIN
    -- Lab codes follow various patterns:
    -- Examples: OZA-123, Wk-12345, Beta-123456, ANU-1234, UCIAMS-12345, OxA-X-1234-56
    -- Pattern: starts with letters, may contain hyphens, numbers, and letter suffixes
    IF code IS NULL OR code = '' THEN RETURN FALSE; END IF;
    RETURN code ~ '^[A-Za-z]{1,10}[-]?[A-Za-z0-9]*[-]?[0-9]+[-]?[A-Za-z0-9]*$';
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- =============================================================================
-- VALIDATION QUERIES
-- =============================================================================

-- Find sites with invalid coordinates
CREATE OR REPLACE VIEW v_invalid_coordinates AS
SELECT
    id,
    site_name,
    latitude,
    longitude,
    CASE
        WHEN latitude IS NULL OR longitude IS NULL THEN 'Missing coordinates'
        WHEN NOT is_valid_australian_coords(latitude, longitude) THEN 'Outside Australian bounds'
        ELSE 'Unknown issue'
    END AS issue
FROM site
WHERE latitude IS NULL
   OR longitude IS NULL
   OR NOT is_valid_australian_coords(latitude, longitude);

-- Find age determinations with quality issues
CREATE OR REPLACE VIEW v_quality_issues AS
SELECT
    ad.id,
    ad.lab_code,
    dm.name AS method,
    ad.c14_age,
    ad.c14_error,
    ad.lum_age_ka,
    ad.lum_error_ka,
    ARRAY_REMOVE(ARRAY[
        CASE WHEN NOT is_valid_lab_code(ad.lab_code) THEN 'Invalid lab code format' END,
        CASE WHEN dm.code = 'C14' AND NOT is_valid_c14_age(ad.c14_age, ad.c14_error) THEN 'Invalid C14 age/error' END,
        CASE WHEN dm.code IN ('OSL', 'TL') AND NOT is_valid_lum_age(ad.lum_age_ka, ad.lum_error_ka) THEN 'Invalid luminescence age/error' END,
        CASE WHEN ad.c14_age IS NULL AND ad.lum_age_ka IS NULL AND ad.age_bp IS NULL THEN 'No age value recorded' END,
        CASE WHEN ad.c14_error IS NOT NULL AND ad.c14_error > ad.c14_age * 0.5 THEN 'Error exceeds 50% of age' END
    ], NULL) AS issues
FROM age_determination ad
LEFT JOIN dating_method dm ON ad.method_id = dm.id
WHERE
    NOT is_valid_lab_code(ad.lab_code)
    OR (dm.code = 'C14' AND NOT is_valid_c14_age(ad.c14_age, ad.c14_error))
    OR (dm.code IN ('OSL', 'TL') AND NOT is_valid_lum_age(ad.lum_age_ka, ad.lum_error_ka))
    OR (ad.c14_age IS NULL AND ad.lum_age_ka IS NULL AND ad.age_bp IS NULL);

-- Find orphaned samples (no age determinations)
CREATE OR REPLACE VIEW v_orphaned_samples AS
SELECT
    s.id,
    s.sample_code,
    si.site_name
FROM sample s
JOIN site si ON s.site_id = si.id
LEFT JOIN age_determination ad ON ad.sample_id = s.id
WHERE ad.id IS NULL;

-- Find duplicate lab codes
CREATE OR REPLACE VIEW v_duplicate_lab_codes AS
SELECT
    lab_code,
    COUNT(*) AS occurrence_count,
    ARRAY_AGG(id ORDER BY id) AS age_ids
FROM age_determination
WHERE lab_code IS NOT NULL AND lab_code != ''
GROUP BY lab_code
HAVING COUNT(*) > 1;

-- =============================================================================
-- VALIDATION SUMMARY FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION validate_data_quality()
RETURNS TABLE (
    check_name VARCHAR,
    issue_count BIGINT,
    severity VARCHAR
) AS $$
BEGIN
    RETURN QUERY

    SELECT 'Sites outside Australia'::VARCHAR,
           COUNT(*)::BIGINT,
           'HIGH'::VARCHAR
    FROM v_invalid_coordinates

    UNION ALL

    SELECT 'Ages with quality issues'::VARCHAR,
           COUNT(*)::BIGINT,
           'MEDIUM'::VARCHAR
    FROM v_quality_issues

    UNION ALL

    SELECT 'Duplicate lab codes'::VARCHAR,
           COUNT(*)::BIGINT,
           'HIGH'::VARCHAR
    FROM v_duplicate_lab_codes

    UNION ALL

    SELECT 'Samples without ages'::VARCHAR,
           COUNT(*)::BIGINT,
           'LOW'::VARCHAR
    FROM v_orphaned_samples

    UNION ALL

    SELECT 'Sites without bioregion'::VARCHAR,
           COUNT(*)::BIGINT,
           'LOW'::VARCHAR
    FROM site
    WHERE bioregion_id IS NULL AND geom IS NOT NULL

    UNION ALL

    SELECT 'Ages without source'::VARCHAR,
           COUNT(*)::BIGINT,
           'LOW'::VARCHAR
    FROM age_determination
    WHERE data_source_id IS NULL

    UNION ALL

    SELECT 'Rejected ages'::VARCHAR,
           COUNT(*)::BIGINT,
           'INFO'::VARCHAR
    FROM age_determination
    WHERE is_rejected = TRUE;

END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- RECORD COUNT VERIFICATION
-- =============================================================================

-- Compare counts against expected values from source
CREATE OR REPLACE FUNCTION verify_record_counts(
    expected_sites INTEGER DEFAULT 1748,
    expected_c14 INTEGER DEFAULT 5044,
    expected_non_c14 INTEGER DEFAULT 478
)
RETURNS TABLE (
    metric VARCHAR,
    expected INTEGER,
    actual BIGINT,
    status VARCHAR
) AS $$
BEGIN
    RETURN QUERY

    SELECT 'Total sites'::VARCHAR,
           expected_sites,
           (SELECT COUNT(*) FROM site)::BIGINT,
           CASE WHEN (SELECT COUNT(*) FROM site) >= expected_sites * 0.95
                THEN 'PASS' ELSE 'CHECK' END::VARCHAR

    UNION ALL

    SELECT 'Radiocarbon ages'::VARCHAR,
           expected_c14,
           (SELECT COUNT(*) FROM age_determination ad
            JOIN dating_method dm ON ad.method_id = dm.id
            WHERE dm.code IN ('C14', 'AMS', 'CONV'))::BIGINT,
           CASE WHEN (SELECT COUNT(*) FROM age_determination ad
                      JOIN dating_method dm ON ad.method_id = dm.id
                      WHERE dm.code IN ('C14', 'AMS', 'CONV')) >= expected_c14 * 0.95
                THEN 'PASS' ELSE 'CHECK' END::VARCHAR

    UNION ALL

    SELECT 'Non-radiocarbon ages'::VARCHAR,
           expected_non_c14,
           (SELECT COUNT(*) FROM age_determination ad
            JOIN dating_method dm ON ad.method_id = dm.id
            WHERE dm.code NOT IN ('C14', 'AMS', 'CONV'))::BIGINT,
           CASE WHEN (SELECT COUNT(*) FROM age_determination ad
                      JOIN dating_method dm ON ad.method_id = dm.id
                      WHERE dm.code NOT IN ('C14', 'AMS', 'CONV')) >= expected_non_c14 * 0.90
                THEN 'PASS' ELSE 'CHECK' END::VARCHAR

    UNION ALL

    SELECT 'Bioregions with data'::VARCHAR,
           75,
           (SELECT COUNT(DISTINCT bioregion_id) FROM site WHERE bioregion_id IS NOT NULL)::BIGINT,
           CASE WHEN (SELECT COUNT(DISTINCT bioregion_id) FROM site WHERE bioregion_id IS NOT NULL) >= 70
                THEN 'PASS' ELSE 'CHECK' END::VARCHAR;

END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SPATIAL INTEGRITY CHECKS
-- =============================================================================

-- Verify all points are valid PostGIS geometries
CREATE OR REPLACE VIEW v_invalid_geometries AS
SELECT
    id,
    site_name,
    ST_IsValidReason(geom) AS reason
FROM site
WHERE geom IS NOT NULL AND NOT ST_IsValid(geom);

-- Check for sites far from any bioregion
CREATE OR REPLACE VIEW v_unassigned_locations AS
SELECT
    s.id,
    s.site_name,
    s.latitude,
    s.longitude,
    s.state
FROM site s
WHERE s.geom IS NOT NULL
  AND s.bioregion_id IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM bioregion b
      WHERE ST_DWithin(s.geom, b.geom, 0.5) -- within ~50km
  );

-- =============================================================================
-- COMPREHENSIVE VALIDATION REPORT
-- =============================================================================

CREATE OR REPLACE FUNCTION generate_validation_report()
RETURNS TABLE (
    category VARCHAR,
    check_name VARCHAR,
    result VARCHAR,
    details TEXT
) AS $$
BEGIN
    -- Record counts
    RETURN QUERY
    SELECT 'Counts'::VARCHAR, 'Total records'::VARCHAR,
           'INFO'::VARCHAR,
           format('Sites: %s, Samples: %s, Ages: %s',
               (SELECT COUNT(*) FROM site),
               (SELECT COUNT(*) FROM sample),
               (SELECT COUNT(*) FROM age_determination))::TEXT;

    -- Coordinate validation
    RETURN QUERY
    SELECT 'Spatial'::VARCHAR, 'Coordinate validity'::VARCHAR,
           CASE WHEN (SELECT COUNT(*) FROM v_invalid_coordinates) = 0
                THEN 'PASS' ELSE 'WARN' END::VARCHAR,
           format('%s sites with invalid coordinates',
               (SELECT COUNT(*) FROM v_invalid_coordinates))::TEXT;

    -- Geometry validity
    RETURN QUERY
    SELECT 'Spatial'::VARCHAR, 'Geometry validity'::VARCHAR,
           CASE WHEN (SELECT COUNT(*) FROM v_invalid_geometries) = 0
                THEN 'PASS' ELSE 'WARN' END::VARCHAR,
           format('%s invalid geometries',
               (SELECT COUNT(*) FROM v_invalid_geometries))::TEXT;

    -- Bioregion assignment
    RETURN QUERY
    SELECT 'Spatial'::VARCHAR, 'Bioregion coverage'::VARCHAR,
           'INFO'::VARCHAR,
           format('%s of %s sites assigned to bioregions',
               (SELECT COUNT(*) FROM site WHERE bioregion_id IS NOT NULL),
               (SELECT COUNT(*) FROM site))::TEXT;

    -- Lab code duplicates
    RETURN QUERY
    SELECT 'Data Quality'::VARCHAR, 'Duplicate lab codes'::VARCHAR,
           CASE WHEN (SELECT COUNT(*) FROM v_duplicate_lab_codes) = 0
                THEN 'PASS' ELSE 'WARN' END::VARCHAR,
           format('%s duplicate lab codes found',
               (SELECT COUNT(*) FROM v_duplicate_lab_codes))::TEXT;

    -- Quality issues
    RETURN QUERY
    SELECT 'Data Quality'::VARCHAR, 'Age quality issues'::VARCHAR,
           CASE WHEN (SELECT COUNT(*) FROM v_quality_issues) <
                     (SELECT COUNT(*) FROM age_determination) * 0.05
                THEN 'PASS' ELSE 'WARN' END::VARCHAR,
           format('%s ages with quality issues (%.1f%%)',
               (SELECT COUNT(*) FROM v_quality_issues),
               (SELECT COUNT(*) FROM v_quality_issues)::FLOAT /
               NULLIF((SELECT COUNT(*) FROM age_determination), 0) * 100)::TEXT;

    -- Temporal distribution
    RETURN QUERY
    SELECT 'Analysis'::VARCHAR, 'Holocene concentration'::VARCHAR,
           'INFO'::VARCHAR,
           format('%.1f%% of ages are Holocene (<11,700 BP)',
               (SELECT COUNT(*) FILTER (WHERE COALESCE(age_bp, c14_age) < 11700)::FLOAT /
                NULLIF(COUNT(*), 0) * 100
                FROM age_determination))::TEXT;

END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- DATA INTEGRITY CHECK (orphan detection)
-- =============================================================================

CREATE OR REPLACE FUNCTION check_data_integrity()
RETURNS TABLE(
    check_name TEXT,
    issue_count BIGINT,
    severity TEXT
) AS $$
BEGIN
    -- Orphaned sites (no samples)
    RETURN QUERY
    SELECT 'Orphaned sites (no samples)'::TEXT,
           COUNT(*)::BIGINT,
           'ERROR'::TEXT
    FROM site s
    LEFT JOIN sample sa ON sa.site_id = s.id
    WHERE sa.id IS NULL;

    -- Orphaned samples (no age_determination)
    RETURN QUERY
    SELECT 'Orphaned samples (no age_determination)'::TEXT,
           COUNT(*)::BIGINT,
           'ERROR'::TEXT
    FROM sample s
    LEFT JOIN age_determination ad ON ad.sample_id = s.id
    WHERE ad.id IS NULL;

    -- Empty import batches
    RETURN QUERY
    SELECT 'Empty import batches'::TEXT,
           COUNT(*)::BIGINT,
           'WARNING'::TEXT
    FROM import_batch ib
    LEFT JOIN site s ON s.import_batch_id = ib.id
    LEFT JOIN age_determination ad ON ad.import_batch_id = ib.id
    WHERE s.id IS NULL AND ad.id IS NULL;

    -- Only flag non-rejected ages without values
    RETURN QUERY
    SELECT 'Non-rejected ages without age values'::TEXT,
           COUNT(*)::BIGINT,
           'WARNING'::TEXT
    FROM age_determination
    WHERE c14_age IS NULL AND lum_age_ka IS NULL AND age_bp IS NULL
      AND NOT is_rejected;

    -- Inverted calibrated age ranges
    RETURN QUERY
    SELECT 'Inverted calibrated age ranges (from < to)'::TEXT,
           COUNT(*)::BIGINT,
           'WARNING'::TEXT
    FROM age_determination
    WHERE cal_age_bp_from IS NOT NULL
      AND cal_age_bp_to IS NOT NULL
      AND cal_age_bp_from < cal_age_bp_to;

    -- Unreasonable C14 ages (> 60,000 years)
    RETURN QUERY
    SELECT 'Unreasonable C14 ages (> 60,000 BP)'::TEXT,
           COUNT(*)::BIGINT,
           'WARNING'::TEXT
    FROM age_determination ad
    JOIN dating_method dm ON ad.method_id = dm.id
    WHERE dm.code IN ('C14', 'AMS', 'CONV') AND ad.c14_age > 60000;

    -- Sites without coordinates
    RETURN QUERY
    SELECT 'Sites without coordinates'::TEXT,
           COUNT(*)::BIGINT,
           'INFO'::TEXT
    FROM site
    WHERE latitude IS NULL OR longitude IS NULL;

    -- Info: total rejected ages
    RETURN QUERY
    SELECT 'Rejected ages (info only)'::TEXT,
           COUNT(*)::BIGINT,
           'INFO'::TEXT
    FROM age_determination
    WHERE is_rejected = TRUE;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION check_data_integrity() IS 'Returns data integrity issues including orphaned records and quality problems';

-- =============================================================================
-- END OF VALIDATION
-- =============================================================================
