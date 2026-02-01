-- =============================================================================
-- AustArch Database Schema
-- Australian Archaeological Dating Database
-- =============================================================================
-- PostgreSQL 15+ with PostGIS 3.x
-- =============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- =============================================================================
-- REFERENCE TABLES
-- =============================================================================

-- Dating method reference table
CREATE TABLE dating_method (
    id SERIAL PRIMARY KEY,
    code VARCHAR(20) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    is_radiometric BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE dating_method IS 'Reference table for archaeological dating techniques';

-- Sample material reference table
CREATE TABLE sample_material (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50), -- organic, inorganic, mixed
    suitable_for_c14 BOOLEAN DEFAULT TRUE,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE sample_material IS 'Reference table for dateable material types';

-- Data source / publication reference
CREATE TABLE data_source (
    id SERIAL PRIMARY KEY,
    citation TEXT NOT NULL,
    author VARCHAR(500),
    year INTEGER,
    title TEXT,
    journal VARCHAR(255),
    volume VARCHAR(50),
    pages VARCHAR(50),
    doi VARCHAR(255),
    url TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_data_source_author ON data_source USING gin(author gin_trgm_ops);
CREATE INDEX idx_data_source_year ON data_source(year);

COMMENT ON TABLE data_source IS 'Publication and citation references for age determinations';

-- =============================================================================
-- BIOREGION TABLE
-- =============================================================================

CREATE TABLE bioregion (
    id SERIAL PRIMARY KEY,
    ibra_code VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    state VARCHAR(50),
    area_km2 NUMERIC(12, 2),
    geom GEOMETRY(MultiPolygon, 4283), -- GDA94 for compatibility
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_bioregion_geom ON bioregion USING GIST(geom);
CREATE INDEX idx_bioregion_name ON bioregion USING gin(name gin_trgm_ops);

COMMENT ON TABLE bioregion IS 'IBRA 7.0 bioregions of Australia (89 regions)';

-- =============================================================================
-- SITE TABLE
-- =============================================================================

CREATE TABLE site (
    id SERIAL PRIMARY KEY,
    site_code VARCHAR(50),
    site_name VARCHAR(255) NOT NULL,
    alternate_names TEXT[],

    -- Location
    latitude NUMERIC(10, 7),
    longitude NUMERIC(11, 7),
    geom GEOMETRY(Point, 4283), -- GDA94
    coordinate_precision VARCHAR(50), -- exact, approximate, centroid
    elevation_m NUMERIC(7, 2),

    -- Administrative
    state VARCHAR(50),
    region VARCHAR(100),
    bioregion_id INTEGER REFERENCES bioregion(id),

    -- Site characteristics
    site_type VARCHAR(100), -- rockshelter, open, midden, etc.
    site_context TEXT,
    land_system VARCHAR(100),

    -- Metadata
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    import_batch_id UUID
);

CREATE INDEX idx_site_geom ON site USING GIST(geom);
CREATE INDEX idx_site_name ON site USING gin(site_name gin_trgm_ops);
CREATE INDEX idx_site_bioregion ON site(bioregion_id);
CREATE INDEX idx_site_state ON site(state);
CREATE INDEX idx_site_type ON site(site_type);
CREATE INDEX idx_site_batch ON site(import_batch_id) WHERE import_batch_id IS NOT NULL;

COMMENT ON TABLE site IS 'Archaeological site locations across Australia';

-- =============================================================================
-- SAMPLE TABLE
-- =============================================================================

CREATE TABLE sample (
    id SERIAL PRIMARY KEY,
    site_id INTEGER NOT NULL REFERENCES site(id) ON DELETE CASCADE,

    -- Sample identification
    sample_code VARCHAR(100),
    field_code VARCHAR(100),

    -- Material
    material_id INTEGER REFERENCES sample_material(id),
    material_description TEXT,
    species VARCHAR(255), -- for shell, bone, etc.

    -- Stratigraphic context
    unit VARCHAR(100),
    layer VARCHAR(100),
    spit VARCHAR(50),
    depth_cm_top NUMERIC(7, 2),
    depth_cm_bottom NUMERIC(7, 2),
    depth_description TEXT,

    -- Association
    cultural_association TEXT,
    feature_association TEXT,

    -- Quality
    contamination_notes TEXT,
    pretreatment TEXT,

    -- Metadata
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_sample_site ON sample(site_id);
CREATE INDEX idx_sample_material ON sample(material_id);

COMMENT ON TABLE sample IS 'Physical samples collected for dating';

-- =============================================================================
-- AGE DETERMINATION TABLE
-- =============================================================================

CREATE TABLE age_determination (
    id SERIAL PRIMARY KEY,
    sample_id INTEGER NOT NULL REFERENCES sample(id) ON DELETE CASCADE,

    -- Laboratory identification
    lab_code VARCHAR(50) NOT NULL, -- e.g., "OZA-123", "Wk-12345"
    lab_name VARCHAR(100),

    -- Dating method
    method_id INTEGER REFERENCES dating_method(id),

    -- Radiocarbon specific fields
    c14_age INTEGER, -- uncalibrated years BP
    c14_error INTEGER, -- 1-sigma error
    delta_c13 NUMERIC(6, 2), -- per mille
    percent_modern_carbon NUMERIC(8, 4),

    -- Calibrated age (2-sigma range)
    cal_age_bp_from INTEGER, -- older bound
    cal_age_bp_to INTEGER, -- younger bound
    cal_range INT4RANGE, -- PostgreSQL range type
    CONSTRAINT chk_cal_range CHECK (
        cal_age_bp_from IS NULL OR cal_age_bp_to IS NULL OR cal_age_bp_from >= cal_age_bp_to
    ),
    calibration_curve VARCHAR(50), -- SHCal20, IntCal20, etc.

    -- Luminescence specific fields (OSL, TL)
    lum_age_ka NUMERIC(8, 3), -- kiloyears
    lum_error_ka NUMERIC(8, 3),
    dose_rate NUMERIC(8, 4),
    equivalent_dose NUMERIC(10, 2),

    -- General age fields (for any method)
    age_bp INTEGER, -- best estimate in years BP
    age_error INTEGER,
    age_min_bp INTEGER,
    age_max_bp INTEGER,

    -- Quality assessment
    quality_rating INTEGER CHECK (quality_rating BETWEEN 1 AND 5),
    is_rejected BOOLEAN DEFAULT FALSE,
    rejection_reason TEXT,
    quality_issues TEXT[],

    -- Source
    data_source_id INTEGER REFERENCES data_source(id),
    source_table VARCHAR(50), -- original table in source dataset
    original_row_id INTEGER, -- row number in source

    -- Metadata
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    import_batch_id UUID
);

-- Primary indexes
CREATE UNIQUE INDEX idx_age_lab_code ON age_determination(lab_code)
    WHERE lab_code IS NOT NULL AND lab_code != '';
CREATE INDEX idx_age_sample ON age_determination(sample_id);
CREATE INDEX idx_age_method ON age_determination(method_id);
CREATE INDEX idx_age_source ON age_determination(data_source_id);

-- Age range indexes
CREATE INDEX idx_age_c14 ON age_determination(c14_age) WHERE c14_age IS NOT NULL;
CREATE INDEX idx_age_bp ON age_determination(age_bp) WHERE age_bp IS NOT NULL;
CREATE INDEX idx_age_cal_range ON age_determination USING GIST(cal_range);

-- Quality indexes
CREATE INDEX idx_age_quality ON age_determination(quality_rating);
CREATE INDEX idx_age_rejected ON age_determination(is_rejected) WHERE is_rejected = TRUE;

-- Batch tracking index
CREATE INDEX idx_age_batch ON age_determination(import_batch_id) WHERE import_batch_id IS NOT NULL;

COMMENT ON TABLE age_determination IS 'Dating results including radiocarbon and luminescence ages';

-- =============================================================================
-- AUDIT LOG TABLE
-- =============================================================================

CREATE TABLE audit_log (
    id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    record_id INTEGER NOT NULL,
    action VARCHAR(10) NOT NULL, -- INSERT, UPDATE, DELETE
    old_values JSONB,
    new_values JSONB,
    changed_by VARCHAR(100) DEFAULT current_user,
    changed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_audit_table ON audit_log(table_name, record_id);
CREATE INDEX idx_audit_time ON audit_log(changed_at);

-- =============================================================================
-- IMPORT TRACKING TABLE
-- =============================================================================

CREATE TABLE import_batch (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_file VARCHAR(255),
    source_url TEXT,
    record_count INTEGER,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status VARCHAR(20) DEFAULT 'running', -- running, completed, failed
    notes TEXT,
    error_log TEXT
);

-- =============================================================================
-- TRIGGERS
-- =============================================================================

-- Auto-generate PostGIS point from lat/lon
CREATE OR REPLACE FUNCTION update_site_geom()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
        NEW.geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4283);
    END IF;
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_site_geom
    BEFORE INSERT OR UPDATE ON site
    FOR EACH ROW
    EXECUTE FUNCTION update_site_geom();

-- Auto-assign bioregion via spatial join
CREATE OR REPLACE FUNCTION assign_bioregion()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.geom IS NOT NULL AND NEW.bioregion_id IS NULL THEN
        SELECT id INTO NEW.bioregion_id
        FROM bioregion
        WHERE ST_Contains(geom, NEW.geom)
        LIMIT 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_assign_bioregion
    BEFORE INSERT OR UPDATE ON site
    FOR EACH ROW
    EXECUTE FUNCTION assign_bioregion();

-- Update timestamp trigger
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sample_timestamp
    BEFORE UPDATE ON sample
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER trg_age_timestamp
    BEFORE UPDATE ON age_determination
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp();

-- Auto-populate cal_range from cal_age_bp fields
CREATE OR REPLACE FUNCTION update_cal_range()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.cal_age_bp_from IS NOT NULL AND NEW.cal_age_bp_to IS NOT NULL THEN
        NEW.cal_range := INT4RANGE(NEW.cal_age_bp_to, NEW.cal_age_bp_from, '[]');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_cal_range
    BEFORE INSERT OR UPDATE ON age_determination
    FOR EACH ROW
    EXECUTE FUNCTION update_cal_range();

-- Audit trigger function
CREATE OR REPLACE FUNCTION audit_trigger_func()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO audit_log(table_name, record_id, action, old_values)
        VALUES (TG_TABLE_NAME, OLD.id, 'DELETE', to_jsonb(OLD));
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit_log(table_name, record_id, action, old_values, new_values)
        VALUES (TG_TABLE_NAME, NEW.id, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO audit_log(table_name, record_id, action, new_values)
        VALUES (TG_TABLE_NAME, NEW.id, 'INSERT', to_jsonb(NEW));
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Apply audit triggers (commented out by default for performance)
-- CREATE TRIGGER audit_site AFTER INSERT OR UPDATE OR DELETE ON site
--     FOR EACH ROW EXECUTE FUNCTION audit_trigger_func();
-- CREATE TRIGGER audit_age AFTER INSERT OR UPDATE OR DELETE ON age_determination
--     FOR EACH ROW EXECUTE FUNCTION audit_trigger_func();

-- =============================================================================
-- VIEWS
-- =============================================================================

-- Comprehensive view joining all main tables
CREATE OR REPLACE VIEW v_age_determinations AS
SELECT
    ad.id AS age_id,
    ad.lab_code,
    dm.code AS method_code,
    dm.name AS method_name,

    -- Ages
    ad.c14_age,
    ad.c14_error,
    ad.cal_age_bp_from,
    ad.cal_age_bp_to,
    ad.lum_age_ka,
    ad.lum_error_ka,
    ad.age_bp,
    ad.age_error,

    -- Sample info
    s.id AS sample_id,
    sm.name AS material_name,
    s.depth_cm_top,
    s.depth_cm_bottom,

    -- Site info
    si.id AS site_id,
    si.site_name,
    si.site_type,
    si.latitude,
    si.longitude,
    si.state,

    -- Bioregion
    br.ibra_code,
    br.name AS bioregion_name,

    -- Quality
    ad.quality_rating,
    ad.is_rejected,

    -- Source
    ds.citation,
    ds.author,
    ds.year AS pub_year

FROM age_determination ad
LEFT JOIN sample s ON ad.sample_id = s.id
LEFT JOIN site si ON s.site_id = si.id
LEFT JOIN bioregion br ON si.bioregion_id = br.id
LEFT JOIN dating_method dm ON ad.method_id = dm.id
LEFT JOIN sample_material sm ON s.material_id = sm.id
LEFT JOIN data_source ds ON ad.data_source_id = ds.id;

-- Temporal distribution view (1000-year brackets)
CREATE OR REPLACE VIEW v_temporal_distribution AS
SELECT
    (COALESCE(age_bp, c14_age) / 1000) * 1000 AS age_bracket_bp,
    dm.code AS method,
    COUNT(*) AS count,
    COUNT(*) FILTER (WHERE NOT is_rejected) AS valid_count
FROM age_determination ad
LEFT JOIN dating_method dm ON ad.method_id = dm.id
WHERE COALESCE(age_bp, c14_age) IS NOT NULL
GROUP BY age_bracket_bp, dm.code
ORDER BY age_bracket_bp;

-- Site summary view
CREATE OR REPLACE VIEW v_site_summary AS
SELECT
    si.id AS site_id,
    si.site_name,
    si.site_type,
    si.state,
    br.name AS bioregion,
    COUNT(DISTINCT s.id) AS sample_count,
    COUNT(DISTINCT ad.id) AS date_count,
    MIN(COALESCE(ad.age_bp, ad.c14_age)) AS youngest_age_bp,
    MAX(COALESCE(ad.age_bp, ad.c14_age)) AS oldest_age_bp
FROM site si
LEFT JOIN bioregion br ON si.bioregion_id = br.id
LEFT JOIN sample s ON s.site_id = si.id
LEFT JOIN age_determination ad ON ad.sample_id = s.id
GROUP BY si.id, si.site_name, si.site_type, si.state, br.name;

-- Bioregion coverage summary
CREATE OR REPLACE VIEW v_bioregion_coverage AS
SELECT
    br.ibra_code,
    br.name AS bioregion_name,
    br.state,
    COUNT(DISTINCT si.id) AS site_count,
    COUNT(DISTINCT ad.id) AS date_count
FROM bioregion br
LEFT JOIN site si ON si.bioregion_id = br.id
LEFT JOIN sample s ON s.site_id = si.id
LEFT JOIN age_determination ad ON ad.sample_id = s.id
GROUP BY br.id, br.ibra_code, br.name, br.state
ORDER BY site_count DESC;

-- =============================================================================
-- MATERIALIZED VIEWS (for performance on large datasets)
-- =============================================================================

-- Materialized summary statistics
CREATE MATERIALIZED VIEW mv_summary_stats AS
SELECT
    'total_sites' AS metric, COUNT(DISTINCT id)::TEXT AS value FROM site
UNION ALL
SELECT 'total_samples', COUNT(*)::TEXT FROM sample
UNION ALL
SELECT 'total_ages', COUNT(*)::TEXT FROM age_determination
UNION ALL
SELECT 'radiocarbon_ages', COUNT(*)::TEXT FROM age_determination ad
    JOIN dating_method dm ON ad.method_id = dm.id WHERE dm.code = 'C14'
UNION ALL
SELECT 'osl_ages', COUNT(*)::TEXT FROM age_determination ad
    JOIN dating_method dm ON ad.method_id = dm.id WHERE dm.code = 'OSL'
UNION ALL
SELECT 'tl_ages', COUNT(*)::TEXT FROM age_determination ad
    JOIN dating_method dm ON ad.method_id = dm.id WHERE dm.code = 'TL'
UNION ALL
SELECT 'rejected_ages', COUNT(*)::TEXT FROM age_determination WHERE is_rejected = TRUE
UNION ALL
SELECT 'bioregions_represented', COUNT(DISTINCT bioregion_id)::TEXT FROM site WHERE bioregion_id IS NOT NULL;

CREATE UNIQUE INDEX idx_mv_summary ON mv_summary_stats(metric);

-- Refresh function for materialized views
CREATE OR REPLACE FUNCTION refresh_materialized_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_summary_stats;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- PARTITIONING STRATEGY (for future growth >100k records)
-- =============================================================================

-- Documented for reference - implement when needed:
--
-- CREATE TABLE age_determination_partitioned (
--     LIKE age_determination INCLUDING ALL
-- ) PARTITION BY RANGE (age_bp);
--
-- CREATE TABLE age_determination_holocene
--     PARTITION OF age_determination_partitioned
--     FOR VALUES FROM (0) TO (11700);
--
-- CREATE TABLE age_determination_pleistocene
--     PARTITION OF age_determination_partitioned
--     FOR VALUES FROM (11700) TO (2600000);

-- =============================================================================
-- PERMISSIONS (adjust as needed)
-- =============================================================================

-- Create read-only role for analysts
-- CREATE ROLE austarch_reader;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO austarch_reader;
-- GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO austarch_reader;

-- Create editor role for data entry
-- CREATE ROLE austarch_editor;
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO austarch_editor;
-- GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO austarch_editor;

-- =============================================================================
-- END OF SCHEMA
-- =============================================================================
