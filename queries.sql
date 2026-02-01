-- =============================================================================
-- AustArch Common Analytical Queries
-- Example queries for archaeological dating analysis
-- =============================================================================

-- =============================================================================
-- TEMPORAL DISTRIBUTION ANALYSIS
-- =============================================================================

-- Temporal distribution by 1000-year brackets
SELECT
    (COALESCE(age_bp, c14_age) / 1000) * 1000 AS age_bracket_bp,
    COUNT(*) AS total_dates,
    COUNT(*) FILTER (WHERE NOT is_rejected) AS valid_dates,
    STRING_AGG(DISTINCT dm.code, ', ' ORDER BY dm.code) AS methods_used
FROM age_determination ad
LEFT JOIN dating_method dm ON ad.method_id = dm.id
WHERE COALESCE(age_bp, c14_age) IS NOT NULL
GROUP BY age_bracket_bp
ORDER BY age_bracket_bp;

-- Holocene vs Pleistocene distribution
SELECT
    CASE
        WHEN COALESCE(age_bp, c14_age) < 11700 THEN 'Holocene'
        ELSE 'Pleistocene'
    END AS epoch,
    dm.name AS method,
    COUNT(*) AS count,
    ROUND(AVG(COALESCE(age_bp, c14_age))) AS mean_age_bp
FROM age_determination ad
LEFT JOIN dating_method dm ON ad.method_id = dm.id
WHERE COALESCE(age_bp, c14_age) IS NOT NULL
  AND NOT ad.is_rejected
GROUP BY epoch, dm.name
ORDER BY epoch, count DESC;

-- Oldest dates per state
SELECT DISTINCT ON (si.state)
    si.state,
    si.site_name,
    ad.lab_code,
    dm.name AS method,
    COALESCE(ad.age_bp, ad.c14_age) AS age_bp,
    COALESCE(ad.age_error, ad.c14_error) AS error
FROM age_determination ad
JOIN sample s ON ad.sample_id = s.id
JOIN site si ON s.site_id = si.id
LEFT JOIN dating_method dm ON ad.method_id = dm.id
WHERE COALESCE(ad.age_bp, ad.c14_age) IS NOT NULL
  AND NOT ad.is_rejected
  AND si.state IS NOT NULL
ORDER BY si.state, COALESCE(ad.age_bp, ad.c14_age) DESC;

-- =============================================================================
-- GEOGRAPHIC ANALYSIS
-- =============================================================================

-- Sites and dates within radius of a point (example: Lake Mungo area)
-- Note: Replace coordinates as needed
WITH target AS (
    SELECT ST_SetSRID(ST_MakePoint(143.05, -33.75), 4283) AS geom
)
SELECT
    si.site_name,
    si.site_type,
    ROUND(ST_Distance(si.geom::geography, t.geom::geography) / 1000) AS distance_km,
    COUNT(ad.id) AS date_count,
    MIN(COALESCE(ad.age_bp, ad.c14_age)) AS youngest_bp,
    MAX(COALESCE(ad.age_bp, ad.c14_age)) AS oldest_bp
FROM site si
CROSS JOIN target t
LEFT JOIN sample s ON s.site_id = si.id
LEFT JOIN age_determination ad ON ad.sample_id = s.id
WHERE ST_DWithin(si.geom::geography, t.geom::geography, 100000) -- 100km radius
GROUP BY si.id, si.site_name, si.site_type, si.geom, t.geom
ORDER BY distance_km;

-- State-level summary
SELECT
    si.state,
    COUNT(DISTINCT si.id) AS site_count,
    COUNT(DISTINCT ad.id) AS date_count,
    MIN(COALESCE(ad.age_bp, ad.c14_age)) AS youngest_bp,
    MAX(COALESCE(ad.age_bp, ad.c14_age)) AS oldest_bp,
    ROUND(AVG(COALESCE(ad.age_bp, ad.c14_age))) AS mean_age_bp
FROM site si
LEFT JOIN sample s ON s.site_id = si.id
LEFT JOIN age_determination ad ON ad.sample_id = s.id
WHERE NOT COALESCE(ad.is_rejected, FALSE)
GROUP BY si.state
ORDER BY date_count DESC;

-- =============================================================================
-- BIOREGION ANALYSIS
-- =============================================================================

-- Bioregion coverage summary
SELECT
    br.ibra_code,
    br.name AS bioregion,
    br.state,
    COUNT(DISTINCT si.id) AS site_count,
    COUNT(DISTINCT ad.id) AS date_count,
    MAX(COALESCE(ad.age_bp, ad.c14_age)) AS oldest_bp
FROM bioregion br
LEFT JOIN site si ON si.bioregion_id = br.id
LEFT JOIN sample s ON s.site_id = si.id
LEFT JOIN age_determination ad ON ad.sample_id = s.id
GROUP BY br.id, br.ibra_code, br.name, br.state
HAVING COUNT(DISTINCT si.id) > 0
ORDER BY date_count DESC;

-- Bioregions without any dated sites
SELECT
    br.ibra_code,
    br.name,
    br.state
FROM bioregion br
LEFT JOIN site si ON si.bioregion_id = br.id
WHERE si.id IS NULL
ORDER BY br.name;

-- =============================================================================
-- MATERIAL TYPE ANALYSIS
-- =============================================================================

-- Material type distribution
SELECT
    COALESCE(sm.name, 'Unknown') AS material,
    COUNT(*) AS sample_count,
    COUNT(ad.id) AS date_count,
    ROUND(AVG(COALESCE(ad.age_bp, ad.c14_age))) AS mean_age_bp,
    SUM(CASE WHEN ad.is_rejected THEN 1 ELSE 0 END) AS rejected_count,
    ROUND(SUM(CASE WHEN ad.is_rejected THEN 1 ELSE 0 END)::NUMERIC /
          NULLIF(COUNT(ad.id), 0) * 100, 1) AS rejection_rate_pct
FROM sample s
LEFT JOIN sample_material sm ON s.material_id = sm.id
LEFT JOIN age_determination ad ON ad.sample_id = s.id
GROUP BY sm.name
ORDER BY date_count DESC;

-- Charcoal vs shell success rates
SELECT
    sm.category,
    sm.name AS material,
    COUNT(*) FILTER (WHERE NOT COALESCE(ad.is_rejected, FALSE)) AS valid_dates,
    COUNT(*) FILTER (WHERE ad.is_rejected) AS rejected_dates,
    ROUND(COUNT(*) FILTER (WHERE NOT COALESCE(ad.is_rejected, FALSE))::NUMERIC /
          NULLIF(COUNT(*), 0) * 100, 1) AS success_rate_pct
FROM sample s
JOIN sample_material sm ON s.material_id = sm.id
JOIN age_determination ad ON ad.sample_id = s.id
WHERE sm.name IN ('Charcoal', 'Marine Shell', 'Freshwater Shell', 'Bone')
GROUP BY sm.category, sm.name
ORDER BY success_rate_pct DESC;

-- =============================================================================
-- DATING METHOD COMPARISON
-- =============================================================================

-- Radiocarbon vs luminescence comparison
SELECT
    CASE
        WHEN dm.code IN ('C14', 'AMS', 'CONV') THEN 'Radiocarbon'
        WHEN dm.code IN ('OSL', 'TL', 'IRSL') THEN 'Luminescence'
        ELSE 'Other'
    END AS method_group,
    dm.name AS method,
    COUNT(*) AS count,
    MIN(COALESCE(ad.age_bp, ad.c14_age, ad.lum_age_ka * 1000)) AS min_age_bp,
    MAX(COALESCE(ad.age_bp, ad.c14_age, ad.lum_age_ka * 1000)) AS max_age_bp,
    ROUND(AVG(COALESCE(ad.age_bp, ad.c14_age, ad.lum_age_ka * 1000))) AS mean_age_bp
FROM age_determination ad
JOIN dating_method dm ON ad.method_id = dm.id
WHERE NOT ad.is_rejected
GROUP BY method_group, dm.name
ORDER BY method_group, count DESC;

-- Sites with both C14 and luminescence dates
SELECT
    si.site_name,
    si.state,
    COUNT(*) FILTER (WHERE dm.code IN ('C14', 'AMS', 'CONV')) AS c14_count,
    COUNT(*) FILTER (WHERE dm.code IN ('OSL', 'TL', 'IRSL')) AS lum_count,
    MAX(ad.c14_age) FILTER (WHERE dm.code IN ('C14', 'AMS', 'CONV')) AS oldest_c14,
    MAX(ad.lum_age_ka * 1000) FILTER (WHERE dm.code IN ('OSL', 'TL', 'IRSL')) AS oldest_lum_bp
FROM site si
JOIN sample s ON s.site_id = si.id
JOIN age_determination ad ON ad.sample_id = s.id
JOIN dating_method dm ON ad.method_id = dm.id
WHERE NOT ad.is_rejected
GROUP BY si.id, si.site_name, si.state
HAVING COUNT(*) FILTER (WHERE dm.code IN ('C14', 'AMS', 'CONV')) > 0
   AND COUNT(*) FILTER (WHERE dm.code IN ('OSL', 'TL', 'IRSL')) > 0
ORDER BY oldest_lum_bp DESC NULLS LAST;

-- =============================================================================
-- QUALITY ANALYSIS
-- =============================================================================

-- Quality rating distribution
SELECT
    quality_rating,
    COUNT(*) AS count,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 1) AS percentage
FROM age_determination
WHERE quality_rating IS NOT NULL
GROUP BY quality_rating
ORDER BY quality_rating;

-- Common rejection reasons
SELECT
    rejection_reason,
    COUNT(*) AS count
FROM age_determination
WHERE is_rejected = TRUE
  AND rejection_reason IS NOT NULL
GROUP BY rejection_reason
ORDER BY count DESC
LIMIT 20;

-- =============================================================================
-- TIME SERIES EXPORT (for OxCal)
-- =============================================================================

-- Export format suitable for OxCal Sum plotting
SELECT
    ad.lab_code AS "Name",
    ad.c14_age AS "BP",
    ad.c14_error AS "Error"
FROM age_determination ad
JOIN dating_method dm ON ad.method_id = dm.id
WHERE dm.code IN ('C14', 'AMS', 'CONV')
  AND ad.c14_age IS NOT NULL
  AND ad.c14_error IS NOT NULL
  AND NOT ad.is_rejected
ORDER BY ad.c14_age;

-- Export with site context for regional analysis
SELECT
    si.site_name,
    si.state,
    br.ibra_code AS bioregion,
    ad.lab_code,
    sm.name AS material,
    ad.c14_age AS bp,
    ad.c14_error AS error,
    ad.delta_c13
FROM age_determination ad
JOIN sample s ON ad.sample_id = s.id
JOIN site si ON s.site_id = si.id
LEFT JOIN bioregion br ON si.bioregion_id = br.id
LEFT JOIN sample_material sm ON s.material_id = sm.id
JOIN dating_method dm ON ad.method_id = dm.id
WHERE dm.code IN ('C14', 'AMS', 'CONV')
  AND ad.c14_age IS NOT NULL
  AND NOT ad.is_rejected
ORDER BY si.state, ad.c14_age;

-- =============================================================================
-- SITE TYPE ANALYSIS
-- =============================================================================

-- Site type distribution
SELECT
    COALESCE(site_type, 'Unknown') AS site_type,
    COUNT(*) AS site_count,
    COUNT(DISTINCT si.state) AS states_represented
FROM site si
GROUP BY site_type
ORDER BY site_count DESC;

-- Dating intensity by site type
SELECT
    COALESCE(si.site_type, 'Unknown') AS site_type,
    COUNT(DISTINCT si.id) AS site_count,
    COUNT(ad.id) AS date_count,
    ROUND(COUNT(ad.id)::NUMERIC / COUNT(DISTINCT si.id), 1) AS dates_per_site
FROM site si
LEFT JOIN sample s ON s.site_id = si.id
LEFT JOIN age_determination ad ON ad.sample_id = s.id
GROUP BY si.site_type
ORDER BY dates_per_site DESC;

-- =============================================================================
-- PUBLICATION/SOURCE ANALYSIS
-- =============================================================================

-- Most cited sources
SELECT
    ds.author,
    ds.year,
    LEFT(ds.title, 80) AS title_excerpt,
    COUNT(ad.id) AS date_count
FROM data_source ds
JOIN age_determination ad ON ad.data_source_id = ds.id
GROUP BY ds.id, ds.author, ds.year, ds.title
ORDER BY date_count DESC
LIMIT 20;

-- Publication year distribution
SELECT
    ds.year,
    COUNT(ad.id) AS dates_published
FROM data_source ds
JOIN age_determination ad ON ad.data_source_id = ds.id
WHERE ds.year IS NOT NULL
GROUP BY ds.year
ORDER BY ds.year;

-- =============================================================================
-- DEPTH ANALYSIS
-- =============================================================================

-- Age vs depth relationship (for stratigraphic sites)
SELECT
    si.site_name,
    s.depth_cm_top,
    COALESCE(ad.age_bp, ad.c14_age) AS age_bp,
    dm.name AS method
FROM age_determination ad
JOIN sample s ON ad.sample_id = s.id
JOIN site si ON s.site_id = si.id
LEFT JOIN dating_method dm ON ad.method_id = dm.id
WHERE s.depth_cm_top IS NOT NULL
  AND COALESCE(ad.age_bp, ad.c14_age) IS NOT NULL
  AND NOT ad.is_rejected
ORDER BY si.site_name, s.depth_cm_top;

-- =============================================================================
-- END OF QUERIES
-- =============================================================================
