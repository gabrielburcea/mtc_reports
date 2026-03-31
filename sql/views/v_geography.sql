-- =============================================================================
-- Silver view: v_geography
-- =============================================================================
-- Renames raw geography column names to the output-standard names and
-- decodes the geographic level indicator.
-- =============================================================================

CREATE OR REPLACE VIEW v_geography AS
SELECT
    -- Local authority
    la_new                                      AS new_la_code,
    la_old                                      AS old_la_code,
    la_name_full                                AS la_name,

    -- Region
    gor_code                                    AS region_code,
    gor_name                                    AS region_name,

    -- National
    country_cd                                  AS country_code,
    country_nm                                  AS country_name,

    -- Geographic level  (raw: geographic_lvl)
    CASE geographic_lvl
        WHEN 'NAT'              THEN 'National'
        WHEN 'NATIONAL'         THEN 'National'
        WHEN 'REG'              THEN 'Regional'
        WHEN 'REGIONAL'         THEN 'Regional'
        WHEN 'LA'               THEN 'Local authority'
        WHEN 'LOCAL_AUTH'       THEN 'Local authority'
        WHEN 'LOCAL_AUTHORITY'  THEN 'Local authority'
        ELSE geographic_lvl
    END                                         AS geographic_level

FROM geography;
