-- =============================================================================
-- Raw source table: geography
-- =============================================================================
-- Geographic hierarchy lookup: Local Authority → Region → National.
-- Used to join pupil/school records to their geographic context.
--
-- Place in:  <catalog>.<schema>.geography
-- =============================================================================

CREATE TABLE IF NOT EXISTS geography (
    -- Local authority
    la_new              STRING    NOT NULL COMMENT 'New LA ONS code, e.g. "E06000047"',
    la_old              STRING             COMMENT 'Old/legacy LA code, e.g. "840"',
    la_name_full        STRING             COMMENT 'Local authority name, e.g. "County Durham"',

    -- Region
    gor_code            STRING             COMMENT 'Government Office Region ONS code, e.g. "E12000001"',
    gor_name            STRING             COMMENT 'Government Office Region name, e.g. "North East"',

    -- National
    country_cd          STRING             COMMENT 'Country ONS code, e.g. "E92000001"',
    country_nm          STRING             COMMENT 'Country name, e.g. "England"',

    -- Geographic level indicator
    geographic_lvl      STRING             COMMENT 'Row level: "LOCAL_AUTHORITY", "REGIONAL", or "NATIONAL"',

    -- Audit
    load_date           DATE               COMMENT 'Date the record was loaded into the catalog'
)
USING DELTA
COMMENT 'Geographic hierarchy lookup. Column names use raw aliases; see sql/views/v_geography.sql for renamed version.';
