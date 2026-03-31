-- =============================================================================
-- Raw source table: school
-- =============================================================================
-- One row per establishment.
-- Holds school-level characteristics with DfE short codes as values.
--
-- Place in:  <catalog>.<schema>.school
-- =============================================================================

CREATE TABLE IF NOT EXISTS school (
    -- Primary key
    school_id                    STRING    NOT NULL COMMENT 'URN or anonymised establishment identifier',

    -- Geography
    la_code_new                  STRING             COMMENT 'New LA code, e.g. "E06000047"',
    la_code_old                  STRING             COMMENT 'Old/legacy LA code',
    country_code                 STRING             COMMENT 'Country ONS code, e.g. "E92000001"',
    country_name                 STRING             COMMENT 'Country name, e.g. "England"',

    -- School characteristics (raw codes)
    estab_type_group             STRING             COMMENT 'Establishment type group code, e.g. "AC", "LA", "FS", "AP"',
    phase_of_education           STRING             COMMENT 'Education phase code: "PRI", "JUN", "SEC"',
    religious_character          STRING             COMMENT 'Religious character code: "CE", "RC", "NONE", etc.',

    -- Pupil intake (school-level, raw codes)
    gender                       STRING             COMMENT 'School gender intake code: "M", "F", or mixed indicator',

    -- Audit
    load_date                    DATE               COMMENT 'Date the record was loaded into the catalog'
)
USING DELTA
COMMENT 'Raw establishment (school) characteristics. Category values are DfE short codes; see sql/views/v_school.sql for decoded version.';
