-- =============================================================================
-- Raw source table: pupil
-- =============================================================================
-- One row per pupil per academic year.
-- Holds NPD census-derived characteristics with DfE/NPD short codes as values.
--
-- Place in:  <catalog>.<schema>.pupil
-- =============================================================================

CREATE TABLE IF NOT EXISTS pupil (
    -- Primary key
    pupil_id             STRING    NOT NULL COMMENT 'Unique anonymised pupil identifier',

    -- Academic year
    acad_year_code       STRING    NOT NULL COMMENT 'Academic year code, e.g. "202122"',

    -- Establishment link
    school_id            STRING    NOT NULL COMMENT 'URN or anonymised establishment identifier',
    estab_type_group     STRING             COMMENT 'Establishment type group code, e.g. "AC", "LA", "AP"',

    -- Geography (raw codes)
    la_code_new          STRING             COMMENT 'New LA code, e.g. "E06000047"',
    la_code_old          STRING             COMMENT 'Old/legacy LA code, e.g. "840"',

    -- Demographics (raw codes)
    gender               STRING             COMMENT 'Gender code: "M" = Male / Boys, "F" = Female / Girls',
    ethnic_major_group   STRING             COMMENT 'Ethnicity major group code, e.g. "WHITE", "ASIAN", "BLACK"',
    ethnic_minor_group   STRING             COMMENT 'Ethnicity minor group code (DfE NPD), e.g. "WBRI", "AIND", "MWBC"',
    first_lang           STRING             COMMENT 'First language code: "ENG", "OTH", "UNK"',

    -- Disadvantage / FSM (raw flags)
    disadvantaged_flag   STRING             COMMENT 'Disadvantage flag: "1"/"Y" = Disadvantaged, "0"/"N" = Not known to be disadvantaged',
    fsm_eligible_flag    STRING             COMMENT 'FSM eligibility flag: "1"/"Y" = FSM eligible, "0"/"N" = Not known to be FSM eligible',

    -- Birth month
    birth_month_code     STRING             COMMENT 'Birth month as integer string "1"–"12"',

    -- SEN
    sen_prov_code        STRING             COMMENT 'SEN provision code: "E" = EHC plan, "K" = SEN support, "N" = No SEN',
    sen_type_rank        STRING             COMMENT 'SEN primary need code, e.g. "AUT", "SEMH", "SPLD"',

    -- Audit
    load_date            DATE               COMMENT 'Date the record was loaded into the catalog'
)
USING DELTA
COMMENT 'Raw NPD pupil census extract. Category values are DfE short codes; see sql/views/v_pupil.sql for decoded version.'
PARTITIONED BY (acad_year_code);
