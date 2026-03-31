-- =============================================================================
-- Raw source table: results
-- =============================================================================
-- One row per pupil per academic year.
-- Holds the raw MTC check outcome: score (0–25) and completion status.
--
-- Place in:  <catalog>.<schema>.results
-- =============================================================================

CREATE TABLE IF NOT EXISTS results (
    -- Links
    pupil_id            STRING    NOT NULL COMMENT 'Matches pupil.pupil_id',
    school_id           STRING    NOT NULL COMMENT 'Matches school.school_id',
    acad_year_code      STRING    NOT NULL COMMENT 'Academic year code, e.g. "202122"',

    -- MTC outcome
    pupil_mark          INT                COMMENT 'Raw MTC score 0–25 (null if check not completed)',
    completion_status   STRING             COMMENT 'Check completion status code: "C"=Completed, "A"=Absent, "U"=Unable to participate, "W"=Working below, "J"=Just arrived, "M"=Missing reason',

    -- Audit
    load_date           DATE               COMMENT 'Date the record was loaded into the catalog'
)
USING DELTA
COMMENT 'Raw MTC check results. completion_status values are single-letter codes; see sql/views/v_results.sql for decoded version.'
PARTITIONED BY (acad_year_code);
