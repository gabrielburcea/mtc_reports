-- =============================================================================
-- Raw source table: claimcare
-- =============================================================================
-- One row per pupil per academic year recording FSM eligibility and
-- disadvantage status as derived from DfE claimcare data.
--
-- Place in:  <catalog>.<schema>.claimcare
-- =============================================================================

CREATE TABLE IF NOT EXISTS claimcare (
    -- Links
    pupil_id            STRING    NOT NULL COMMENT 'Matches pupil.pupil_id',
    acad_year_code      STRING    NOT NULL COMMENT 'Academic year code, e.g. "202122"',

    -- Disadvantage / FSM flags (raw codes)
    claim_type          STRING             COMMENT 'Disadvantage status: "1"/"Y" = Disadvantaged, "0"/"N" = Not known to be disadvantaged',
    fsm_claim_flag      STRING             COMMENT 'FSM eligibility: "1"/"Y" = FSM eligible, "0"/"N" = Not known to be FSM eligible',

    -- Additional claimcare variables
    ever_fsm_6          STRING             COMMENT 'Ever FSM in last 6 years flag: "1"/"Y" / "0"/"N"',
    pupil_premium_flag  STRING             COMMENT 'Pupil premium recipient flag: "1"/"Y" / "0"/"N"',
    service_child_flag  STRING             COMMENT 'Service child flag: "1"/"Y" / "0"/"N"',
    lac_flag            STRING             COMMENT 'Looked-after child flag: "1"/"Y" / "0"/"N"',

    -- Audit
    load_date           DATE               COMMENT 'Date the record was loaded into the catalog'
)
USING DELTA
COMMENT 'Raw DfE claimcare data recording FSM and disadvantage eligibility. All flags are raw codes; see sql/views/v_claimcare.sql for decoded version.'
PARTITIONED BY (acad_year_code);
