-- =============================================================================
-- Silver view: v_claimcare
-- =============================================================================
-- Renames and decodes the DfE claimcare flags for FSM eligibility and
-- disadvantage status.
-- =============================================================================

CREATE OR REPLACE VIEW v_claimcare AS
SELECT
    pupil_id,
    acad_year_code                              AS time_period,

    -- Disadvantage status  (raw: claim_type)
    CASE claim_type
        WHEN '1'            THEN 'Disadvantaged'
        WHEN 'Y'            THEN 'Disadvantaged'
        WHEN 'True'         THEN 'Disadvantaged'
        WHEN 'Disadvantaged' THEN 'Disadvantaged'
        WHEN '0'            THEN 'Not known to be disadvantaged'
        WHEN 'N'            THEN 'Not known to be disadvantaged'
        WHEN 'False'        THEN 'Not known to be disadvantaged'
        WHEN 'T'            THEN 'Total'
        WHEN 'TOTAL'        THEN 'Total'
        ELSE claim_type
    END                                         AS disadvantage_status,

    -- FSM eligibility  (raw: fsm_claim_flag)
    CASE fsm_claim_flag
        WHEN '1'    THEN 'FSM eligible'
        WHEN 'Y'    THEN 'FSM eligible'
        WHEN 'True' THEN 'FSM eligible'
        WHEN '0'    THEN 'Not known to be FSM eligible'
        WHEN 'N'    THEN 'Not known to be FSM eligible'
        WHEN 'False' THEN 'Not known to be FSM eligible'
        WHEN 'T'     THEN 'Total'
        WHEN 'TOTAL' THEN 'Total'
        ELSE fsm_claim_flag
    END                                         AS fsm_status

FROM claimcare;
