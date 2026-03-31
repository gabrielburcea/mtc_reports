-- =============================================================================
-- Silver view: v_results
-- =============================================================================
-- Exposes the MTC results table with the score column renamed to the
-- output standard name (pupil_mark → mtc_score) and adds a human-readable
-- completion status label alongside the raw code.
-- =============================================================================

CREATE OR REPLACE VIEW v_results AS
SELECT
    pupil_id,
    school_id,
    acad_year_code                              AS time_period,

    -- Rename score column
    pupil_mark                                  AS mtc_score,

    -- Keep raw code for filtering in aggregation queries
    completion_status,

    -- Human-readable completion status label
    CASE completion_status
        WHEN 'C' THEN 'Completed'
        WHEN 'A' THEN 'Absent'
        WHEN 'U' THEN 'Unable to participate'
        WHEN 'W' THEN 'Working below standard'
        WHEN 'J' THEN 'Just arrived in England'
        WHEN 'M' THEN 'Missing reason'
        ELSE completion_status
    END                                         AS completion_status_label

FROM results;
