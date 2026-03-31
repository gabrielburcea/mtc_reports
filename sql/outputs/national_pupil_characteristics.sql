-- =============================================================================
-- Gold output: mtc_national_pupil_characteristics
-- =============================================================================
-- Aggregated national pupil-characteristics report.
-- Joins the Silver views and rolls up to the required dimensions.
-- Run once per academic year or as a CTAS / Delta table refresh.
--
-- Usage:
--   CREATE OR REPLACE TABLE mtc_national_pupil_characteristics AS
--   SELECT * FROM (  <this query>  );
--
-- Replace :acad_year with the target year code, e.g. '202324',
-- or remove the WHERE clause to produce all years at once.
-- =============================================================================

SELECT
    p.time_period,
    p.time_identifier,
    g.geographic_level,
    g.country_code,
    g.country_name,
    p.establishment_type_group,
    p.sex,
    c.disadvantage_status,
    p.ethnicity_major,
    p.ethnicity_minor,
    p.first_language,
    c.fsm_status,
    p.month_of_birth,
    p.sen_provision,
    p.sen_primary_need,

    -- Establishment count
    COUNT(DISTINCT p.school_id)                                        AS establishment_count,

    -- Pupil counts
    COUNT(p.pupil_id)                                                  AS eligible_pupil_count,

    SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END)        AS completed_check_pupil_count,
    SUM(CASE WHEN r.completion_status != 'C'
             OR  r.completion_status IS NULL THEN 1 ELSE 0 END)        AS not_completed_check_pupil_count,
    SUM(CASE WHEN r.completion_status = 'A' THEN 1 ELSE 0 END)        AS absent_pupil_count,
    SUM(CASE WHEN r.completion_status = 'U' THEN 1 ELSE 0 END)        AS unable_to_participate_pupil_count,
    SUM(CASE WHEN r.completion_status = 'W' THEN 1 ELSE 0 END)        AS working_below_pupil_count,
    SUM(CASE WHEN r.completion_status = 'J' THEN 1 ELSE 0 END)        AS just_arrived_pupil_count,
    SUM(CASE WHEN r.completion_status = 'M' THEN 1 ELSE 0 END)        AS missing_reason_pupil_count,

    -- Score totals
    SUM(r.mtc_score)                                                   AS mtc_score_total,
    ROUND(
        SUM(r.mtc_score)
        / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0),
        1
    )                                                                  AS mtc_score_average,

    -- Percentages  (rounded to 0 dp as in the reference CSV)
    ROUND(
        SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(p.pupil_id), 0) * 100, 0
    )                                                                  AS completed_check_pupil_percent,
    ROUND(
        SUM(CASE WHEN r.completion_status != 'C'
                 OR  r.completion_status IS NULL THEN 1 ELSE 0 END)
        / NULLIF(COUNT(p.pupil_id), 0) * 100, 0
    )                                                                  AS not_completed_check_pupil_percent,
    ROUND(
        SUM(CASE WHEN r.completion_status = 'A' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(p.pupil_id), 0) * 100, 0
    )                                                                  AS absent_pupil_percent,
    ROUND(
        SUM(CASE WHEN r.completion_status = 'U' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(p.pupil_id), 0) * 100, 0
    )                                                                  AS unable_to_participate_pupil_percent,
    ROUND(
        SUM(CASE WHEN r.completion_status = 'W' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(p.pupil_id), 0) * 100, 0
    )                                                                  AS working_below_pupil_percent,
    ROUND(
        SUM(CASE WHEN r.completion_status = 'J' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(p.pupil_id), 0) * 100, 0
    )                                                                  AS just_arrived_pupil_percent,
    ROUND(
        SUM(CASE WHEN r.completion_status = 'M' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(p.pupil_id), 0) * 100, 0
    )                                                                  AS missing_reason_pupil_percent

FROM      v_pupil     p
JOIN      v_results   r  ON r.pupil_id = p.pupil_id
                        AND r.time_period = p.time_period
JOIN      v_claimcare c  ON c.pupil_id = p.pupil_id
                        AND c.time_period = p.time_period
JOIN      v_geography g  ON g.new_la_code = p.new_la_code

WHERE g.geographic_level = 'National'
  -- AND p.time_period = :acad_year   -- uncomment to filter to a single year

GROUP BY
    p.time_period,
    p.time_identifier,
    g.geographic_level,
    g.country_code,
    g.country_name,
    p.establishment_type_group,
    p.sex,
    c.disadvantage_status,
    p.ethnicity_major,
    p.ethnicity_minor,
    p.first_language,
    c.fsm_status,
    p.month_of_birth,
    p.sen_provision,
    p.sen_primary_need

ORDER BY
    p.time_period,
    p.establishment_type_group,
    p.sex;
