-- =============================================================================
-- Gold output: mtc_national_school_characteristics
-- =============================================================================
-- Aggregated national school-characteristics report.
-- =============================================================================

SELECT
    r.time_period,
    'Academic year'                                                    AS time_identifier,
    'National'                                                         AS geographic_level,
    s.country_code,
    s.country_name,
    s.establishment_type_group,
    s.education_phase,
    s.school_religious_character,
    s.sex,

    COUNT(DISTINCT p.school_id)                                        AS establishment_count,
    COUNT(p.pupil_id)                                                  AS eligible_pupil_count,

    SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END)        AS completed_check_pupil_count,
    SUM(CASE WHEN r.completion_status != 'C'
             OR  r.completion_status IS NULL THEN 1 ELSE 0 END)        AS not_completed_check_pupil_count,
    SUM(CASE WHEN r.completion_status = 'A' THEN 1 ELSE 0 END)        AS absent_pupil_count,
    SUM(CASE WHEN r.completion_status = 'U' THEN 1 ELSE 0 END)        AS unable_to_participate_pupil_count,
    SUM(CASE WHEN r.completion_status = 'W' THEN 1 ELSE 0 END)        AS working_below_pupil_count,
    SUM(CASE WHEN r.completion_status = 'J' THEN 1 ELSE 0 END)        AS just_arrived_pupil_count,
    SUM(CASE WHEN r.completion_status = 'M' THEN 1 ELSE 0 END)        AS missing_reason_pupil_count,

    SUM(r.mtc_score)                                                   AS mtc_score_total,
    ROUND(
        SUM(r.mtc_score)
        / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0),
        1
    )                                                                  AS mtc_score_average,

    ROUND(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(p.pupil_id), 0) * 100, 0)                    AS completed_check_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status != 'C'
                   OR r.completion_status IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(p.pupil_id), 0) * 100, 0)                    AS not_completed_check_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'A' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(p.pupil_id), 0) * 100, 0)                    AS absent_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'U' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(p.pupil_id), 0) * 100, 0)                    AS unable_to_participate_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'W' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(p.pupil_id), 0) * 100, 0)                    AS working_below_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'J' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(p.pupil_id), 0) * 100, 0)                    AS just_arrived_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'M' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(p.pupil_id), 0) * 100, 0)                    AS missing_reason_pupil_percent

FROM      v_pupil   p
JOIN      v_results r  ON r.pupil_id = p.pupil_id
                      AND r.time_period = p.time_period
JOIN      v_school  s  ON s.school_id = p.school_id

GROUP BY
    r.time_period,
    s.country_code,
    s.country_name,
    s.establishment_type_group,
    s.education_phase,
    s.school_religious_character,
    s.sex

ORDER BY
    r.time_period,
    s.establishment_type_group,
    s.education_phase,
    s.sex;
