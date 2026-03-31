-- =============================================================================
-- Gold output: mtc_regional_la_pupil_characteristics
-- =============================================================================
-- Aggregated regional and local-authority pupil-characteristics report.
-- =============================================================================

SELECT
    p.time_period,
    p.time_identifier,
    g.geographic_level,
    g.country_code,
    g.country_name,
    g.region_code,
    g.region_name,
    g.old_la_code,
    g.new_la_code,
    g.la_name,
    p.establishment_type_group,
    p.sex,
    c.disadvantage_status,
    p.ethnicity_major,
    p.ethnicity_minor,
    p.first_language,
    c.fsm_status,
    p.sen_provision,

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

FROM      v_pupil     p
JOIN      v_results   r  ON r.pupil_id = p.pupil_id
                        AND r.time_period = p.time_period
JOIN      v_claimcare c  ON c.pupil_id = p.pupil_id
                        AND c.time_period = p.time_period
JOIN      v_geography g  ON g.new_la_code = p.new_la_code

WHERE g.geographic_level IN ('Regional', 'Local authority')

GROUP BY
    p.time_period,
    p.time_identifier,
    g.geographic_level,
    g.country_code,
    g.country_name,
    g.region_code,
    g.region_name,
    g.old_la_code,
    g.new_la_code,
    g.la_name,
    p.establishment_type_group,
    p.sex,
    c.disadvantage_status,
    p.ethnicity_major,
    p.ethnicity_minor,
    p.first_language,
    c.fsm_status,
    p.sen_provision

ORDER BY
    p.time_period,
    g.region_name,
    g.la_name,
    p.sex;
