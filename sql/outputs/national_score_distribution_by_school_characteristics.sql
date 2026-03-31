-- =============================================================================
-- Gold output: mtc_national_score_distribution_by_school_characteristics
-- =============================================================================
-- Wide-format score distribution by school characteristics.
-- One column per score value 0–25 (count + percent of completed checks).
-- =============================================================================

SELECT
    r.time_period,
    'Academic year'                                                         AS time_identifier,
    'National'                                                              AS geographic_level,
    s.country_code,
    s.country_name,
    s.establishment_type_group,
    s.education_phase,
    s.school_religious_character,
    s.sex,

    -- Counts
    COUNT(DISTINCT p.school_id)                                             AS establishment_count,
    COUNT(p.pupil_id)                                                       AS eligible_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END)             AS completed_check_pupil_count,
    SUM(CASE WHEN r.completion_status != 'C'
             OR  r.completion_status IS NULL THEN 1 ELSE 0 END)             AS not_completed_check_pupil_count,

    -- Score counts (25 → 0)
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 25 THEN 1 ELSE 0 END) AS mtc_score_25_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 24 THEN 1 ELSE 0 END) AS mtc_score_24_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 23 THEN 1 ELSE 0 END) AS mtc_score_23_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 22 THEN 1 ELSE 0 END) AS mtc_score_22_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 21 THEN 1 ELSE 0 END) AS mtc_score_21_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 20 THEN 1 ELSE 0 END) AS mtc_score_20_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 19 THEN 1 ELSE 0 END) AS mtc_score_19_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 18 THEN 1 ELSE 0 END) AS mtc_score_18_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 17 THEN 1 ELSE 0 END) AS mtc_score_17_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 16 THEN 1 ELSE 0 END) AS mtc_score_16_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 15 THEN 1 ELSE 0 END) AS mtc_score_15_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 14 THEN 1 ELSE 0 END) AS mtc_score_14_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 13 THEN 1 ELSE 0 END) AS mtc_score_13_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 12 THEN 1 ELSE 0 END) AS mtc_score_12_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 11 THEN 1 ELSE 0 END) AS mtc_score_11_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 10 THEN 1 ELSE 0 END) AS mtc_score_10_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  9 THEN 1 ELSE 0 END) AS mtc_score_9_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  8 THEN 1 ELSE 0 END) AS mtc_score_8_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  7 THEN 1 ELSE 0 END) AS mtc_score_7_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  6 THEN 1 ELSE 0 END) AS mtc_score_6_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  5 THEN 1 ELSE 0 END) AS mtc_score_5_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  4 THEN 1 ELSE 0 END) AS mtc_score_4_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  3 THEN 1 ELSE 0 END) AS mtc_score_3_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  2 THEN 1 ELSE 0 END) AS mtc_score_2_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  1 THEN 1 ELSE 0 END) AS mtc_score_1_pupil_count,
    SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  0 THEN 1 ELSE 0 END) AS mtc_score_0_pupil_count,

    -- Summary
    SUM(r.mtc_score)                                                        AS mtc_score_total,
    ROUND(SUM(r.mtc_score)
          / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0),
          1)                                                                 AS mtc_score_average,

    -- Completion percentages
    ROUND(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(p.pupil_id), 0) * 100, 0)                         AS completed_check_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status != 'C'
                   OR r.completion_status IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(p.pupil_id), 0) * 100, 0)                         AS not_completed_check_pupil_percent,

    -- Score percentages (of completed checks)
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 25 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_25_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 24 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_24_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 23 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_23_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 22 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_22_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 21 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_21_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 20 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_20_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 19 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_19_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 18 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_18_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 17 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_17_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 16 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_16_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 15 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_15_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 14 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_14_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 13 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_13_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 12 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_12_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 11 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_11_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score = 10 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_10_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  9 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_9_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  8 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_8_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  7 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_7_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  6 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_6_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  5 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_5_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  4 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_4_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  3 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_3_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  2 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_2_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  1 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_1_pupil_percent,
    ROUND(SUM(CASE WHEN r.completion_status = 'C' AND r.mtc_score =  0 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN r.completion_status = 'C' THEN 1 ELSE 0 END), 0) * 100, 0) AS mtc_score_0_pupil_percent

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
