-- =============================================================================
-- Gold output: mtc_national_cumulative_score_distribution
-- =============================================================================
-- For each (sex, establishment_type_group, score) shows the count of pupils
-- who achieved exactly that score AND the cumulative count / percent for pupils
-- who achieved AT LEAST that score (reading from the top: score 25 → 0).
-- =============================================================================

WITH score_counts AS (
    SELECT
        p.time_period,
        'Academic year'                                                AS time_identifier,
        'National'                                                     AS geographic_level,
        g.country_code,
        g.country_name,
        p.establishment_type_group,
        p.sex,
        r.mtc_score,
        COUNT(p.pupil_id)                                              AS pupil_count
    FROM      v_pupil     p
    JOIN      v_results   r  ON r.pupil_id    = p.pupil_id
                            AND r.time_period  = p.time_period
    JOIN      v_geography g  ON g.new_la_code  = p.new_la_code
    WHERE r.completion_status = 'C'             -- completed checks only
      AND g.geographic_level  = 'National'
    GROUP BY
        p.time_period,
        g.country_code,
        g.country_name,
        p.establishment_type_group,
        p.sex,
        r.mtc_score
),

totals AS (
    SELECT
        time_period,
        establishment_type_group,
        sex,
        country_code,
        country_name,
        SUM(pupil_count) AS total_completed
    FROM score_counts
    GROUP BY time_period, establishment_type_group, sex, country_code, country_name
)

SELECT
    sc.time_period,
    sc.time_identifier,
    sc.geographic_level,
    sc.country_code,
    sc.country_name,
    sc.establishment_type_group,
    sc.sex,
    sc.mtc_score,
    sc.pupil_count,

    -- Cumulative count: pupils who achieved this score OR higher
    SUM(sc.pupil_count) OVER (
        PARTITION BY sc.time_period,
                     sc.establishment_type_group,
                     sc.sex,
                     sc.country_code,
                     sc.country_name
        ORDER BY sc.mtc_score DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                                  AS cumulative_pupil_count,

    -- Cumulative percent
    ROUND(
        SUM(sc.pupil_count) OVER (
            PARTITION BY sc.time_period,
                         sc.establishment_type_group,
                         sc.sex,
                         sc.country_code,
                         sc.country_name
            ORDER BY sc.mtc_score DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / NULLIF(t.total_completed, 0) * 100,
        0
    )                                                                  AS cumulative_pupil_percent

FROM  score_counts sc
JOIN  totals       t  ON t.time_period            = sc.time_period
                     AND t.establishment_type_group= sc.establishment_type_group
                     AND t.sex                    = sc.sex
                     AND t.country_code           = sc.country_code
                     AND t.country_name           = sc.country_name

ORDER BY
    sc.time_period,
    sc.establishment_type_group,
    sc.sex,
    sc.mtc_score DESC;
