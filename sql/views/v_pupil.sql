-- =============================================================================
-- Silver view: v_pupil
-- =============================================================================
-- Renames raw column names to output-friendly names AND recodes all
-- DfE/NPD short-code category values into human-readable labels.
--
-- This view sits between the raw `pupil` table (Bronze) and the
-- aggregated output tables (Gold).
-- =============================================================================

CREATE OR REPLACE VIEW v_pupil AS
SELECT
    -- Keys / links
    pupil_id,
    school_id,
    acad_year_code                                                              AS time_period,
    'Academic year'                                                             AS time_identifier,

    -- Establishment type group  (raw: estab_type_group)
    CASE estab_type_group
        WHEN 'ALL_STATE'    THEN 'All state funded'
        WHEN 'ALL'          THEN 'All state funded'
        WHEN 'AC'           THEN 'Academy converter'
        WHEN 'ACADEMY_CONV' THEN 'Academy converter'
        WHEN 'AS'           THEN 'Academy sponsor led'
        WHEN 'ACADEMY_SPONS'THEN 'Academy sponsor led'
        WHEN 'AC_FS'        THEN 'Academies and free schools'
        WHEN 'FS'           THEN 'Free schools'
        WHEN 'FREE_SCHOOL'  THEN 'Free schools'
        WHEN 'LA'           THEN 'Local authority maintained'
        WHEN 'LA_MAINT'     THEN 'Local authority maintained'
        WHEN 'AP'           THEN 'Alternative provision'
        WHEN 'ALT_PROV'     THEN 'Alternative provision'
        WHEN 'SS'           THEN 'State funded special schools'
        WHEN 'SPEC'         THEN 'State funded special schools'
        WHEN 'MST'          THEN 'State funded mainstream schools'
        WHEN 'STATE_AP'     THEN 'State-funded schools and alternative provision'
        ELSE estab_type_group
    END                                                                         AS establishment_type_group,

    -- Geography (renamed)
    la_code_new                                                                 AS new_la_code,
    la_code_old                                                                 AS old_la_code,

    -- Sex / Gender  (raw: gender)
    CASE gender
        WHEN 'M'      THEN 'Boys'
        WHEN 'Male'   THEN 'Boys'
        WHEN '1'      THEN 'Boys'
        WHEN 'F'      THEN 'Girls'
        WHEN 'Female' THEN 'Girls'
        WHEN '2'      THEN 'Girls'
        WHEN 'T'      THEN 'Total'
        WHEN '0'      THEN 'Total'
        ELSE 'Total'
    END                                                                         AS sex,

    -- Ethnicity major group  (raw: ethnic_major_group)
    CASE ethnic_major_group
        WHEN 'WHITE'   THEN 'White'
        WHEN 'W'       THEN 'White'
        WHEN 'MIXED'   THEN 'Mixed / Multiple ethnic groups'
        WHEN 'MIX'     THEN 'Mixed / Multiple ethnic groups'
        WHEN 'ASIAN'   THEN 'Asian / Asian British'
        WHEN 'A'       THEN 'Asian / Asian British'
        WHEN 'BLACK'   THEN 'Black / African / Caribbean / Black British'
        WHEN 'B'       THEN 'Black / African / Caribbean / Black British'
        WHEN 'OTHER'   THEN 'Other ethnic group'
        WHEN 'O'       THEN 'Other ethnic group'
        WHEN 'UNKNOWN' THEN 'Unknown'
        WHEN 'UNK'     THEN 'Unknown'
        WHEN 'NOBT'    THEN 'Unknown'
        WHEN 'REFU'    THEN 'Unknown'
        WHEN 'T'       THEN 'Total'
        WHEN 'TOTAL'   THEN 'Total'
        ELSE ethnic_major_group
    END                                                                         AS ethnicity_major,

    -- Ethnicity minor group  (raw: ethnic_minor_group)
    CASE ethnic_minor_group
        -- White
        WHEN 'WBRI'  THEN 'English / Welsh / Scottish / Northern Irish / British'
        WHEN 'WIRI'  THEN 'Irish'
        WHEN 'WIRT'  THEN 'Irish Traveller'
        WHEN 'WROM'  THEN 'Gypsy'
        WHEN 'WOTW'  THEN 'Any other White background'
        WHEN 'WOTH'  THEN 'Any other White background'
        -- Mixed
        WHEN 'MWBC'  THEN 'White and Black Caribbean'
        WHEN 'MWBA'  THEN 'White and Black African'
        WHEN 'MWAS'  THEN 'White and Asian'
        WHEN 'MOTH'  THEN 'Any other Mixed / Multiple ethnic background'
        WHEN 'MFIL'  THEN 'Any other Mixed / Multiple ethnic background'
        -- Asian
        WHEN 'AIND'  THEN 'Indian'
        WHEN 'APKN'  THEN 'Pakistani'
        WHEN 'ABAN'  THEN 'Bangladeshi'
        WHEN 'ACHN'  THEN 'Chinese'
        WHEN 'AOTH'  THEN 'Any other Asian background'
        -- Black
        WHEN 'BCRB'  THEN 'Caribbean'
        WHEN 'BAFR'  THEN 'African'
        WHEN 'BOTH'  THEN 'Any other Black / African / Caribbean background'
        -- Other
        WHEN 'OOEG'  THEN 'Any other ethnic group'
        -- Unknown
        WHEN 'NOBT'  THEN 'Unknown'
        WHEN 'REFU'  THEN 'Unknown'
        WHEN 'UNK'   THEN 'Unknown'
        -- Aggregate rows
        WHEN 'ALL_W' THEN 'All White'
        WHEN 'ALL_M' THEN 'All Mixed / Multiple ethnic groups'
        WHEN 'ALL_A' THEN 'All Asian / Asian British'
        WHEN 'ALL_B' THEN 'All Black / African / Caribbean / Black British'
        WHEN 'T'     THEN 'Total'
        WHEN 'TOTAL' THEN 'Total'
        ELSE ethnic_minor_group
    END                                                                         AS ethnicity_minor,

    -- First language  (raw: first_lang)
    CASE first_lang
        WHEN 'ENG'   THEN 'Known or believed to be English'
        WHEN 'E'     THEN 'Known or believed to be English'
        WHEN 'OTH'   THEN 'Known or believed to be other than English'
        WHEN 'O'     THEN 'Known or believed to be other than English'
        WHEN 'UNK'   THEN 'First language unknown'
        WHEN 'U'     THEN 'First language unknown'
        WHEN 'T'     THEN 'Total'
        WHEN 'TOTAL' THEN 'Total'
        ELSE first_lang
    END                                                                         AS first_language,

    -- Month of birth  (raw: birth_month_code)
    CASE birth_month_code
        WHEN '1'  THEN 'January'
        WHEN '01' THEN 'January'
        WHEN '2'  THEN 'February'
        WHEN '02' THEN 'February'
        WHEN '3'  THEN 'March'
        WHEN '03' THEN 'March'
        WHEN '4'  THEN 'April'
        WHEN '04' THEN 'April'
        WHEN '5'  THEN 'May'
        WHEN '05' THEN 'May'
        WHEN '6'  THEN 'June'
        WHEN '06' THEN 'June'
        WHEN '7'  THEN 'July'
        WHEN '07' THEN 'July'
        WHEN '8'  THEN 'August'
        WHEN '08' THEN 'August'
        WHEN '9'  THEN 'September'
        WHEN '09' THEN 'September'
        WHEN '10' THEN 'October'
        WHEN '11' THEN 'November'
        WHEN '12' THEN 'December'
        WHEN 'T'     THEN 'Total'
        WHEN 'TOTAL' THEN 'Total'
        ELSE birth_month_code
    END                                                                         AS month_of_birth,

    -- SEN provision  (raw: sen_prov_code)
    CASE sen_prov_code
        WHEN 'E'          THEN 'Education, health and care plan'
        WHEN 'EHC'        THEN 'Education, health and care plan'
        WHEN 'K'          THEN 'SEN support / SEN without an EHC plan'
        WHEN 'SEN_SUPPORT'THEN 'SEN support / SEN without an EHC plan'
        WHEN 'N'          THEN 'No SEN provision'
        WHEN 'NONE'       THEN 'No SEN provision'
        WHEN 'ALL_SEN'    THEN 'All SEN provision'
        WHEN 'A'          THEN 'All SEN provision'
        WHEN 'UNK'        THEN 'Unknown SEN provision'
        WHEN 'U'          THEN 'Unknown SEN provision'
        WHEN 'T'          THEN 'Total'
        WHEN 'TOTAL'      THEN 'Total'
        ELSE sen_prov_code
    END                                                                         AS sen_provision,

    -- SEN primary need  (raw: sen_type_rank)
    CASE sen_type_rank
        WHEN 'AUT'  THEN 'Autistic spectrum disorder'
        WHEN 'SEMH' THEN 'Social, emotional and mental health'
        WHEN 'BESD' THEN 'Social, emotional and mental health'
        WHEN 'HI'   THEN 'Hearing impairment'
        WHEN 'MLD'  THEN 'Moderate learning difficulty'
        WHEN 'MSI'  THEN 'Multi-sensory impairment'
        WHEN 'OTH'  THEN 'Other difficulty or disability'
        WHEN 'PD'   THEN 'Physical disability'
        WHEN 'PMLD' THEN 'Profound and multiple learning difficulty'
        WHEN 'SLCN' THEN 'Speech, language and communication needs'
        WHEN 'SLD'  THEN 'Severe learning difficulty'
        WHEN 'SPLD' THEN 'Specific learning difficulty'
        WHEN 'VI'   THEN 'Vision impairment'
        WHEN 'DYNA' THEN 'Down syndrome'
        WHEN 'NSA'  THEN 'SEN support but no specialist assessment of type of need'
        WHEN 'T'     THEN 'Total'
        WHEN 'TOTAL' THEN 'Total'
        ELSE sen_type_rank
    END                                                                         AS sen_primary_need

FROM pupil;
