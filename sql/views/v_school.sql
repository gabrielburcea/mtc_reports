-- =============================================================================
-- Silver view: v_school
-- =============================================================================
-- Renames raw column names and decodes DfE short-code category values for the
-- school/establishment characteristics table.
-- =============================================================================

CREATE OR REPLACE VIEW v_school AS
SELECT
    school_id,
    la_code_new                                                                 AS new_la_code,
    la_code_old                                                                 AS old_la_code,
    country_code,
    country_name,

    -- Establishment type group  (raw: estab_type_group)
    CASE estab_type_group
        WHEN 'ALL_STATE'     THEN 'All state funded'
        WHEN 'ALL'           THEN 'All state funded'
        WHEN 'AC'            THEN 'Academy converter'
        WHEN 'ACADEMY_CONV'  THEN 'Academy converter'
        WHEN 'AS'            THEN 'Academy sponsor led'
        WHEN 'ACADEMY_SPONS' THEN 'Academy sponsor led'
        WHEN 'AC_FS'         THEN 'Academies and free schools'
        WHEN 'FS'            THEN 'Free schools'
        WHEN 'FREE_SCHOOL'   THEN 'Free schools'
        WHEN 'LA'            THEN 'Local authority maintained'
        WHEN 'LA_MAINT'      THEN 'Local authority maintained'
        WHEN 'AP'            THEN 'Alternative provision'
        WHEN 'ALT_PROV'      THEN 'Alternative provision'
        WHEN 'SS'            THEN 'State funded special schools'
        WHEN 'SPEC'          THEN 'State funded special schools'
        WHEN 'MST'           THEN 'State funded mainstream schools'
        WHEN 'STATE_AP'      THEN 'State-funded schools and alternative provision'
        ELSE estab_type_group
    END                                                                         AS establishment_type_group,

    -- Education phase  (raw: phase_of_education)
    CASE phase_of_education
        WHEN 'PRI'       THEN 'Highest statutory age 7'
        WHEN 'PRIMARY'   THEN 'Highest statutory age 7'
        WHEN 'JUN'       THEN 'Highest statutory age 8 to 11'
        WHEN 'JUNIOR'    THEN 'Highest statutory age 8 to 11'
        WHEN 'MID'       THEN 'Highest statutory age 8 to 11'
        WHEN 'SEC'       THEN 'Highest statutory age greater than 11'
        WHEN 'SECONDARY' THEN 'Highest statutory age greater than 11'
        WHEN 'ALL_THROUGH' THEN 'Highest statutory age greater than 11'
        WHEN 'T'         THEN 'Total'
        WHEN 'TOTAL'     THEN 'Total'
        ELSE phase_of_education
    END                                                                         AS education_phase,

    -- School religious character  (raw: religious_character)
    CASE religious_character
        WHEN 'CE'               THEN 'Church of England'
        WHEN 'COE'              THEN 'Church of England'
        WHEN 'RC'               THEN 'Roman Catholic'
        WHEN 'ROM_CATH'         THEN 'Roman Catholic'
        WHEN 'JEW'              THEN 'Jewish'
        WHEN 'JEWISH'           THEN 'Jewish'
        WHEN 'METH'             THEN 'Methodist'
        WHEN 'METHODIST'        THEN 'Methodist'
        WHEN 'MUS'              THEN 'Muslim'
        WHEN 'MUSLIM'           THEN 'Muslim'
        WHEN 'SIKH'             THEN 'Sikh'
        WHEN 'OTH_XIAN'         THEN 'Other Christian faith'
        WHEN 'OTHER_CHRISTIAN'  THEN 'Other Christian faith'
        WHEN 'OTH_REL'          THEN 'Other religious character'
        WHEN 'OTHER_RELIGIOUS'  THEN 'Other religious character'
        WHEN 'NONE'             THEN 'No religious character'
        WHEN 'NO_RELI'          THEN 'No religious character'
        WHEN 'T'                THEN 'Total'
        WHEN 'TOTAL'            THEN 'Total'
        ELSE religious_character
    END                                                                         AS school_religious_character,

    -- Sex  (raw: gender)
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
    END                                                                         AS sex

FROM school;
