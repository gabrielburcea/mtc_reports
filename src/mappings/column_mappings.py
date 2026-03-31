"""
Column name mappings from raw Databricks Unity Catalog table fields
to the required output column names used in the CSV reports.

Each key is a source table identifier; its value is a dict that maps
raw column name → output column name.  Only columns that need renaming
are listed – columns whose names are already correct in the raw data can
be omitted.
"""

# ---------------------------------------------------------------------------
# Pupil-level census / characteristics table
# (typically the NPD census extract held in Unity Catalog)
# ---------------------------------------------------------------------------
PUPIL_TABLE_COLUMNS: dict[str, str] = {
    # identification / geography
    "la_code_old": "old_la_code",
    "la_code_new": "new_la_code",
    "local_authority_name": "la_name",
    "gor_code": "region_code",
    "gor_name": "region_name",
    "nat_code": "country_code",
    "nat_name": "country_name",

    # demographics
    "gender": "sex",
    "ethnic_major_group": "ethnicity_major",
    "ethnic_minor_group": "ethnicity_minor",
    "first_lang": "first_language",

    # disadvantage / FSM
    "disadvantaged_flag": "disadvantage_status",
    "fsm_eligible_flag": "fsm_status",

    # birth month
    "birth_month_code": "month_of_birth",

    # SEN
    "sen_prov_code": "sen_provision",
    "sen_type_rank": "sen_primary_need",

    # establishment grouping
    "estab_type_group": "establishment_type_group",
}

# ---------------------------------------------------------------------------
# School / establishment characteristics table
# ---------------------------------------------------------------------------
SCHOOL_TABLE_COLUMNS: dict[str, str] = {
    "estab_type_group": "establishment_type_group",
    "phase_of_education": "education_phase",
    "religious_character": "school_religious_character",
    "gender": "sex",
}

# ---------------------------------------------------------------------------
# MTC results / marks table
# ---------------------------------------------------------------------------
RESULTS_TABLE_COLUMNS: dict[str, str] = {
    "pupil_mark": "mtc_score",
    "total_mark": "mtc_score_total",
    "average_mark": "mtc_score_average",
}

# ---------------------------------------------------------------------------
# Claimcare / FSM eligibility table
# (DfE claimcare data – records FSM & disadvantaged status)
# ---------------------------------------------------------------------------
CLAIMCARE_TABLE_COLUMNS: dict[str, str] = {
    "claim_type": "disadvantage_status",
    "fsm_claim_flag": "fsm_status",
}

# ---------------------------------------------------------------------------
# Geography lookup table
# ---------------------------------------------------------------------------
GEOGRAPHY_TABLE_COLUMNS: dict[str, str] = {
    "gor_code": "region_code",
    "gor_name": "region_name",
    "la_old": "old_la_code",
    "la_new": "new_la_code",
    "la_name_full": "la_name",
    "geographic_lvl": "geographic_level",
    "country_cd": "country_code",
    "country_nm": "country_name",
}

# ---------------------------------------------------------------------------
# Time / academic year dimension
# ---------------------------------------------------------------------------
TIME_TABLE_COLUMNS: dict[str, str] = {
    "acad_year_code": "time_period",
    "acad_year_label": "time_identifier",
}
