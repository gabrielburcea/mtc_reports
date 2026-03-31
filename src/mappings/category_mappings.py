"""
Category-value mappings from raw codes stored in the Databricks Unity Catalog
tables to the human-readable labels used in the CSV output reports.

Each top-level key corresponds to an output column name; its value is a dict
that maps raw code → human-readable label.

Sources
-------
* DfE / NPD data standard ethnicity codes
* DfE SEN provision and primary-need codes
* DfE school phase / type / religious-character codes
* MTC results and claimcare data specifications
"""

# ---------------------------------------------------------------------------
# Sex / Gender
# ---------------------------------------------------------------------------
SEX: dict[str, str] = {
    "1": "Boys",
    "2": "Girls",
    "M": "Boys",
    "F": "Girls",
    "Male": "Boys",
    "Female": "Girls",
    "0": "Total",
    "T": "Total",
    "Total": "Total",
}

# ---------------------------------------------------------------------------
# Ethnicity – major group
# (raw codes produced by grouping NPD ethnic-minor codes)
# ---------------------------------------------------------------------------
ETHNICITY_MAJOR: dict[str, str] = {
    "WHITE": "White",
    "W": "White",
    "MIXED": "Mixed / Multiple ethnic groups",
    "MIX": "Mixed / Multiple ethnic groups",
    "ASIAN": "Asian / Asian British",
    "A": "Asian / Asian British",
    "BLACK": "Black / African / Caribbean / Black British",
    "B": "Black / African / Caribbean / Black British",
    "OTHER": "Other ethnic group",
    "O": "Other ethnic group",
    "UNKNOWN": "Unknown",
    "UNK": "Unknown",
    "NOBT": "Unknown",
    "REFU": "Unknown",
    "TOTAL": "Total",
    "T": "Total",
}

# ---------------------------------------------------------------------------
# Ethnicity – minor group  (DfE NPD ethnic codes)
# ---------------------------------------------------------------------------
ETHNICITY_MINOR: dict[str, str] = {
    # White
    "WBRI": "English / Welsh / Scottish / Northern Irish / British",
    "WIRI": "Irish",
    "WIRT": "Irish Traveller",
    "WROM": "Gypsy",
    "WOTW": "Any other White background",
    "WOTH": "Any other White background",
    # Mixed / Multiple
    "MWBC": "White and Black Caribbean",
    "MWBA": "White and Black African",
    "MWAS": "White and Asian",
    "MOTH": "Any other Mixed / Multiple ethnic background",
    "MFIL": "Any other Mixed / Multiple ethnic background",
    # Asian / Asian British
    "AIND": "Indian",
    "APKN": "Pakistani",
    "ABAN": "Bangladeshi",
    "ACHN": "Chinese",
    "AOTH": "Any other Asian background",
    "AAFR": "Any other Asian background",
    # Black / African / Caribbean / Black British
    "BCRB": "Caribbean",
    "BAFR": "African",
    "BOTH": "Any other Black / African / Caribbean background",
    # Other ethnic group
    "OOEG": "Any other ethnic group",
    "CHNE": "Chinese",
    # Aggregate / unknown
    "NOBT": "Unknown",
    "REFU": "Unknown",
    "UNK": "Unknown",
    # All-group aggregates (used in roll-up rows)
    "ALL_W": "All White",
    "ALL_M": "All Mixed / Multiple ethnic groups",
    "ALL_A": "All Asian / Asian British",
    "ALL_B": "All Black / African / Caribbean / Black British",
    "TOTAL": "Total",
    "T": "Total",
}

# ---------------------------------------------------------------------------
# First language
# ---------------------------------------------------------------------------
FIRST_LANGUAGE: dict[str, str] = {
    "ENG": "Known or believed to be English",
    "E": "Known or believed to be English",
    "OTH": "Known or believed to be other than English",
    "O": "Known or believed to be other than English",
    "UNK": "First language unknown",
    "U": "First language unknown",
    "TOTAL": "Total",
    "T": "Total",
}

# ---------------------------------------------------------------------------
# FSM (Free School Meals) eligibility status
# ---------------------------------------------------------------------------
FSM_STATUS: dict[str, str] = {
    "1": "FSM eligible",
    "Y": "FSM eligible",
    "True": "FSM eligible",
    "0": "Not known to be FSM eligible",
    "N": "Not known to be FSM eligible",
    "False": "Not known to be FSM eligible",
    "TOTAL": "Total",
    "T": "Total",
}

# ---------------------------------------------------------------------------
# Disadvantage status  (derived from claimcare / NPD data)
# ---------------------------------------------------------------------------
DISADVANTAGE_STATUS: dict[str, str] = {
    "1": "Disadvantaged",
    "Y": "Disadvantaged",
    "True": "Disadvantaged",
    "Disadvantaged": "Disadvantaged",
    "0": "Not known to be disadvantaged",
    "N": "Not known to be disadvantaged",
    "False": "Not known to be disadvantaged",
    "TOTAL": "Total",
    "T": "Total",
}

# ---------------------------------------------------------------------------
# Month of birth
# ---------------------------------------------------------------------------
MONTH_OF_BIRTH: dict[str, str] = {
    "1": "January",
    "2": "February",
    "3": "March",
    "4": "April",
    "5": "May",
    "6": "June",
    "7": "July",
    "8": "August",
    "9": "September",
    "10": "October",
    "11": "November",
    "12": "December",
    "01": "January",
    "02": "February",
    "03": "March",
    "04": "April",
    "05": "May",
    "06": "June",
    "07": "July",
    "08": "August",
    "09": "September",
    "TOTAL": "Total",
    "T": "Total",
}

# ---------------------------------------------------------------------------
# SEN provision type
# ---------------------------------------------------------------------------
SEN_PROVISION: dict[str, str] = {
    "E": "Education, health and care plan",
    "EHC": "Education, health and care plan",
    "K": "SEN support / SEN without an EHC plan",
    "SEN_SUPPORT": "SEN support / SEN without an EHC plan",
    "N": "No SEN provision",
    "NONE": "No SEN provision",
    "ALL_SEN": "All SEN provision",
    "A": "All SEN provision",
    "UNK": "Unknown SEN provision",
    "U": "Unknown SEN provision",
    "TOTAL": "Total",
    "T": "Total",
}

# ---------------------------------------------------------------------------
# SEN primary need  (DfE SEN type codes)
# ---------------------------------------------------------------------------
SEN_PRIMARY_NEED: dict[str, str] = {
    "AUT": "Autistic spectrum disorder",
    "SEMH": "Social, emotional and mental health",
    "BESD": "Social, emotional and mental health",
    "HI": "Hearing impairment",
    "MLD": "Moderate learning difficulty",
    "MSI": "Multi-sensory impairment",
    "OTH": "Other difficulty or disability",
    "PD": "Physical disability",
    "PMLD": "Profound and multiple learning difficulty",
    "SLCN": "Speech, language and communication needs",
    "SLD": "Severe learning difficulty",
    "SPLD": "Specific learning difficulty",
    "VI": "Vision impairment",
    "DYNA": "Down syndrome",
    "NSA": "SEN support but no specialist assessment of type of need",
    "TOTAL": "Total",
    "T": "Total",
}

# ---------------------------------------------------------------------------
# Establishment type group
# ---------------------------------------------------------------------------
ESTABLISHMENT_TYPE_GROUP: dict[str, str] = {
    "AC": "Academy converter",
    "ACADEMY_CONV": "Academy converter",
    "AS": "Academy sponsor led",
    "ACADEMY_SPONS": "Academy sponsor led",
    "FS": "Free schools",
    "FREE_SCHOOL": "Free schools",
    "AC_FS": "Academies and free schools",
    "ACADEMIES_FREE": "Academies and free schools",
    "LA": "Local authority maintained",
    "LA_MAINT": "Local authority maintained",
    "AP": "Alternative provision",
    "ALT_PROV": "Alternative provision",
    "SS": "State funded special schools",
    "SPEC": "State funded special schools",
    "MST": "State funded mainstream schools",
    "MAINSTREAM": "State funded mainstream schools",
    "ALL_STATE": "All state funded",
    "ALL": "All state funded",
    "STATE_AP": "State-funded schools and alternative provision",
    "STATE_AND_AP": "State-funded schools and alternative provision",
}

# ---------------------------------------------------------------------------
# Education phase (school-level)
# ---------------------------------------------------------------------------
EDUCATION_PHASE: dict[str, str] = {
    "PRI": "Highest statutory age 7",
    "PRIMARY": "Highest statutory age 7",
    "JUN": "Highest statutory age 8 to 11",
    "JUNIOR": "Highest statutory age 8 to 11",
    "MID": "Highest statutory age 8 to 11",
    "SEC": "Highest statutory age greater than 11",
    "SECONDARY": "Highest statutory age greater than 11",
    "ALL_THROUGH": "Highest statutory age greater than 11",
    "TOTAL": "Total",
    "T": "Total",
}

# ---------------------------------------------------------------------------
# School religious character
# ---------------------------------------------------------------------------
SCHOOL_RELIGIOUS_CHARACTER: dict[str, str] = {
    "CE": "Church of England",
    "COE": "Church of England",
    "RC": "Roman Catholic",
    "ROM_CATH": "Roman Catholic",
    "JEW": "Jewish",
    "JEWISH": "Jewish",
    "METH": "Methodist",
    "METHODIST": "Methodist",
    "MUS": "Muslim",
    "MUSLIM": "Muslim",
    "SIKH": "Sikh",
    "OTH_XIAN": "Other Christian faith",
    "OTHER_CHRISTIAN": "Other Christian faith",
    "OTH_REL": "Other religious character",
    "OTHER_RELIGIOUS": "Other religious character",
    "NONE": "No religious character",
    "NO_RELI": "No religious character",
    "TOTAL": "Total",
    "T": "Total",
}

# ---------------------------------------------------------------------------
# Geographic level
# ---------------------------------------------------------------------------
GEOGRAPHIC_LEVEL: dict[str, str] = {
    "NAT": "National",
    "NATIONAL": "National",
    "REG": "Regional",
    "REGIONAL": "Regional",
    "LA": "Local authority",
    "LOCAL_AUTH": "Local authority",
    "LOCAL_AUTHORITY": "Local authority",
}

# ---------------------------------------------------------------------------
# Convenience master mapping: output column name → lookup dict
# ---------------------------------------------------------------------------
ALL_CATEGORY_MAPPINGS: dict[str, dict[str, str]] = {
    "sex": SEX,
    "ethnicity_major": ETHNICITY_MAJOR,
    "ethnicity_minor": ETHNICITY_MINOR,
    "first_language": FIRST_LANGUAGE,
    "fsm_status": FSM_STATUS,
    "disadvantage_status": DISADVANTAGE_STATUS,
    "month_of_birth": MONTH_OF_BIRTH,
    "sen_provision": SEN_PROVISION,
    "sen_primary_need": SEN_PRIMARY_NEED,
    "establishment_type_group": ESTABLISHMENT_TYPE_GROUP,
    "education_phase": EDUCATION_PHASE,
    "school_religious_character": SCHOOL_RELIGIOUS_CHARACTER,
    "geographic_level": GEOGRAPHIC_LEVEL,
}
