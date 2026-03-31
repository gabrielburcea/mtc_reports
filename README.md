# MTC Reports – Variable Mapping & Transformation Pipeline

This repository contains the ETL pipeline that reads raw MTC (Multiplication
Tables Check) data from a **Databricks Unity Catalog**, maps raw database column
names and category codes to human-readable labels, and produces the seven CSV
report files whose format is shown in the `data/` directory.

---

## Problem

The raw Databricks Unity Catalog tables contain:

* **Column names** that differ from the output CSV headers  
  (e.g. `gender` → `sex`, `ethnic_minor_group` → `ethnicity_minor`,
  `sen_prov_code` → `sen_provision`).
* **Category values stored as short codes** that must be decoded into
  human-readable labels  
  (e.g. ethnicity code `WBRI` →
  `"English / Welsh / Scottish / Northern Irish / British"`,
  SEN code `E` → `"Education, health and care plan"`).

The CSV files in `data/` show the **desired output format**.

---

## Source Tables (Unity Catalog)

| Table | Contents |
|---|---|
| `pupil` | NPD census extract – one row per pupil, containing demographics (gender, ethnicity, language, SEN, birth month) |
| `results` | MTC check results – pupil score (0–25), completion status |
| `claimcare` | DfE claimcare data – FSM eligibility and disadvantage flags |
| `school` | Establishment characteristics – type group, education phase, religious character |
| `geography` | Geographic hierarchy – LA → Region → National lookup |

---

## Output Files

| File | Description |
|---|---|
| `mtc_national_pupil_characteristics_YYYY_to_YYYY.csv` | National breakdowns by pupil characteristics (sex, ethnicity, language, FSM, SEN, birth month, disadvantage) |
| `mtc_regional_la_pupil_characteristics_YYYY_to_YYYY.csv` | Regional & LA breakdowns by pupil characteristics |
| `mtc_national_school_characteristics_YYYY_to_YYYY.csv` | National breakdowns by school characteristics (type, phase, religious character) |
| `mtc_national_cumulative_score_distribution_YYYY_to_YYYY.csv` | Cumulative score distribution at national level |
| `mtc_national_score_distribution_by_school_characteristics_YYYY_to_YYYY.csv` | Wide-format score distribution (one column per score 0–25) by school characteristics |
| `mtc_national_score_distribution_by_pupil_characteristics_YYYY_to_YYYY.csv` | Wide-format score distribution by pupil characteristics |
| `mtc_regional_la_score_distribution_by_pupil_characteristics_YYYY_to_YYYY.csv` | Regional/LA wide-format score distribution by pupil characteristics |

---

## Repository Structure

```
src/
├── mappings/
│   ├── column_mappings.py    # Raw DB column name  → output column name
│   └── category_mappings.py  # Raw category codes  → human-readable labels
├── transforms/
│   ├── base.py               # Shared helper methods (rename, recode, aggregate)
│   ├── pupil_characteristics.py   # Builds national & regional/LA pupil outputs
│   ├── school_characteristics.py  # Builds national school output
│   └── score_distributions.py     # Builds all four score-distribution outputs
└── pipeline.py               # Orchestrates the full run; CLI entry-point
data/
└── *.csv                     # Reference output files (desired format)
```

---

## Mappings

### Column mappings (`src/mappings/column_mappings.py`)

Each dictionary maps a **raw table column name** to its **output column name**:

```python
PUPIL_TABLE_COLUMNS = {
    "gender":             "sex",
    "ethnic_minor_group": "ethnicity_minor",
    "first_lang":         "first_language",
    "sen_prov_code":      "sen_provision",
    "sen_type_rank":      "sen_primary_need",
    ...
}
```

### Category mappings (`src/mappings/category_mappings.py`)

Each dictionary maps raw **category codes** to **human-readable labels**:

| Column | Example code → label |
|---|---|
| `sex` | `"M"` → `"Boys"` |
| `ethnicity_minor` | `"WBRI"` → `"English / Welsh / Scottish / Northern Irish / British"` |
| `ethnicity_minor` | `"MWBC"` → `"White and Black Caribbean"` |
| `first_language` | `"ENG"` → `"Known or believed to be English"` |
| `fsm_status` | `"1"` / `"Y"` → `"FSM eligible"` |
| `disadvantage_status` | `"1"` / `"Y"` → `"Disadvantaged"` |
| `sen_provision` | `"E"` → `"Education, health and care plan"` |
| `sen_provision` | `"K"` → `"SEN support / SEN without an EHC plan"` |
| `sen_primary_need` | `"AUT"` → `"Autistic spectrum disorder"` |
| `sen_primary_need` | `"SEMH"` → `"Social, emotional and mental health"` |
| `month_of_birth` | `"9"` → `"September"` |
| `establishment_type_group` | `"AC"` → `"Academy converter"` |
| `education_phase` | `"PRI"` → `"Highest statutory age 7"` |
| `school_religious_character` | `"CE"` → `"Church of England"` |

---

## How to Run

### In a Databricks Notebook

```python
# Install the package (run once)
# %pip install -e /path/to/mtc_reports

from src.pipeline import MtcReportsPipeline

pipeline = MtcReportsPipeline(
    spark=spark,                          # Databricks SparkSession (auto-available)
    catalog="your_catalog",               # Unity Catalog name
    schema="your_schema",                 # Schema / database name
    output_path="/dbfs/mnt/reports/mtc",  # Output directory
    time_periods=["202122", "202223", "202324", "202425"],
)
pipeline.run()
```

### Via `spark-submit`

```bash
spark-submit src/pipeline.py \
    --catalog your_catalog \
    --schema  your_schema  \
    --output  /dbfs/mnt/reports/mtc \
    --years   202122 202223 202324 202425
```

---

## Extending the Mappings

**To add a new raw column name mapping**, open
`src/mappings/column_mappings.py` and add an entry to the relevant
table dictionary:

```python
PUPIL_TABLE_COLUMNS = {
    ...
    "new_raw_column": "output_column_name",
}
```

**To add a new category code**, open
`src/mappings/category_mappings.py` and add an entry to the relevant
mapping dictionary:

```python
ETHNICITY_MINOR = {
    ...
    "NEWCODE": "Human-readable label",
}
```

The `ALL_CATEGORY_MAPPINGS` dict at the bottom of that file automatically
picks up any additions.

---

## Requirements

* Databricks Runtime with PySpark ≥ 3.3
* Unity Catalog enabled on the Databricks workspace
* Read permissions on the source tables listed above
