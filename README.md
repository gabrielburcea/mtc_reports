# MTC Reports – Variable Mapping & Transformation Pipeline

This repository implements the full solution for producing the seven MTC
(Multiplication Tables Check) statistical CSV reports from raw data held in a
**Databricks Unity Catalog**.

The CSV files in `data/` show the **exact desired output format**.

---

## Architecture – Bronze → Silver → Gold

The solution follows the Databricks **Medallion** pattern:

```
┌────────────────────────────────────────────────────────────────────┐
│  BRONZE  –  raw source tables  (sql/ddl/)                         │
│  Original column names (gender, ethnic_minor_group, …)            │
│  Category values stored as short codes (WBRI, E, AUT, …)         │
└──────────────────────────────┬─────────────────────────────────────┘
                               │ sql/views/  (CREATE OR REPLACE VIEW)
┌──────────────────────────────▼─────────────────────────────────────┐
│  SILVER  –  decoded & renamed views  (sql/views/)                 │
│  Output column names (sex, ethnicity_minor, sen_provision, …)     │
│  Human-readable labels  (Boys, White and Black Caribbean, …)      │
└──────────────────────────────┬─────────────────────────────────────┘
                               │ sql/outputs/  (SELECT … GROUP BY)
┌──────────────────────────────▼─────────────────────────────────────┐
│  GOLD  –  aggregated output tables  (sql/outputs/)                │
│  Seven report tables matching the reference CSVs in data/         │
└────────────────────────────────────────────────────────────────────┘
```

The same mappings are also available as Python dictionaries for use in the
PySpark pipeline (`src/`), which is useful for scheduled Databricks Jobs or
`spark-submit` workflows.

---

## The Problem This Solves

The raw Unity Catalog tables contain:

* **Column names** that differ from the output CSV headers  
  (e.g. `gender` → `sex`, `ethnic_minor_group` → `ethnicity_minor`,
  `sen_prov_code` → `sen_provision`).
* **Category values stored as short codes** that must be decoded into
  human-readable labels  
  (e.g. ethnicity code `WBRI` →
  `"English / Welsh / Scottish / Northern Irish / British"`,
  SEN code `E` → `"Education, health and care plan"`).

---

## Raw Source Tables (Bronze)

Five Delta tables in Unity Catalog, created from `sql/ddl/`:

| Table | DDL | Contents |
|---|---|---|
| `pupil` | `sql/ddl/pupil.sql` | NPD census extract – one row per pupil per year; demographics, SEN, geography |
| `school` | `sql/ddl/school.sql` | Establishment characteristics – type, phase, religious character |
| `results` | `sql/ddl/results.sql` | MTC check outcome – score (0–25) and completion status code |
| `claimcare` | `sql/ddl/claimcare.sql` | DfE claimcare data – FSM eligibility and disadvantage flags |
| `geography` | `sql/ddl/geography.sql` | Geographic hierarchy – LA → Region → National |

Each DDL file documents every column with its raw name, data type and
description.

---

## Silver Views – Decoded & Renamed

One view per source table, created from `sql/views/`:

| View | Script | What it does |
|---|---|---|
| `v_pupil` | `sql/views/v_pupil.sql` | Renames all columns; decodes gender, ethnicity (major + minor), first language, birth month, SEN provision, SEN primary need, establishment type |
| `v_school` | `sql/views/v_school.sql` | Decodes establishment type, education phase, religious character, gender |
| `v_results` | `sql/views/v_results.sql` | Renames `pupil_mark` → `mtc_score`; adds human-readable `completion_status_label` |
| `v_claimcare` | `sql/views/v_claimcare.sql` | Decodes `claim_type` → `disadvantage_status`; `fsm_claim_flag` → `fsm_status` |
| `v_geography` | `sql/views/v_geography.sql` | Renames all geography columns; decodes `geographic_lvl` → `geographic_level` |

---

## Output Queries (Gold)

Seven aggregation scripts in `sql/outputs/` — each joins the Silver views and
produces one of the reference CSV reports:

| Script | Output table / CSV |
|---|---|
| `national_pupil_characteristics.sql` | `mtc_national_pupil_characteristics_YYYY_to_YYYY.csv` |
| `regional_la_pupil_characteristics.sql` | `mtc_regional_la_pupil_characteristics_YYYY_to_YYYY.csv` |
| `national_school_characteristics.sql` | `mtc_national_school_characteristics_YYYY_to_YYYY.csv` |
| `national_cumulative_score_distribution.sql` | `mtc_national_cumulative_score_distribution_YYYY_to_YYYY.csv` |
| `national_score_distribution_by_school_characteristics.sql` | `mtc_national_score_distribution_by_school_characteristics_YYYY_to_YYYY.csv` |
| `national_score_distribution_by_pupil_characteristics.sql` | `mtc_national_score_distribution_by_pupil_characteristics_YYYY_to_YYYY.csv` |
| `regional_la_score_distribution_by_pupil_characteristics.sql` | `mtc_regional_la_score_distribution_by_pupil_characteristics_YYYY_to_YYYY.csv` |

---

## Category Mappings (quick reference)

| Output column | Raw code examples → Human-readable label |
|---|---|
| `sex` | `M` / `1` → `Boys` \| `F` / `2` → `Girls` |
| `ethnicity_minor` | `WBRI` → `English / Welsh / Scottish / Northern Irish / British` |
| `ethnicity_minor` | `MWBC` → `White and Black Caribbean` |
| `ethnicity_minor` | `AIND` → `Indian` \| `BAFR` → `African` |
| `first_language` | `ENG` → `Known or believed to be English` \| `OTH` → `Known or believed to be other than English` |
| `fsm_status` | `1` / `Y` → `FSM eligible` |
| `disadvantage_status` | `1` / `Y` → `Disadvantaged` |
| `sen_provision` | `E` → `Education, health and care plan` \| `K` → `SEN support / SEN without an EHC plan` |
| `sen_primary_need` | `AUT` → `Autistic spectrum disorder` \| `SEMH` → `Social, emotional and mental health` |
| `month_of_birth` | `9` → `September` \| `12` → `December` |
| `establishment_type_group` | `AC` → `Academy converter` \| `LA` → `Local authority maintained` |
| `education_phase` | `PRI` → `Highest statutory age 7` \| `SEC` → `Highest statutory age greater than 11` |
| `school_religious_character` | `CE` → `Church of England` \| `RC` → `Roman Catholic` |
| `geographic_level` | `NAT` → `National` \| `LA` → `Local authority` |

---

## Repository Structure

```
sql/
├── ddl/                                   # BRONZE – raw table definitions
│   ├── pupil.sql
│   ├── school.sql
│   ├── results.sql
│   ├── claimcare.sql
│   └── geography.sql
├── views/                                 # SILVER – decoded & renamed views
│   ├── v_pupil.sql
│   ├── v_school.sql
│   ├── v_results.sql
│   ├── v_claimcare.sql
│   └── v_geography.sql
└── outputs/                               # GOLD – aggregated report queries
    ├── national_pupil_characteristics.sql
    ├── regional_la_pupil_characteristics.sql
    ├── national_school_characteristics.sql
    ├── national_cumulative_score_distribution.sql
    ├── national_score_distribution_by_school_characteristics.sql
    ├── national_score_distribution_by_pupil_characteristics.sql
    └── regional_la_score_distribution_by_pupil_characteristics.sql
src/
├── mappings/
│   ├── column_mappings.py    # Same mappings as Python dicts (for PySpark)
│   └── category_mappings.py
├── transforms/               # PySpark transformer classes
│   ├── base.py
│   ├── pupil_characteristics.py
│   ├── school_characteristics.py
│   └── score_distributions.py
└── pipeline.py               # PySpark pipeline orchestrator / CLI
data/
└── *.csv                     # Reference output files (desired format)
```

---

## How to Use – SQL Approach (recommended for Databricks)

### Step 1 – Create the raw tables (Bronze)

Run each DDL script once against your catalog and schema:

```sql
-- In a Databricks SQL editor or notebook (%sql cell)
USE CATALOG your_catalog;
USE SCHEMA  your_schema;

-- Run each of these files:
-- sql/ddl/pupil.sql
-- sql/ddl/school.sql
-- sql/ddl/results.sql
-- sql/ddl/claimcare.sql
-- sql/ddl/geography.sql
```

Then load your source data into these tables using your existing ingestion
process (e.g. COPY INTO, Auto Loader, or a data pipeline).

### Step 2 – Create the Silver views

```sql
USE CATALOG your_catalog;
USE SCHEMA  your_schema;

-- Run each of these files:
-- sql/views/v_pupil.sql
-- sql/views/v_school.sql
-- sql/views/v_results.sql
-- sql/views/v_claimcare.sql
-- sql/views/v_geography.sql
```

### Step 3 – Run an output query (Gold)

Each script in `sql/outputs/` is a self-contained `SELECT` statement.
Use it directly or wrap it in a `CREATE OR REPLACE TABLE … AS`:

```sql
CREATE OR REPLACE TABLE mtc_national_pupil_characteristics AS
-- paste contents of sql/outputs/national_pupil_characteristics.sql here
```

To produce output for a single academic year, uncomment and set the year
filter at the bottom of each query:

```sql
-- AND p.time_period = '202324'
```

---

## How to Use – PySpark Pipeline

```python
from src.pipeline import MtcReportsPipeline

pipeline = MtcReportsPipeline(
    spark=spark,                          # Databricks SparkSession
    catalog="your_catalog",
    schema="your_schema",
    output_path="/dbfs/mnt/reports/mtc",
    time_periods=["202122", "202223", "202324", "202425"],
)
pipeline.run()
```

---

## Extending the Mappings

**Add a new raw column name** → edit the relevant dict in
`src/mappings/column_mappings.py` **and** update the corresponding
`CASE` expression in `sql/views/`.

**Add a new category code** → add a `WHEN` branch to the relevant `CASE`
expression in `sql/views/` **and** add the code to the corresponding dict in
`src/mappings/category_mappings.py`.

---

## Requirements

* Databricks Runtime ≥ 12.x (Unity Catalog support)
* PySpark ≥ 3.3 (for the Python pipeline)
* Read/write permissions on the source tables and schema
