"""
MTC Reports Pipeline
====================

Orchestrates the full ETL pipeline that reads raw tables from a Databricks
Unity Catalog, applies column-name mappings and category-code recoding, and
writes the seven CSV output files that match the desired report format.

Usage (Databricks notebook cell)
---------------------------------
.. code-block:: python

    from src.pipeline import MtcReportsPipeline

    pipeline = MtcReportsPipeline(
        spark=spark,                        # Databricks SparkSession
        catalog="your_catalog",
        schema="your_schema",
        output_path="/dbfs/mnt/reports/mtc",
        time_periods=["202122", "202223", "202324", "202425"],
    )
    pipeline.run()

Command-line (``spark-submit``)
--------------------------------
.. code-block:: bash

    spark-submit src/pipeline.py \\
        --catalog your_catalog \\
        --schema  your_schema  \\
        --output  /dbfs/mnt/reports/mtc \\
        --years   202122 202223 202324 202425
"""

from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.transforms.pupil_characteristics import PupilCharacteristicsTransformer
from src.transforms.school_characteristics import SchoolCharacteristicsTransformer
from src.transforms.score_distributions import ScoreDistributionTransformer

logger = logging.getLogger(__name__)

# ── Output file names (without extension) ─────────────────────────────────
_OUTPUT_FILES = {
    "national_pupil":           "mtc_national_pupil_characteristics_{years}",
    "regional_la_pupil":        "mtc_regional_la_pupil_characteristics_{years}",
    "national_school":          "mtc_national_school_characteristics_{years}",
    "national_cumulative":      "mtc_national_cumulative_score_distribution_{years}",
    "national_school_scores":   "mtc_national_score_distribution_by_school_characteristics_{years}",
    "national_pupil_scores":    "mtc_national_score_distribution_by_pupil_characteristics_{years}",
    "regional_la_pupil_scores": "mtc_regional_la_score_distribution_by_pupil_characteristics_{years}",
}


class MtcReportsPipeline:
    """
    End-to-end pipeline: Unity Catalog → transformed DataFrames → CSV files.

    Parameters
    ----------
    spark : SparkSession
    catalog : str
        Unity Catalog name.
    schema : str
        Database / schema name inside the catalog.
    output_path : str
        Root directory where CSV files will be written (DBFS path or local).
    time_periods : list[str]
        Academic year codes to process (e.g. ``["202122", "202223"]``).
        Results for all years are stacked into a single output file per table.
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        schema: str,
        output_path: str,
        time_periods: list[str],
    ) -> None:
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.output_path = output_path
        self.time_periods = time_periods

        self._pupil_tx = PupilCharacteristicsTransformer(spark, catalog, schema)
        self._school_tx = SchoolCharacteristicsTransformer(spark, catalog, schema)
        self._score_tx = ScoreDistributionTransformer(spark, catalog, schema)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _year_suffix(self) -> str:
        """e.g. "2022_to_2025" derived from the time_periods list."""
        years = sorted(self.time_periods)
        first_start = years[0][:4]
        last_end = "20" + years[-1][4:]
        return f"{first_start}_to_{last_end}"

    def _union_across_years(
        self,
        build_fn,
    ) -> DataFrame:
        """Call *build_fn(time_period)* for every year and union the results."""
        frames = [build_fn(tp) for tp in self.time_periods]
        result = frames[0]
        for df in frames[1:]:
            result = result.unionByName(df)
        return result

    def _write_csv(self, df: DataFrame, key: str) -> None:
        """Write *df* as a single CSV file to ``output_path``."""
        suffix = self._year_suffix()
        filename = _OUTPUT_FILES[key].format(years=suffix)
        full_path = os.path.join(self.output_path, filename)

        logger.info("Writing %s …", filename)
        (
            df.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(full_path)
        )
        logger.info("  ✓ %s", full_path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Execute the full pipeline for all configured time periods."""
        logger.info(
            "Starting MTC reports pipeline | catalog=%s schema=%s years=%s",
            self.catalog,
            self.schema,
            self.time_periods,
        )

        self._write_csv(
            self._union_across_years(self._pupil_tx.build_national),
            "national_pupil",
        )
        self._write_csv(
            self._union_across_years(self._pupil_tx.build_regional_la),
            "regional_la_pupil",
        )
        self._write_csv(
            self._union_across_years(self._school_tx.build_national),
            "national_school",
        )
        self._write_csv(
            self._union_across_years(self._score_tx.build_national_cumulative),
            "national_cumulative",
        )
        self._write_csv(
            self._union_across_years(
                self._score_tx.build_national_school_score_distribution
            ),
            "national_school_scores",
        )
        self._write_csv(
            self._union_across_years(
                self._score_tx.build_national_pupil_score_distribution
            ),
            "national_pupil_scores",
        )
        self._write_csv(
            self._union_across_years(
                self._score_tx.build_regional_la_pupil_score_distribution
            ),
            "regional_la_pupil_scores",
        )

        logger.info("Pipeline complete.")


# ── CLI entry-point ────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the MTC reports pipeline.")
    parser.add_argument("--catalog", required=True, help="Unity Catalog name")
    parser.add_argument("--schema", required=True, help="Schema / database name")
    parser.add_argument("--output", required=True, help="Output root path (DBFS or local)")
    parser.add_argument(
        "--years",
        nargs="+",
        required=True,
        help="Academic year codes, e.g. 202122 202223",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args()

    spark = SparkSession.builder.appName("mtc_reports").getOrCreate()

    MtcReportsPipeline(
        spark=spark,
        catalog=args.catalog,
        schema=args.schema,
        output_path=args.output,
        time_periods=args.years,
    ).run()
