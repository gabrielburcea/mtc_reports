"""
Transform for the national school-characteristics output table:

``mtc_national_school_characteristics``

Raw source tables (Unity Catalog)
----------------------------------
* ``school``   – Establishment-level data (type, phase, religious character)
* ``results``  – MTC check results / marks
* ``pupil``    – Used only to derive eligible / completed pupil counts
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.mappings.column_mappings import (
    PUPIL_TABLE_COLUMNS,
    RESULTS_TABLE_COLUMNS,
    SCHOOL_TABLE_COLUMNS,
)
from src.transforms.base import BaseTransformer

# ── Output column order ────────────────────────────────────────────────────
NATIONAL_SCHOOL_COLUMNS: list[str] = [
    "time_period",
    "time_identifier",
    "geographic_level",
    "country_code",
    "country_name",
    "establishment_type_group",
    "education_phase",
    "school_religious_character",
    "sex",
    "establishment_count",
    "eligible_pupil_count",
    "completed_check_pupil_count",
    "not_completed_check_pupil_count",
    "absent_pupil_count",
    "unable_to_participate_pupil_count",
    "working_below_pupil_count",
    "just_arrived_pupil_count",
    "missing_reason_pupil_count",
    "mtc_score_total",
    "mtc_score_average",
    "completed_check_pupil_percent",
    "not_completed_check_pupil_percent",
    "absent_pupil_percent",
    "unable_to_participate_pupil_percent",
    "working_below_pupil_percent",
    "just_arrived_pupil_percent",
    "missing_reason_pupil_percent",
]


class SchoolCharacteristicsTransformer(BaseTransformer):
    """
    Produce the national school-characteristics output.
    """

    SCHOOL_TABLE = "school"
    PUPIL_TABLE = "pupil"
    RESULTS_TABLE = "results"

    COMPLETED_CODE = "C"
    ABSENT_CODE = "A"
    UNABLE_CODE = "U"
    WORKING_BELOW_CODE = "W"
    JUST_ARRIVED_CODE = "J"
    MISSING_CODE = "M"

    def _build_base_dataframe(self) -> DataFrame:
        """
        Join school, pupil and results tables; rename and recode columns.
        """
        school_df = self.read_table(self.SCHOOL_TABLE)
        pupil_df = self.read_table(self.PUPIL_TABLE)
        results_df = self.read_table(self.RESULTS_TABLE)

        school_df = self.rename_columns(school_df, SCHOOL_TABLE_COLUMNS)
        pupil_df = self.rename_columns(pupil_df, PUPIL_TABLE_COLUMNS)
        results_df = self.rename_columns(results_df, RESULTS_TABLE_COLUMNS)

        # Join pupil ↔ results
        df = pupil_df.join(results_df, on="pupil_id", how="left")

        # Join with school characteristics
        df = df.join(
            school_df.select(
                "school_id",
                "establishment_type_group",
                "education_phase",
                "school_religious_character",
                "country_code",
                "country_name",
            ),
            on="school_id",
            how="left",
        )

        # Recode categorical values
        df = self.recode_standard_columns(df)

        # Completion-status indicator columns
        df = (
            df.withColumn(
                "is_completed",
                F.when(F.col("completion_status") == self.COMPLETED_CODE, 1).otherwise(0),
            )
            .withColumn(
                "is_absent",
                F.when(F.col("completion_status") == self.ABSENT_CODE, 1).otherwise(0),
            )
            .withColumn(
                "is_unable",
                F.when(F.col("completion_status") == self.UNABLE_CODE, 1).otherwise(0),
            )
            .withColumn(
                "is_working_below",
                F.when(
                    F.col("completion_status") == self.WORKING_BELOW_CODE, 1
                ).otherwise(0),
            )
            .withColumn(
                "is_just_arrived",
                F.when(
                    F.col("completion_status") == self.JUST_ARRIVED_CODE, 1
                ).otherwise(0),
            )
            .withColumn(
                "is_missing",
                F.when(F.col("completion_status") == self.MISSING_CODE, 1).otherwise(0),
            )
        )

        return df

    def build_national(self, time_period: str) -> DataFrame:
        """
        Return the national school-characteristics DataFrame for *time_period*.
        """
        df = self._build_base_dataframe()

        group_cols = [
            "establishment_type_group",
            "education_phase",
            "school_religious_character",
            "sex",
            "country_code",
            "country_name",
        ]

        agg_df = df.groupBy(*group_cols).agg(
            F.countDistinct("school_id").alias("establishment_count"),
            F.count("pupil_id").alias("eligible_pupil_count"),
            F.sum("is_completed").alias("completed_check_pupil_count"),
            (F.count("pupil_id") - F.sum("is_completed")).alias(
                "not_completed_check_pupil_count"
            ),
            F.sum("is_absent").alias("absent_pupil_count"),
            F.sum("is_unable").alias("unable_to_participate_pupil_count"),
            F.sum("is_working_below").alias("working_below_pupil_count"),
            F.sum("is_just_arrived").alias("just_arrived_pupil_count"),
            F.sum("is_missing").alias("missing_reason_pupil_count"),
            F.sum("mtc_score").alias("mtc_score_total"),
        )

        agg_df = agg_df.withColumn(
            "mtc_score_average",
            F.round(
                F.when(
                    F.col("completed_check_pupil_count") > 0,
                    F.col("mtc_score_total") / F.col("completed_check_pupil_count"),
                ).otherwise(None),
                1,
            ),
        )

        pct_pairs = [
            ("completed_check_pupil_count", "completed_check_pupil_percent"),
            ("not_completed_check_pupil_count", "not_completed_check_pupil_percent"),
            ("absent_pupil_count", "absent_pupil_percent"),
            (
                "unable_to_participate_pupil_count",
                "unable_to_participate_pupil_percent",
            ),
            ("working_below_pupil_count", "working_below_pupil_percent"),
            ("just_arrived_pupil_count", "just_arrived_pupil_percent"),
            ("missing_reason_pupil_count", "missing_reason_pupil_percent"),
        ]
        for numerator, output_col in pct_pairs:
            agg_df = self.compute_percent(
                agg_df, numerator, "eligible_pupil_count", output_col, scale=0
            )

        agg_df = self.add_time_columns(agg_df, time_period)
        agg_df = agg_df.withColumn("geographic_level", F.lit("National"))

        return self.select_output_columns(agg_df, NATIONAL_SCHOOL_COLUMNS)
