"""
Transforms for the four score-distribution output tables:

1. ``mtc_national_cumulative_score_distribution``
2. ``mtc_national_score_distribution_by_school_characteristics``
3. ``mtc_national_score_distribution_by_pupil_characteristics``
4. ``mtc_regional_la_score_distribution_by_pupil_characteristics``

MTC scores range from 0 (no correct answers) to 25 (all correct).

Raw source tables (Unity Catalog)
----------------------------------
* ``results``   – One row per pupil; contains ``mtc_score`` (0–25) and
                  ``completion_status``
* ``pupil``     – Pupil characteristics
* ``school``    – School characteristics
* ``claimcare`` – FSM / disadvantage eligibility
* ``geography`` – Geographic hierarchy
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.mappings.column_mappings import (
    CLAIMCARE_TABLE_COLUMNS,
    GEOGRAPHY_TABLE_COLUMNS,
    PUPIL_TABLE_COLUMNS,
    RESULTS_TABLE_COLUMNS,
    SCHOOL_TABLE_COLUMNS,
)
from src.transforms.base import BaseTransformer

# All possible MTC scores
MTC_SCORES: list[int] = list(range(25, -1, -1))  # 25 → 0

# ── Output column orders ───────────────────────────────────────────────────
CUMULATIVE_COLUMNS: list[str] = [
    "time_period",
    "time_identifier",
    "geographic_level",
    "country_code",
    "country_name",
    "establishment_type_group",
    "sex",
    "mtc_score",
    "pupil_count",
    "cumulative_pupil_count",
    "cumulative_pupil_percent",
]

_SCORE_DIST_BASE_METRICS: list[str] = (
    [f"mtc_score_{s}_pupil_count" for s in MTC_SCORES]
    + ["mtc_score_total", "mtc_score_average"]
    + ["completed_check_pupil_percent", "not_completed_check_pupil_percent"]
    + [f"mtc_score_{s}_pupil_percent" for s in MTC_SCORES]
)

SCHOOL_SCORE_DIST_COLUMNS: list[str] = [
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
] + _SCORE_DIST_BASE_METRICS

PUPIL_SCORE_DIST_NATIONAL_COLUMNS: list[str] = [
    "time_period",
    "time_identifier",
    "geographic_level",
    "country_code",
    "country_name",
    "establishment_type_group",
    "sex",
    "disadvantage_status",
    "ethnicity_major",
    "ethnicity_minor",
    "first_language",
    "fsm_status",
    "month_of_birth",
    "sen_provision",
    "sen_primary_need",
    "establishment_count",
    "eligible_pupil_count",
    "completed_check_pupil_count",
    "not_completed_check_pupil_count",
] + _SCORE_DIST_BASE_METRICS

PUPIL_SCORE_DIST_REGIONAL_COLUMNS: list[str] = [
    "time_period",
    "time_identifier",
    "geographic_level",
    "country_code",
    "country_name",
    "region_code",
    "region_name",
    "old_la_code",
    "new_la_code",
    "la_name",
    "establishment_type_group",
    "sex",
    "disadvantage_status",
    "ethnicity_major",
    "ethnicity_minor",
    "first_language",
    "fsm_status",
    "sen_provision",
    "establishment_count",
    "eligible_pupil_count",
    "completed_check_pupil_count",
    "not_completed_check_pupil_count",
] + _SCORE_DIST_BASE_METRICS


def _pivot_score_counts(df: DataFrame, group_cols: list[str]) -> DataFrame:
    """
    Aggregate pupil counts per score value (0–25) into wide-format columns
    ``mtc_score_<n>_pupil_count`` alongside summary metrics.
    """
    agg_exprs = [
        F.countDistinct("school_id").alias("establishment_count"),
        F.count("pupil_id").alias("eligible_pupil_count"),
        F.sum(
            F.when(F.col("completion_status") == "C", 1).otherwise(0)
        ).alias("completed_check_pupil_count"),
        (
            F.count("pupil_id")
            - F.sum(F.when(F.col("completion_status") == "C", 1).otherwise(0))
        ).alias("not_completed_check_pupil_count"),
        F.sum(F.col("mtc_score")).alias("mtc_score_total"),
    ]

    # One count column per score value (0–25)
    for score in MTC_SCORES:
        agg_exprs.append(
            F.sum(
                F.when(
                    (F.col("completion_status") == "C")
                    & (F.col("mtc_score") == score),
                    1,
                ).otherwise(0)
            ).alias(f"mtc_score_{score}_pupil_count")
        )

    agg_df = df.groupBy(*group_cols).agg(*agg_exprs)

    # Average score
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

    # Percentage columns
    agg_df = agg_df.withColumn(
        "completed_check_pupil_percent",
        F.round(
            F.when(
                F.col("eligible_pupil_count") > 0,
                F.col("completed_check_pupil_count") / F.col("eligible_pupil_count") * 100,
            ).otherwise(None),
            0,
        ),
    ).withColumn(
        "not_completed_check_pupil_percent",
        F.round(
            F.when(
                F.col("eligible_pupil_count") > 0,
                F.col("not_completed_check_pupil_count")
                / F.col("eligible_pupil_count")
                * 100,
            ).otherwise(None),
            0,
        ),
    )

    for score in MTC_SCORES:
        agg_df = agg_df.withColumn(
            f"mtc_score_{score}_pupil_percent",
            F.round(
                F.when(
                    F.col("completed_check_pupil_count") > 0,
                    F.col(f"mtc_score_{score}_pupil_count")
                    / F.col("completed_check_pupil_count")
                    * 100,
                ).otherwise(None),
                0,
            ),
        )

    return agg_df


class ScoreDistributionTransformer(BaseTransformer):
    """
    Produce all four score-distribution output tables.
    """

    SCHOOL_TABLE = "school"
    PUPIL_TABLE = "pupil"
    RESULTS_TABLE = "results"
    CLAIMCARE_TABLE = "claimcare"
    GEOGRAPHY_TABLE = "geography"

    def _build_pupil_results(self) -> DataFrame:
        """Join pupil, results, claimcare and geography; rename & recode."""
        pupil_df = self.read_table(self.PUPIL_TABLE)
        results_df = self.read_table(self.RESULTS_TABLE)
        claimcare_df = self.read_table(self.CLAIMCARE_TABLE)
        geography_df = self.read_table(self.GEOGRAPHY_TABLE)

        pupil_df = self.rename_columns(pupil_df, PUPIL_TABLE_COLUMNS)
        results_df = self.rename_columns(results_df, RESULTS_TABLE_COLUMNS)
        claimcare_df = self.rename_columns(claimcare_df, CLAIMCARE_TABLE_COLUMNS)
        geography_df = self.rename_columns(geography_df, GEOGRAPHY_TABLE_COLUMNS)

        df = pupil_df.join(results_df, on="pupil_id", how="left")
        df = df.join(
            claimcare_df.select("pupil_id", "disadvantage_status", "fsm_status"),
            on="pupil_id",
            how="left",
        )
        df = df.join(
            geography_df.select(
                "new_la_code",
                "old_la_code",
                "la_name",
                "region_code",
                "region_name",
                "geographic_level",
                "country_code",
                "country_name",
            ),
            on="new_la_code",
            how="left",
        )
        df = self.recode_standard_columns(df)
        return df

    def _build_school_results(self) -> DataFrame:
        """Join school, pupil and results; rename & recode."""
        school_df = self.read_table(self.SCHOOL_TABLE)
        pupil_df = self.read_table(self.PUPIL_TABLE)
        results_df = self.read_table(self.RESULTS_TABLE)

        school_df = self.rename_columns(school_df, SCHOOL_TABLE_COLUMNS)
        pupil_df = self.rename_columns(pupil_df, PUPIL_TABLE_COLUMNS)
        results_df = self.rename_columns(results_df, RESULTS_TABLE_COLUMNS)

        df = pupil_df.join(results_df, on="pupil_id", how="left")
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
        df = self.recode_standard_columns(df)
        return df

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def build_national_cumulative(self, time_period: str) -> DataFrame:
        """
        National cumulative score distribution.

        For each (sex, establishment_type_group, score) combination the output
        contains the number of pupils who achieved *at least* that score
        (cumulative count from top down) and the corresponding percentage.
        """
        df = self._build_pupil_results()
        df = df.filter(F.col("geographic_level") == "National")
        df = df.filter(F.col("completion_status") == "C")

        group_cols = [
            "establishment_type_group",
            "sex",
            "mtc_score",
            "country_code",
            "country_name",
        ]

        agg_df = df.groupBy(*group_cols).agg(
            F.count("pupil_id").alias("pupil_count")
        )

        # Cumulative counts (descending score order)
        window = Window.partitionBy(
            "establishment_type_group", "sex", "country_code", "country_name"
        ).orderBy(F.col("mtc_score").desc()).rowsBetween(
            Window.unboundedPreceding, Window.currentRow
        )
        agg_df = agg_df.withColumn(
            "cumulative_pupil_count", F.sum("pupil_count").over(window)
        )

        total_window = Window.partitionBy(
            "establishment_type_group", "sex", "country_code", "country_name"
        )
        agg_df = agg_df.withColumn(
            "total_count", F.sum("pupil_count").over(total_window)
        )
        agg_df = agg_df.withColumn(
            "cumulative_pupil_percent",
            F.round(
                F.col("cumulative_pupil_count") / F.col("total_count") * 100, 0
            ),
        ).drop("total_count")

        agg_df = self.add_time_columns(agg_df, time_period)
        agg_df = agg_df.withColumn("geographic_level", F.lit("National"))

        return self.select_output_columns(agg_df, CUMULATIVE_COLUMNS)

    def build_national_school_score_distribution(
        self, time_period: str
    ) -> DataFrame:
        """National score distribution broken down by school characteristics."""
        df = self._build_school_results()

        group_cols = [
            "establishment_type_group",
            "education_phase",
            "school_religious_character",
            "sex",
            "country_code",
            "country_name",
        ]

        agg_df = _pivot_score_counts(df, group_cols)
        agg_df = self.add_time_columns(agg_df, time_period)
        agg_df = agg_df.withColumn("geographic_level", F.lit("National"))

        return self.select_output_columns(agg_df, SCHOOL_SCORE_DIST_COLUMNS)

    def build_national_pupil_score_distribution(
        self, time_period: str
    ) -> DataFrame:
        """National score distribution broken down by pupil characteristics."""
        df = self._build_pupil_results()
        df = df.filter(F.col("geographic_level") == "National")

        group_cols = [
            "establishment_type_group",
            "sex",
            "disadvantage_status",
            "ethnicity_major",
            "ethnicity_minor",
            "first_language",
            "fsm_status",
            "month_of_birth",
            "sen_provision",
            "sen_primary_need",
            "country_code",
            "country_name",
        ]

        agg_df = _pivot_score_counts(df, group_cols)
        agg_df = self.add_time_columns(agg_df, time_period)
        agg_df = agg_df.withColumn("geographic_level", F.lit("National"))

        return self.select_output_columns(
            agg_df, PUPIL_SCORE_DIST_NATIONAL_COLUMNS
        )

    def build_regional_la_pupil_score_distribution(
        self, time_period: str
    ) -> DataFrame:
        """Regional / LA score distribution broken down by pupil characteristics."""
        df = self._build_pupil_results()
        df = df.filter(
            F.col("geographic_level").isin("Regional", "Local authority")
        )

        group_cols = [
            "geographic_level",
            "country_code",
            "country_name",
            "region_code",
            "region_name",
            "old_la_code",
            "new_la_code",
            "la_name",
            "establishment_type_group",
            "sex",
            "disadvantage_status",
            "ethnicity_major",
            "ethnicity_minor",
            "first_language",
            "fsm_status",
            "sen_provision",
        ]

        agg_df = _pivot_score_counts(df, group_cols)
        agg_df = self.add_time_columns(agg_df, time_period)

        return self.select_output_columns(
            agg_df, PUPIL_SCORE_DIST_REGIONAL_COLUMNS
        )
