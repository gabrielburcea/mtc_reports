"""
Transforms for the two pupil-characteristics output tables:

1. ``mtc_national_pupil_characteristics``   – National level
2. ``mtc_regional_la_pupil_characteristics`` – Regional / Local-authority level

Raw source tables (Unity Catalog)
----------------------------------
* ``pupil``     – NPD census extract with pupil characteristics
* ``results``   – MTC check results / marks
* ``claimcare`` – FSM and disadvantage eligibility (DfE claimcare data)
* ``geography`` – Geographic hierarchy lookup (LA → Region → National)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.mappings.column_mappings import (
    CLAIMCARE_TABLE_COLUMNS,
    GEOGRAPHY_TABLE_COLUMNS,
    PUPIL_TABLE_COLUMNS,
    RESULTS_TABLE_COLUMNS,
)
from src.transforms.base import BaseTransformer

# ── Output column order for national pupil characteristics ─────────────────
NATIONAL_PUPIL_COLUMNS: list[str] = [
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

# ── Output column order for regional / LA pupil characteristics ────────────
REGIONAL_LA_PUPIL_COLUMNS: list[str] = [
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


class PupilCharacteristicsTransformer(BaseTransformer):
    """
    Produce the national and regional/LA pupil-characteristics outputs.
    """

    # Raw table names in Unity Catalog
    PUPIL_TABLE = "pupil"
    RESULTS_TABLE = "results"
    CLAIMCARE_TABLE = "claimcare"
    GEOGRAPHY_TABLE = "geography"

    # Completion-status codes in the raw results table
    COMPLETED_CODE = "C"          # check completed
    ABSENT_CODE = "A"             # absent
    UNABLE_CODE = "U"             # unable to participate
    WORKING_BELOW_CODE = "W"      # working below standard
    JUST_ARRIVED_CODE = "J"       # just arrived in England
    MISSING_CODE = "M"            # missing reason

    def _build_base_dataframe(self) -> DataFrame:
        """
        Join pupil census, MTC results, claimcare and geography tables,
        apply column renames and category recoding, and return a single
        flat DataFrame ready for aggregation.
        """
        # --- Load raw tables --------------------------------------------------
        pupil_df = self.read_table(self.PUPIL_TABLE)
        results_df = self.read_table(self.RESULTS_TABLE)
        claimcare_df = self.read_table(self.CLAIMCARE_TABLE)
        geography_df = self.read_table(self.GEOGRAPHY_TABLE)

        # --- Rename columns ---------------------------------------------------
        pupil_df = self.rename_columns(pupil_df, PUPIL_TABLE_COLUMNS)
        results_df = self.rename_columns(results_df, RESULTS_TABLE_COLUMNS)
        claimcare_df = self.rename_columns(claimcare_df, CLAIMCARE_TABLE_COLUMNS)
        geography_df = self.rename_columns(geography_df, GEOGRAPHY_TABLE_COLUMNS)

        # --- Join pupil ↔ results ---------------------------------------------
        df = pupil_df.join(results_df, on="pupil_id", how="left")

        # --- Join claimcare (FSM / disadvantage) ------------------------------
        df = df.join(
            claimcare_df.select("pupil_id", "disadvantage_status", "fsm_status"),
            on="pupil_id",
            how="left",
        )

        # --- Join geography ---------------------------------------------------
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

        # --- Recode categorical values ----------------------------------------
        df = self.recode_standard_columns(df)

        # --- Derive completion-status indicator columns -----------------------
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
                F.when(F.col("completion_status") == self.WORKING_BELOW_CODE, 1).otherwise(0),
            )
            .withColumn(
                "is_just_arrived",
                F.when(F.col("completion_status") == self.JUST_ARRIVED_CODE, 1).otherwise(0),
            )
            .withColumn(
                "is_missing",
                F.when(F.col("completion_status") == self.MISSING_CODE, 1).otherwise(0),
            )
        )

        return df

    def _aggregate(
        self,
        df: DataFrame,
        group_cols: list[str],
    ) -> DataFrame:
        """
        Aggregate pupil counts, scores and percentages over *group_cols*.
        """
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

        # Average score (completed checks only)
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
        pct_pairs = [
            ("completed_check_pupil_count", "completed_check_pupil_percent"),
            ("not_completed_check_pupil_count", "not_completed_check_pupil_percent"),
            ("absent_pupil_count", "absent_pupil_percent"),
            ("unable_to_participate_pupil_count", "unable_to_participate_pupil_percent"),
            ("working_below_pupil_count", "working_below_pupil_percent"),
            ("just_arrived_pupil_count", "just_arrived_pupil_percent"),
            ("missing_reason_pupil_count", "missing_reason_pupil_percent"),
        ]
        for numerator, output_col in pct_pairs:
            agg_df = self.compute_percent(
                agg_df, numerator, "eligible_pupil_count", output_col, scale=0
            )

        return agg_df

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def build_national(self, time_period: str) -> DataFrame:
        """
        Return the national pupil-characteristics DataFrame for *time_period*.
        """
        df = self._build_base_dataframe()
        df = df.filter(F.col("geographic_level") == "National")

        group_cols = [
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
        ]

        agg_df = self._aggregate(df, group_cols)
        agg_df = self.add_time_columns(agg_df, time_period)
        return self.select_output_columns(agg_df, NATIONAL_PUPIL_COLUMNS)

    def build_regional_la(self, time_period: str) -> DataFrame:
        """
        Return the regional / local-authority pupil-characteristics DataFrame.
        """
        df = self._build_base_dataframe()
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

        agg_df = self._aggregate(df, group_cols)
        agg_df = self.add_time_columns(agg_df, time_period)
        return self.select_output_columns(agg_df, REGIONAL_LA_PUPIL_COLUMNS)
