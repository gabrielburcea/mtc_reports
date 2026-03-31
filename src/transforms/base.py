"""
Base transformer class shared by all output-specific transformers.

The class provides helpers for:
  * renaming columns using a column-mapping dictionary
  * recoding category values using a category-mapping dictionary
  * rounding numeric columns consistently
  * adding the standard time / geography header columns
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.mappings.category_mappings import ALL_CATEGORY_MAPPINGS


class BaseTransformer:
    """
    Common transformation logic for all MTC report outputs.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session (provided by Databricks at runtime).
    catalog : str
        Unity Catalog name (e.g. ``"mtc_catalog"``).
    schema : str
        Database / schema within the catalog (e.g. ``"mtc_data"``).
    """

    def __init__(self, spark: SparkSession, catalog: str, schema: str) -> None:
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _full_table_name(self, table: str) -> str:
        """Return a fully-qualified Unity Catalog table reference."""
        return f"{self.catalog}.{self.schema}.{table}"

    def read_table(self, table: str) -> DataFrame:
        """Read a Unity Catalog table into a Spark DataFrame."""
        return self.spark.read.table(self._full_table_name(table))

    @staticmethod
    def rename_columns(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
        """
        Rename DataFrame columns using *mapping* (raw name → output name).

        Only columns present in *df* are renamed; unknown keys are silently
        ignored so that partial mappings are safe to use.
        """
        for raw, output in mapping.items():
            if raw in df.columns:
                df = df.withColumnRenamed(raw, output)
        return df

    @staticmethod
    def recode_column(
        df: DataFrame,
        column: str,
        mapping: dict[str, str],
        default: str = "Total",
    ) -> DataFrame:
        """
        Map raw category codes in *column* to human-readable labels.

        Values not found in *mapping* are replaced with *default*.

        Parameters
        ----------
        df : DataFrame
        column : str
            Name of the column to recode (must already be the *output* name).
        mapping : dict[str, str]
            Code → label lookup.
        default : str
            Fallback label for unmapped codes.
        """
        if column not in df.columns:
            return df

        expr = F.lit(default)
        for code, label in mapping.items():
            expr = F.when(F.col(column) == code, label).otherwise(expr)

        return df.withColumn(column, expr)

    def recode_standard_columns(self, df: DataFrame) -> DataFrame:
        """
        Apply all category mappings in :data:`ALL_CATEGORY_MAPPINGS` to *df*.

        Only columns that are present in *df* are recoded.
        """
        for col_name, mapping in ALL_CATEGORY_MAPPINGS.items():
            df = self.recode_column(df, col_name, mapping)
        return df

    @staticmethod
    def round_numeric_columns(
        df: DataFrame,
        columns: list[str],
        scale: int = 1,
    ) -> DataFrame:
        """Round the listed numeric *columns* to *scale* decimal places."""
        for col in columns:
            if col in df.columns:
                df = df.withColumn(col, F.round(F.col(col), scale))
        return df

    @staticmethod
    def add_time_columns(
        df: DataFrame,
        time_period: str,
        time_identifier: str = "Academic year",
    ) -> DataFrame:
        """
        Add or overwrite ``time_period`` and ``time_identifier`` columns.

        *time_period* should be in the format ``YYYYYYYY`` (e.g. ``"202122"``).
        """
        return df.withColumn("time_period", F.lit(time_period)).withColumn(
            "time_identifier", F.lit(time_identifier)
        )

    @staticmethod
    def compute_percent(
        df: DataFrame,
        numerator: str,
        denominator: str,
        output_col: str,
        scale: int = 0,
    ) -> DataFrame:
        """
        Add a percentage column as ``round(numerator / denominator * 100, scale)``.

        Safe-divides: returns ``None`` when *denominator* is zero or null.
        """
        return df.withColumn(
            output_col,
            F.round(
                F.when(
                    F.col(denominator) > 0,
                    F.col(numerator) / F.col(denominator) * 100,
                ).otherwise(None),
                scale,
            ),
        )

    @staticmethod
    def select_output_columns(df: DataFrame, columns: list[str]) -> DataFrame:
        """
        Return *df* with only *columns* in the specified order.

        Missing columns are added as ``null`` to guarantee a fixed schema.
        """
        exprs: list[Any] = []
        for col in columns:
            if col in df.columns:
                exprs.append(F.col(col))
            else:
                exprs.append(F.lit(None).alias(col))
        return df.select(*exprs)
