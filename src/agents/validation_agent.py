"""
Validation Agent - Data Quality and Schema Validation
======================================================
Purpose: Validate report data quality, schema compliance, and business rules
before publication.

Key Responsibilities:
1. Schema validation (column names, data types)
2. Data quality checks (nulls, value ranges)
3. Business rule validation (MTC scores 0-25)
4. Historical drift detection
5. Row count sanity checks
6. Warning vs error classification
"""

import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

class ValidationIssue(BaseModel):
    """A single validation finding."""

    severity: str = Field(description="'error' or 'warning'")
    check_name: str = Field(description="Name of the validation check")
    message: str = Field(description="Human-readable description of the issue")
    affected_columns: List[str] = Field(default_factory=list)
    affected_rows: int = Field(default=0)
    details: Optional[Dict[str, Any]] = Field(default=None)


class ValidationResult(BaseModel):
    """Aggregated validation result for a report."""

    passed: bool = Field(description="True if no errors (warnings are acceptable)")
    report_type: str
    report_subtype: str
    row_count: int
    issues: List[ValidationIssue] = Field(default_factory=list)
    error_count: int = Field(default=0)
    warning_count: int = Field(default=0)
    summary: str = Field(default="")

    def model_post_init(self, __context: Any) -> None:
        self.error_count = sum(1 for i in self.issues if i.severity == "error")
        self.warning_count = sum(1 for i in self.issues if i.severity == "warning")
        self.passed = self.error_count == 0
        if self.passed:
            self.summary = (
                f"Validation passed with {self.warning_count} warning(s). "
                f"Row count: {self.row_count}."
            )
        else:
            self.summary = (
                f"Validation FAILED: {self.error_count} error(s), "
                f"{self.warning_count} warning(s). Row count: {self.row_count}."
            )


# ---------------------------------------------------------------------------
# Business Rule Definitions
# ---------------------------------------------------------------------------

MTC_BUSINESS_RULES: Dict[str, Any] = {
    "mtc_score": {"min": 0, "max": 25, "dtype": "numeric"},
    "average_score": {"min": 0.0, "max": 25.0, "dtype": "numeric"},
    "pupil_count": {"min": 0, "dtype": "numeric"},
    "percentage": {"min": 0.0, "max": 100.0, "dtype": "numeric"},
    "time_period": {"pattern": r"^\d{6}$", "dtype": "string"},
    "geographic_level": {
        "allowed_values": ["National", "Regional", "Local authority"],
        "dtype": "string",
    },
    "sex": {
        "allowed_values": ["Boys", "Girls", "Total"],
        "dtype": "string",
    },
}

REQUIRED_COLUMNS_BY_SUBTYPE: Dict[str, List[str]] = {
    "national_pupil_characteristics": [
        "time_period", "time_identifier", "geographic_level",
        "country_name", "sex", "pupil_count",
    ],
    "national_school_characteristics": [
        "time_period", "time_identifier", "geographic_level",
        "establishment_type_group", "pupil_count",
    ],
    "score_distribution": [
        "time_period", "geographic_level", "score", "pupil_count",
    ],
    "regional_la_pupil_characteristics": [
        "time_period", "geographic_level", "region_name", "pupil_count",
    ],
}


# ---------------------------------------------------------------------------
# Validation Agent
# ---------------------------------------------------------------------------

class ValidationAgent:
    """
    Validates educational report DataFrames against schema, data quality,
    and business rules.
    """

    def __init__(
        self,
        business_rules: Optional[Dict[str, Any]] = None,
        required_columns: Optional[Dict[str, List[str]]] = None,
        min_row_count: int = 1,
        max_null_pct: float = 0.05,
        drift_threshold_pct: float = 0.20,
    ) -> None:
        """
        Initialise the Validation Agent.

        Args:
            business_rules: Column-level business rules (overrides defaults).
            required_columns: Required columns per report subtype.
            min_row_count: Minimum acceptable row count.
            max_null_pct: Maximum acceptable null percentage per column (0-1).
            drift_threshold_pct: Row count change % that triggers a drift warning.
        """
        self.business_rules = business_rules or MTC_BUSINESS_RULES
        self.required_columns = required_columns or REQUIRED_COLUMNS_BY_SUBTYPE
        self.min_row_count = min_row_count
        self.max_null_pct = max_null_pct
        self.drift_threshold_pct = drift_threshold_pct
        logger.info("ValidationAgent initialised")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate(
        self,
        df: pd.DataFrame,
        report_type: str,
        report_subtype: str,
        historical_row_count: Optional[int] = None,
    ) -> ValidationResult:
        """
        Run all validation checks on a report DataFrame.

        Args:
            df: The report data to validate.
            report_type: e.g. 'MTC'.
            report_subtype: e.g. 'national_pupil_characteristics'.
            historical_row_count: Previous run row count for drift detection.

        Returns:
            ValidationResult with all findings.
        """
        issues: List[ValidationIssue] = []

        issues += self._check_row_count(df)
        issues += self._check_required_columns(df, report_subtype)
        issues += self._check_null_rates(df)
        issues += self._check_business_rules(df)
        if historical_row_count is not None:
            issues += self._check_historical_drift(df, historical_row_count)

        result = ValidationResult(
            passed=True,  # will be recalculated in model_post_init
            report_type=report_type,
            report_subtype=report_subtype,
            row_count=len(df),
            issues=issues,
        )
        logger.info(result.summary)
        return result

    # ------------------------------------------------------------------
    # Check methods
    # ------------------------------------------------------------------

    def _check_row_count(self, df: pd.DataFrame) -> List[ValidationIssue]:
        """Ensure the DataFrame has at least min_row_count rows."""
        if len(df) < self.min_row_count:
            return [
                ValidationIssue(
                    severity="error",
                    check_name="row_count_minimum",
                    message=(
                        f"Report has {len(df)} rows, below minimum of {self.min_row_count}."
                    ),
                    affected_rows=len(df),
                )
            ]
        return []

    def _check_required_columns(
        self, df: pd.DataFrame, report_subtype: str
    ) -> List[ValidationIssue]:
        """Check all required columns are present."""
        issues = []
        required = self.required_columns.get(report_subtype, [])
        missing = [col for col in required if col not in df.columns]
        if missing:
            issues.append(
                ValidationIssue(
                    severity="error",
                    check_name="required_columns",
                    message=f"Missing required columns: {missing}",
                    affected_columns=missing,
                )
            )
        return issues

    def _check_null_rates(self, df: pd.DataFrame) -> List[ValidationIssue]:
        """Flag columns with null rates exceeding threshold."""
        issues = []
        for col in df.columns:
            null_pct = df[col].isnull().mean()
            if null_pct > self.max_null_pct:
                severity = "error" if null_pct > 0.5 else "warning"
                issues.append(
                    ValidationIssue(
                        severity=severity,
                        check_name="null_rate",
                        message=(
                            f"Column '{col}' has {null_pct:.1%} null values "
                            f"(threshold: {self.max_null_pct:.1%})."
                        ),
                        affected_columns=[col],
                        affected_rows=int(df[col].isnull().sum()),
                    )
                )
        return issues

    def _check_business_rules(self, df: pd.DataFrame) -> List[ValidationIssue]:
        """Apply column-level business rules."""
        import re
        issues = []
        for col, rules in self.business_rules.items():
            if col not in df.columns:
                continue
            series = df[col].dropna()
            if series.empty:
                continue

            # Numeric range checks
            if rules.get("dtype") == "numeric":
                try:
                    numeric = pd.to_numeric(series, errors="coerce")
                    out_of_range = 0
                    if "min" in rules:
                        out_of_range += int((numeric < rules["min"]).sum())
                    if "max" in rules:
                        out_of_range += int((numeric > rules["max"]).sum())
                    if out_of_range > 0:
                        issues.append(
                            ValidationIssue(
                                severity="error",
                                check_name="business_rule_range",
                                message=(
                                    f"Column '{col}' has {out_of_range} value(s) outside "
                                    f"allowed range [{rules.get('min', '-inf')}, {rules.get('max', '+inf')}]."
                                ),
                                affected_columns=[col],
                                affected_rows=out_of_range,
                            )
                        )
                except Exception as exc:
                    logger.warning("Could not apply numeric check to '%s': %s", col, exc)

            # Allowed values check
            if "allowed_values" in rules:
                invalid = series[~series.isin(rules["allowed_values"])]
                if not invalid.empty:
                    issues.append(
                        ValidationIssue(
                            severity="warning",
                            check_name="business_rule_allowed_values",
                            message=(
                                f"Column '{col}' contains unexpected values: "
                                f"{invalid.unique().tolist()[:5]}"
                            ),
                            affected_columns=[col],
                            affected_rows=len(invalid),
                            details={"unexpected_values": invalid.unique().tolist()[:10]},
                        )
                    )

            # Pattern check (string columns)
            if "pattern" in rules and rules.get("dtype") == "string":
                pattern = re.compile(rules["pattern"])
                invalid_mask = ~series.astype(str).str.match(pattern)
                invalid_count = int(invalid_mask.sum())
                if invalid_count > 0:
                    issues.append(
                        ValidationIssue(
                            severity="warning",
                            check_name="business_rule_pattern",
                            message=(
                                f"Column '{col}' has {invalid_count} value(s) not matching "
                                f"pattern '{rules['pattern']}'."
                            ),
                            affected_columns=[col],
                            affected_rows=invalid_count,
                        )
                    )

        return issues

    def _check_historical_drift(
        self, df: pd.DataFrame, historical_row_count: int
    ) -> List[ValidationIssue]:
        """Detect significant row count change versus historical run."""
        if historical_row_count <= 0:
            return []
        current = len(df)
        change_pct = abs(current - historical_row_count) / historical_row_count
        if change_pct > self.drift_threshold_pct:
            severity = "error" if change_pct > 0.5 else "warning"
            return [
                ValidationIssue(
                    severity=severity,
                    check_name="historical_drift",
                    message=(
                        f"Row count changed by {change_pct:.1%} "
                        f"(previous: {historical_row_count}, current: {current})."
                    ),
                    details={
                        "previous_count": historical_row_count,
                        "current_count": current,
                        "change_pct": round(change_pct, 4),
                    },
                )
            ]
        return []


# ---------------------------------------------------------------------------
# Example Usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import numpy as np

    rng = np.random.default_rng(42)
    n = 500
    df = pd.DataFrame(
        {
            "time_period": rng.choice(["202324", "202425"], n).astype(str),
            "time_identifier": "Academic Year",
            "geographic_level": rng.choice(["National", "Regional"], n),
            "country_name": "England",
            "sex": rng.choice(["Boys", "Girls", "Total"], n),
            "pupil_count": rng.integers(100, 50000, n),
            "average_score": rng.uniform(0, 25, n),
            "mtc_score": rng.integers(0, 26, n),  # one value out of range (25 is max)
        }
    )

    agent = ValidationAgent()
    result = agent.validate(df, "MTC", "national_pupil_characteristics", historical_row_count=450)
    print(result.summary)
    for issue in result.issues:
        print(f"  [{issue.severity.upper()}] {issue.check_name}: {issue.message}")
