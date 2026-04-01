"""Tests for ValidationAgent - quality checks and business rules."""

import numpy as np
import pandas as pd
import pytest

from src.agents.validation_agent import ValidationAgent, ValidationIssue, ValidationResult


@pytest.fixture
def valid_df():
    """A valid MTC national pupil characteristics DataFrame."""
    rng = np.random.default_rng(0)
    n = 100
    return pd.DataFrame(
        {
            "time_period": rng.choice(["202324", "202425"], n).astype(str),
            "time_identifier": "Academic Year",
            "geographic_level": rng.choice(["National", "Regional"], n),
            "country_name": "England",
            "sex": rng.choice(["Boys", "Girls", "Total"], n),
            "pupil_count": rng.integers(500, 50000, n),
            "average_score": rng.uniform(10.0, 24.0, n),
        }
    )


@pytest.fixture
def agent():
    return ValidationAgent()


class TestRowCountCheck:
    def test_sufficient_rows_passes(self, agent, valid_df):
        result = agent.validate(valid_df, "MTC", "national_pupil_characteristics")
        errors = [i for i in result.issues if i.check_name == "row_count_minimum"]
        assert len(errors) == 0

    def test_empty_df_fails(self, agent):
        empty = pd.DataFrame()
        result = agent.validate(empty, "MTC", "national_pupil_characteristics")
        assert not result.passed
        errors = [i for i in result.issues if i.check_name == "row_count_minimum"]
        assert len(errors) == 1

    def test_custom_min_row_count(self):
        a = ValidationAgent(min_row_count=200)
        df = pd.DataFrame({"time_period": ["202425"] * 100})
        result = a.validate(df, "MTC", "score_distribution")
        errors = [i for i in result.issues if i.check_name == "row_count_minimum"]
        assert len(errors) == 1


class TestRequiredColumns:
    def test_missing_column_is_error(self, agent):
        df = pd.DataFrame({"time_period": ["202425"], "sex": ["Boys"]})
        result = agent.validate(df, "MTC", "national_pupil_characteristics")
        errors = [i for i in result.issues if i.check_name == "required_columns"]
        assert len(errors) == 1
        assert not result.passed

    def test_all_columns_present_passes(self, agent, valid_df):
        result = agent.validate(valid_df, "MTC", "national_pupil_characteristics")
        errors = [i for i in result.issues if i.check_name == "required_columns"]
        assert len(errors) == 0


class TestNullRates:
    def test_high_null_rate_is_flagged(self, agent):
        df = pd.DataFrame(
            {
                "time_period": ["202425"] * 100,
                "time_identifier": "Academic Year",
                "geographic_level": ["National"] * 100,
                "country_name": ["England"] * 100,
                "sex": ["Total"] * 100,
                "pupil_count": [None] * 80 + [1000] * 20,  # 80% null
            }
        )
        result = agent.validate(df, "MTC", "national_pupil_characteristics")
        null_issues = [i for i in result.issues if i.check_name == "null_rate"]
        assert len(null_issues) > 0

    def test_low_null_rate_passes(self, agent, valid_df):
        result = agent.validate(valid_df, "MTC", "national_pupil_characteristics")
        null_issues = [i for i in result.issues if i.check_name == "null_rate"]
        assert len(null_issues) == 0


class TestBusinessRules:
    def test_mtc_score_out_of_range_flagged(self, agent):
        df = pd.DataFrame(
            {
                "time_period": ["202425"] * 5,
                "time_identifier": "Academic Year",
                "geographic_level": "National",
                "country_name": "England",
                "sex": "Total",
                "pupil_count": [1000] * 5,
                "mtc_score": [0, 10, 25, 26, 30],  # 26 and 30 out of range
            }
        )
        result = agent.validate(df, "MTC", "national_pupil_characteristics")
        range_issues = [i for i in result.issues if i.check_name == "business_rule_range"]
        assert len(range_issues) > 0

    def test_invalid_geographic_level_warned(self, agent):
        df = pd.DataFrame(
            {
                "time_period": ["202425"] * 3,
                "time_identifier": "Academic Year",
                "geographic_level": ["National", "Regional", "Planet"],  # 'Planet' invalid
                "country_name": "England",
                "sex": "Total",
                "pupil_count": [1000, 500, 100],
            }
        )
        result = agent.validate(df, "MTC", "national_pupil_characteristics")
        value_issues = [i for i in result.issues if i.check_name == "business_rule_allowed_values"]
        assert len(value_issues) > 0


class TestHistoricalDrift:
    def test_large_drift_flagged(self, agent, valid_df):
        result = agent.validate(
            valid_df, "MTC", "national_pupil_characteristics", historical_row_count=10
        )
        drift_issues = [i for i in result.issues if i.check_name == "historical_drift"]
        assert len(drift_issues) > 0

    def test_small_drift_not_flagged(self, agent, valid_df):
        result = agent.validate(
            valid_df, "MTC", "national_pupil_characteristics", historical_row_count=95
        )
        drift_issues = [i for i in result.issues if i.check_name == "historical_drift"]
        assert len(drift_issues) == 0


class TestValidationResult:
    def test_passed_when_no_errors(self, agent, valid_df):
        result = agent.validate(valid_df, "MTC", "national_pupil_characteristics")
        assert result.passed is True

    def test_summary_contains_row_count(self, agent, valid_df):
        result = agent.validate(valid_df, "MTC", "national_pupil_characteristics")
        assert str(result.row_count) in result.summary
