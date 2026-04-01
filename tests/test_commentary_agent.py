"""Tests for CommentaryAgent - statistical analysis and narrative generation."""

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.agents.commentary_agent import (
    CommentaryAgent,
    CommentaryRequest,
    CommentaryResult,
    StatisticalAnalyser,
)


@pytest.fixture
def sample_df():
    """Realistic MTC report DataFrame."""
    rng = np.random.default_rng(42)
    n = 300
    return pd.DataFrame(
        {
            "time_period": rng.choice(["202324", "202425"], n).astype(str),
            "sex": rng.choice(["Boys", "Girls"], n),
            "sen_provision": rng.choice(["No SEN provision", "SEN support / SEN without an EHC plan"], n),
            "fsm_status": rng.choice(["FSM eligible", "Not known to be FSM eligible"], n),
            "region_name": rng.choice(["London", "South East", "North West"], n),
            "average_score": rng.uniform(15.0, 24.0, n),
        }
    )


class TestStatisticalAnalyser:
    def test_summary_has_overall_mean(self, sample_df):
        summary = StatisticalAnalyser.compute_summary(sample_df, "average_score")
        assert "overall_mean" in summary
        assert 15.0 <= summary["overall_mean"] <= 24.0

    def test_summary_has_yoy_change(self, sample_df):
        summary = StatisticalAnalyser.compute_summary(sample_df, "average_score")
        assert "yoy_change" in summary
        assert "yoy_pct_change" in summary

    def test_summary_has_gender_gap(self, sample_df):
        summary = StatisticalAnalyser.compute_summary(sample_df, "average_score")
        assert "gender_gap" in summary
        assert "Boys" in summary["gender_gap"]
        assert "Girls" in summary["gender_gap"]

    def test_summary_has_fsm_gap(self, sample_df):
        summary = StatisticalAnalyser.compute_summary(sample_df, "average_score")
        assert "fsm_gap" in summary

    def test_summary_has_regional_range(self, sample_df):
        summary = StatisticalAnalyser.compute_summary(sample_df, "average_score")
        assert "regional_range" in summary
        assert "highest_region" in summary

    def test_empty_df_returns_empty(self):
        summary = StatisticalAnalyser.compute_summary(pd.DataFrame(), "average_score")
        assert summary == {}

    def test_detect_trends_increasing(self):
        trend = StatisticalAnalyser.detect_trends({"2022": 18.0, "2023": 19.5, "2024": 21.0})
        assert trend == "increasing"

    def test_detect_trends_decreasing(self):
        trend = StatisticalAnalyser.detect_trends({"2022": 21.0, "2023": 20.0, "2024": 19.0})
        assert trend == "decreasing"

    def test_detect_trends_stable(self):
        trend = StatisticalAnalyser.detect_trends({"2022": 20.0, "2023": 20.02, "2024": 19.99})
        assert trend == "stable"

    def test_detect_trends_single_year(self):
        trend = StatisticalAnalyser.detect_trends({"2024": 20.0})
        assert trend == "stable"


class TestCommentaryAgent:
    @pytest.fixture
    def agent(self):
        with patch("src.agents.commentary_agent.ChatOpenAI") as mock_cls:
            mock_llm = MagicMock()
            mock_cls.return_value = mock_llm
            a = CommentaryAgent(api_key="test-key")
            a.llm = mock_llm
            return a

    def test_generate_returns_commentary_result(self, agent, sample_df):
        expected = CommentaryResult(
            headline="MTC results are improving.",
            key_findings=["Average score increased by 0.5 pp."],
            detailed_commentary="Overall, MTC results improved in 2024-25.",
        )
        structured_mock = MagicMock()
        structured_mock.invoke.return_value = expected
        agent.llm.with_structured_output.return_value = structured_mock

        result = agent.generate_from_dataframe(sample_df, "MTC", "national_pupil_characteristics")
        assert result.headline == "MTC results are improving."
        assert len(result.key_findings) > 0

    def test_fallback_commentary_on_llm_error(self, agent, sample_df):
        structured_mock = MagicMock()
        structured_mock.invoke.side_effect = Exception("LLM error")
        agent.llm.with_structured_output.return_value = structured_mock

        result = agent.generate_from_dataframe(sample_df, "MTC", "national_pupil_characteristics")
        # Should not raise, should return fallback
        assert isinstance(result, CommentaryResult)
        assert result.headline != ""
        assert len(result.data_quality_notes) > 0

    def test_generate_with_request(self, agent):
        request = CommentaryRequest(
            report_type="MTC",
            report_subtype="national_pupil_characteristics",
            time_periods=["202425"],
            data_summary={"overall_mean": 20.2, "yoy_change": 0.5},
        )
        expected = CommentaryResult(
            headline="Test headline",
            key_findings=["Finding 1"],
            detailed_commentary="Details.",
        )
        structured_mock = MagicMock()
        structured_mock.invoke.return_value = expected
        agent.llm.with_structured_output.return_value = structured_mock

        result = agent.generate(request)
        assert result.headline == "Test headline"
