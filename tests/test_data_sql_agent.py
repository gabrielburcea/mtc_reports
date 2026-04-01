"""Tests for DataSqlAgent - SQL generation and validation."""

from unittest.mock import MagicMock, patch

import pytest

from src.agents.data_sql_agent import (
    DataSqlAgent,
    SQLGenerationRequest,
    SQLGenerationResult,
    sanitize_natural_language,
    sanitize_time_periods,
)


class TestSanitization:
    def test_valid_input_passes(self):
        result = sanitize_natural_language("Show MTC scores by gender for 2024-25")
        assert "MTC" in result

    def test_forbidden_keyword_raises(self):
        with pytest.raises(ValueError, match="forbidden SQL keywords"):
            sanitize_natural_language("DROP TABLE pupils")

    def test_delete_raises(self):
        with pytest.raises(ValueError):
            sanitize_natural_language("DELETE FROM results")

    def test_valid_time_period(self):
        result = sanitize_time_periods(["202425", "202324"])
        assert result == ["202425", "202324"]

    def test_invalid_time_period_raises(self):
        with pytest.raises(ValueError, match="Invalid time period format"):
            sanitize_time_periods(["2024-25"])

    def test_non_numeric_period_raises(self):
        with pytest.raises(ValueError):
            sanitize_time_periods(["abcdef"])


class TestSQLValidation:
    @pytest.fixture
    def agent(self):
        with patch("src.agents.data_sql_agent.ChatOpenAI"):
            return DataSqlAgent(api_key="test-key")

    def test_valid_select_passes(self, agent):
        valid, err = agent._validate_sql("SELECT id FROM edu_insights.bronze.pupil")
        assert valid is True
        assert err is None

    def test_non_select_fails(self, agent):
        valid, err = agent._validate_sql("INSERT INTO table VALUES (1)")
        assert valid is False
        assert err is not None

    def test_forbidden_ddl_fails(self, agent):
        valid, err = agent._validate_sql("SELECT * FROM t; DROP TABLE t")
        assert valid is False

    def test_unbalanced_parens_fails(self, agent):
        valid, err = agent._validate_sql("SELECT COUNT( FROM t")
        assert valid is False


class TestTemplateRetrieval:
    @pytest.fixture
    def agent(self):
        with patch("src.agents.data_sql_agent.ChatOpenAI"):
            return DataSqlAgent(api_key="test-key")

    def test_retrieves_national_template(self, agent):
        result = agent._retrieve_similar_template("national pupil characteristics by gender")
        assert result is not None
        name, sql = result
        assert "national" in name

    def test_retrieves_score_distribution(self, agent):
        result = agent._retrieve_similar_template("score distribution breakdown")
        assert result is not None
        name, sql = result
        assert "score" in name

    def test_no_match_returns_none(self, agent):
        result = agent._retrieve_similar_template("xyzzy unfamiliar query type")
        assert result is None


class TestSQLGeneration:
    @pytest.fixture
    def agent(self):
        with patch("src.agents.data_sql_agent.ChatOpenAI") as mock_cls:
            mock_llm = MagicMock()
            mock_cls.return_value = mock_llm
            a = DataSqlAgent(api_key="test-key")
            a.llm = mock_llm
            return a

    def test_successful_generation(self, agent):
        mock_response = MagicMock()
        mock_response.content = "SELECT * FROM edu_insights.bronze.pupil"
        agent.llm.invoke.return_value = mock_response

        request = SQLGenerationRequest(
            natural_language="Get all pupils",
            time_periods=["202425"],
        )
        result = agent.generate_query_structured(request)
        assert result.is_valid is True
        assert "SELECT" in result.sql

    def test_retry_on_invalid_sql(self, agent):
        mock_response_bad = MagicMock()
        mock_response_bad.content = "INSERT INTO bad"
        mock_response_good = MagicMock()
        mock_response_good.content = "SELECT id FROM edu_insights.bronze.pupil"
        agent.llm.invoke.side_effect = [mock_response_bad, mock_response_good]

        request = SQLGenerationRequest(
            natural_language="Get pupils", max_retries=2
        )
        result = agent.generate_query_structured(request)
        assert result.is_valid is True
        assert result.retries_used >= 1

    def test_generate_query_raises_on_failure(self, agent):
        mock_response = MagicMock()
        mock_response.content = "NOT A SELECT STATEMENT"
        agent.llm.invoke.return_value = mock_response

        with pytest.raises(RuntimeError):
            agent.generate_query("Get data")

    def test_strips_markdown_fences(self, agent):
        mock_response = MagicMock()
        mock_response.content = "```sql\nSELECT 1\n```"
        agent.llm.invoke.return_value = mock_response

        request = SQLGenerationRequest(natural_language="Get one")
        result = agent.generate_query_structured(request)
        assert "```" not in result.sql
