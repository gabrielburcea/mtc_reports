"""Tests for OrchestratorAgent - query parsing and routing logic."""

from unittest.mock import MagicMock, patch

import pytest

from src.agents.orchestrator_agent import (
    Intent,
    OrchestratorAgent,
    ParsedQuery,
    ReportType,
)


@pytest.fixture
def agent():
    """Create agent with a mocked LLM."""
    with patch("src.agents.orchestrator_agent.ChatOpenAI") as mock_llm_cls:
        mock_llm = MagicMock()
        mock_llm_cls.return_value = mock_llm
        a = OrchestratorAgent(api_key="test-key")
        a.llm = mock_llm
        yield a


class TestQueryParsing:
    def test_parse_query_returns_parsed_query(self, agent):
        expected = ParsedQuery(
            intent=Intent.GENERATE_REPORT,
            report_type=ReportType.MTC,
            report_subtype="national_pupil_characteristics",
            time_periods=["202425"],
            natural_language_query="Generate MTC report",
            confidence=0.95,
        )
        structured_mock = MagicMock()
        structured_mock.invoke.return_value = expected
        agent.llm.with_structured_output.return_value = structured_mock

        result = agent.parse_query("Generate MTC report")

        assert result.intent == Intent.GENERATE_REPORT
        assert result.report_type == ReportType.MTC
        assert result.confidence == 0.95

    def test_parse_query_returns_unknown_on_exception(self, agent):
        structured_mock = MagicMock()
        structured_mock.invoke.side_effect = Exception("LLM failure")
        agent.llm.with_structured_output.return_value = structured_mock

        result = agent.parse_query("some query")

        assert result.intent == Intent.UNKNOWN
        assert result.confidence == 0.0

    def test_parsed_query_defaults(self):
        pq = ParsedQuery(
            intent=Intent.LIST_REPORTS,
            natural_language_query="list reports",
            confidence=1.0,
        )
        assert pq.report_type is None
        assert pq.time_periods == []
        assert pq.filters == {}


class TestRouting:
    def test_routing_generate_report(self, agent):
        pq = ParsedQuery(
            intent=Intent.GENERATE_REPORT,
            natural_language_query="q",
            confidence=0.9,
        )
        assert agent.route_to_agent(pq) == "report_generation_agent"

    def test_routing_commentary(self, agent):
        pq = ParsedQuery(
            intent=Intent.ADD_COMMENTARY,
            natural_language_query="q",
            confidence=0.9,
        )
        assert agent.route_to_agent(pq) == "commentary_agent"

    def test_routing_list_reports(self, agent):
        pq = ParsedQuery(
            intent=Intent.LIST_REPORTS,
            natural_language_query="q",
            confidence=0.9,
        )
        assert agent.route_to_agent(pq) == "metadata_agent"

    def test_routing_unknown_falls_back(self, agent):
        pq = ParsedQuery(
            intent=Intent.UNKNOWN,
            natural_language_query="q",
            confidence=0.0,
        )
        assert agent.route_to_agent(pq) == "clarification_agent"


class TestClarification:
    def test_clarification_for_missing_report_type(self, agent):
        pq = ParsedQuery(
            intent=Intent.GENERATE_REPORT,
            report_type=ReportType.UNKNOWN,
            natural_language_query="make a report",
            confidence=0.4,
        )
        msg = agent.generate_clarification(pq)
        assert "clarification" in msg.lower() or "report type" in msg.lower()

    def test_clarification_uses_existing_message(self, agent):
        pq = ParsedQuery(
            intent=Intent.UNKNOWN,
            natural_language_query="q",
            confidence=0.1,
            clarification_needed="Which year?",
        )
        assert agent.generate_clarification(pq) == "Which year?"


class TestAvailableReports:
    def test_available_reports_loaded(self, agent):
        assert len(agent.available_reports) > 0
        assert all("report_type" in r for r in agent.available_reports)

    def test_available_time_periods_loaded(self, agent):
        assert len(agent.available_time_periods) > 0
