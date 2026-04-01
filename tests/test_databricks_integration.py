"""Tests for DatabricksConnector - connection and query execution."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.integrations.databricks_connector import (
    DatabricksConfig,
    DatabricksConnector,
    QueryResult,
)


class TestDatabricksConfig:
    def test_from_env_requires_host(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.delenv("DATABRICKS_HTTP_PATH", raising=False)
        with pytest.raises(ValueError, match="DATABRICKS_HOST"):
            DatabricksConfig.from_env()

    def test_from_env_reads_variables(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_HOST", "adb-123.azuredatabricks.net")
        monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc")
        monkeypatch.setenv("DATABRICKS_TOKEN", "mytoken")
        config = DatabricksConfig.from_env()
        assert config.host == "adb-123.azuredatabricks.net"
        assert config.access_token == "mytoken"


class TestMockMode:
    def test_connector_starts_in_mock_mode_without_config(self):
        connector = DatabricksConnector(config=None)
        # Without DATABRICKS_HOST env var it should be mock mode
        assert connector.is_mock_mode() is True

    def test_mock_query_returns_data(self):
        connector = DatabricksConnector()
        result = connector.execute_query("SELECT * FROM edu_insights.bronze.pupil LIMIT 3")
        assert result.success is True
        assert result.row_count > 0
        assert len(result.columns) > 0

    def test_mock_query_returns_valid_structure(self):
        connector = DatabricksConnector()
        result = connector.execute_query("SELECT 1")
        assert isinstance(result, QueryResult)
        assert result.data is not None

    def test_execute_to_dataframe_mock(self):
        connector = DatabricksConnector()
        df = connector.execute_to_dataframe("SELECT * FROM table")
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_get_report_registry_mock(self):
        connector = DatabricksConnector()
        # Should not raise in mock mode
        df = connector.get_report_registry()
        assert isinstance(df, pd.DataFrame)


class TestQueryResultModel:
    def test_success_result(self):
        result = QueryResult(
            success=True,
            row_count=5,
            columns=["a", "b"],
            data=[{"a": 1, "b": 2}],
        )
        assert result.success is True
        assert result.error is None

    def test_error_result(self):
        result = QueryResult(success=False, error="Connection refused")
        assert result.success is False
        assert result.row_count == 0


class TestTimeTravelValidation:
    def test_time_travel_without_version_or_timestamp_raises(self):
        connector = DatabricksConnector()
        with pytest.raises(ValueError):
            connector.execute_time_travel("edu_insights.bronze.pupil")

    def test_time_travel_with_version_mock(self):
        connector = DatabricksConnector()
        result = connector.execute_time_travel("edu_insights.bronze.pupil", version=5)
        assert result.success is True
