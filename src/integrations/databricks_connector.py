"""
Databricks Integration - Unity Catalog Connection Management
============================================================
Purpose: Manage connections to Databricks Unity Catalog and execute queries.

Key Responsibilities:
1. Authentication (service principal, PAT token)
2. Delta Lake query execution
3. Time-travel support for historical snapshots
4. Metadata repository queries
5. Vector Search integration stubs for RAG
"""

import logging
import os
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

import pandas as pd
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

class DatabricksConfig(BaseModel):
    """Connection configuration for Databricks."""

    host: str = Field(description="Databricks workspace hostname, e.g. adb-xxx.azuredatabricks.net")
    http_path: str = Field(description="SQL warehouse HTTP path")
    access_token: Optional[str] = Field(default=None, description="Personal access token (PAT)")
    catalog: str = Field(default="edu_insights", description="Unity Catalog catalog name")
    default_schema: str = Field(default="bronze", description="Default schema")
    timeout: int = Field(default=300, description="Query timeout in seconds")

    @classmethod
    def from_env(cls) -> "DatabricksConfig":
        """Load configuration from environment variables."""
        host = os.environ.get("DATABRICKS_HOST", "")
        http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")
        token = os.environ.get("DATABRICKS_TOKEN")
        if not host or not http_path:
            raise ValueError(
                "DATABRICKS_HOST and DATABRICKS_HTTP_PATH environment variables must be set."
            )
        return cls(host=host, http_path=http_path, access_token=token)


class QueryResult(BaseModel):
    """Result from a Databricks SQL query."""

    success: bool
    row_count: int = 0
    columns: List[str] = Field(default_factory=list)
    data: List[Dict[str, Any]] = Field(default_factory=list)
    error: Optional[str] = None
    execution_time_ms: Optional[int] = None


# ---------------------------------------------------------------------------
# Databricks Connector
# ---------------------------------------------------------------------------

class DatabricksConnector:
    """
    Manages connections and executes queries against Databricks Unity Catalog.

    Uses databricks-sql-connector for warehouse connectivity. Falls back to
    mock mode when the connector is not installed or credentials are absent.
    """

    def __init__(self, config: Optional[DatabricksConfig] = None) -> None:
        """
        Initialise connector.

        Args:
            config: DatabricksConfig instance. If None, loads from environment.
        """
        self.config = config
        self._mock_mode = False
        self._connection = None

        if config is None:
            try:
                self.config = DatabricksConfig.from_env()
            except (ValueError, Exception) as exc:
                logger.warning("Databricks config not available, using mock mode: %s", exc)
                self._mock_mode = True

        if not self._mock_mode:
            try:
                import databricks.sql  # noqa: F401
            except ImportError:
                logger.warning(
                    "databricks-sql-connector not installed. Running in mock mode. "
                    "Install with: pip install databricks-sql-connector"
                )
                self._mock_mode = True

        logger.info(
            "DatabricksConnector initialised (mock_mode=%s)", self._mock_mode
        )

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open a persistent connection to the SQL warehouse."""
        if self._mock_mode:
            logger.info("Mock mode: skipping connection")
            return
        import databricks.sql as dbsql

        assert self.config is not None
        self._connection = dbsql.connect(
            server_hostname=self.config.host,
            http_path=self.config.http_path,
            access_token=self.config.access_token,
        )
        logger.info("Connected to Databricks workspace: %s", self.config.host)

    def disconnect(self) -> None:
        """Close the persistent connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.info("Disconnected from Databricks")

    @contextmanager
    def cursor(self) -> Generator:
        """Context manager that yields a cursor and handles cleanup."""
        if self._mock_mode:
            yield None
            return

        if self._connection is None:
            self.connect()

        assert self._connection is not None
        cursor = self._connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def execute_query(
        self,
        sql: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> QueryResult:
        """
        Execute a SQL SELECT query and return results as a QueryResult.

        Args:
            sql: SQL query string.
            parameters: Optional parameter dict for parameterised queries.

        Returns:
            QueryResult with data and metadata.
        """
        import time

        if self._mock_mode:
            return self._mock_query(sql)

        start = time.time()
        try:
            with self.cursor() as cur:
                if cur is None:
                    return self._mock_query(sql)
                cur.execute(sql, parameters or {})
                columns = [desc[0] for desc in cur.description] if cur.description else []
                rows = cur.fetchall()
                data = [dict(zip(columns, row)) for row in rows]
                elapsed_ms = int((time.time() - start) * 1000)
                logger.info(
                    "Query returned %d rows in %d ms", len(data), elapsed_ms
                )
                return QueryResult(
                    success=True,
                    row_count=len(data),
                    columns=columns,
                    data=data,
                    execution_time_ms=elapsed_ms,
                )
        except Exception as exc:
            logger.error("Query execution failed: %s", exc)
            return QueryResult(success=False, error=str(exc))

    def execute_to_dataframe(
        self,
        sql: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a pandas DataFrame.

        Args:
            sql: SQL query string.
            parameters: Optional parameter dict.

        Returns:
            pandas DataFrame (empty on error).
        """
        result = self.execute_query(sql, parameters)
        if not result.success or not result.data:
            if not result.success:
                logger.warning("Returning empty DataFrame due to query error: %s", result.error)
            return pd.DataFrame(columns=result.columns)
        return pd.DataFrame(result.data)

    # ------------------------------------------------------------------
    # Time-travel support
    # ------------------------------------------------------------------

    def execute_time_travel(
        self,
        table: str,
        version: Optional[int] = None,
        timestamp: Optional[str] = None,
        additional_sql: str = "",
    ) -> QueryResult:
        """
        Execute a Delta Lake time-travel query.

        Args:
            table: Fully-qualified table name.
            version: Delta Lake version number (integer).
            timestamp: Timestamp string, e.g. '2024-01-01 00:00:00'.
            additional_sql: Optional WHERE/GROUP BY/etc to append.

        Returns:
            QueryResult.
        """
        if version is not None:
            travel_clause = f"VERSION AS OF {version}"
        elif timestamp is not None:
            travel_clause = f"TIMESTAMP AS OF '{timestamp}'"
        else:
            raise ValueError("Provide either version or timestamp for time-travel.")

        sql = f"SELECT * FROM {table} {travel_clause} {additional_sql}".strip()
        logger.info("Time-travel query: %s", sql)
        return self.execute_query(sql)

    # ------------------------------------------------------------------
    # Metadata repository helpers
    # ------------------------------------------------------------------

    def get_report_registry(self) -> pd.DataFrame:
        """Retrieve all active reports from the metadata report_registry table."""
        sql = (
            f"SELECT * FROM {self.config.catalog}.metadata.report_registry "
            "WHERE is_active = TRUE ORDER BY report_type, report_name"
            if self.config
            else "SELECT 1"
        )
        return self.execute_to_dataframe(sql)

    def get_certified_snapshots(self, report_type: Optional[str] = None) -> pd.DataFrame:
        """Retrieve certified report snapshots."""
        base_sql = (
            f"SELECT * FROM {self.config.catalog}.metadata.report_snapshots "
            "WHERE is_certified = TRUE"
            if self.config
            else "SELECT 1"
        )
        if report_type:
            base_sql += f" AND report_type = '{report_type}'"
        return self.execute_to_dataframe(base_sql)

    # ------------------------------------------------------------------
    # Mock mode
    # ------------------------------------------------------------------

    def _mock_query(self, sql: str) -> QueryResult:
        """Return mock data when running without a real Databricks connection."""
        logger.info("Mock query executed: %s", sql[:80])
        mock_data = [
            {
                "time_period": "202425",
                "geographic_level": "National",
                "country_name": "England",
                "sex": "Total",
                "pupil_count": 580000,
                "average_score": 20.2,
            },
            {
                "time_period": "202425",
                "geographic_level": "National",
                "country_name": "England",
                "sex": "Boys",
                "pupil_count": 295000,
                "average_score": 20.0,
            },
            {
                "time_period": "202425",
                "geographic_level": "National",
                "country_name": "England",
                "sex": "Girls",
                "pupil_count": 285000,
                "average_score": 20.4,
            },
        ]
        return QueryResult(
            success=True,
            row_count=len(mock_data),
            columns=list(mock_data[0].keys()),
            data=mock_data,
        )

    def is_mock_mode(self) -> bool:
        """Return True if the connector is running in mock mode."""
        return self._mock_mode


# ---------------------------------------------------------------------------
# Example Usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    connector = DatabricksConnector()
    print(f"Mock mode: {connector.is_mock_mode()}")

    result = connector.execute_query(
        "SELECT * FROM edu_insights.bronze.pupil WHERE acad_year_code = '202425' LIMIT 10"
    )
    print(f"Rows: {result.row_count}")
    df = connector.execute_to_dataframe(
        "SELECT * FROM edu_insights.bronze.results LIMIT 5"
    )
    print(df.head())
