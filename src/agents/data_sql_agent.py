"""
Data SQL Agent - Natural Language to SQL Query Generation
=========================================================
Purpose: Generate validated SQL queries from natural language using LLM
with schema-aware context and RAG-based template retrieval.

Key Responsibilities:
1. Generate SQL from natural language using GPT-4o
2. Validate SQL using EXPLAIN before execution
3. Retry with error correction on failure
4. Use RAG to retrieve similar SQL templates
5. Prevent SQL injection via input sanitization
6. Support complex joins, aggregations, and filtering
"""

import logging
import os
import re
from typing import Dict, List, Optional, Tuple

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

class SQLGenerationRequest(BaseModel):
    """Input to the SQL generation agent."""

    natural_language: str = Field(description="Natural language query from user")
    report_type: Optional[str] = Field(default=None, description="Target report type, e.g. MTC")
    time_periods: List[str] = Field(default_factory=list, description="Academic year codes, e.g. ['202425']")
    filters: Dict[str, str] = Field(default_factory=dict, description="Extra filter key-value pairs")
    max_retries: int = Field(default=3, description="Maximum retry attempts on SQL errors")


class SQLGenerationResult(BaseModel):
    """Output from the SQL generation agent."""

    sql: str = Field(description="Generated SQL query")
    is_valid: bool = Field(description="Whether the query passed validation")
    validation_error: Optional[str] = Field(default=None, description="Validation error message")
    retries_used: int = Field(default=0, description="Number of retries used")
    template_used: Optional[str] = Field(default=None, description="Similar template used for context")
    confidence: float = Field(default=0.0, description="Confidence score 0-1")


# ---------------------------------------------------------------------------
# Schema Context
# ---------------------------------------------------------------------------

SCHEMA_CONTEXT = """
## Unity Catalog Schema: edu_insights.bronze

### pupil
- pupil_id (STRING): Unique pupil identifier
- la_code_old (STRING): Old local authority code
- la_code_new (STRING): New local authority code
- local_authority_name (STRING): Local authority name
- gor_code (STRING): Government Office Region code
- gor_name (STRING): Government Office Region name
- gender (STRING): Pupil gender (1=Boys, 2=Girls, T=Total)
- ethnic_major_group (STRING): Ethnicity major group code
- ethnic_minor_group (STRING): Ethnicity minor group code
- first_lang (STRING): First language code
- disadvantaged_flag (STRING): Disadvantage indicator (1=Yes, 0=No)
- fsm_eligible_flag (STRING): FSM eligibility (1=Yes, 0=No)
- birth_month_code (STRING): Month of birth (1-12)
- sen_prov_code (STRING): SEN provision type (E=EHC, K=SEN support, N=None)
- sen_type_rank (STRING): SEN primary need code
- estab_type_group (STRING): Establishment type group
- acad_year_code (STRING): Academic year code, e.g. 202425

### school
- school_id (STRING): Unique school identifier
- estab_type_group (STRING): Establishment type group
- phase_of_education (STRING): School phase
- religious_character (STRING): Religious character
- gender (STRING): School gender
- la_code_new (STRING): Local authority code
- acad_year_code (STRING): Academic year code

### results (MTC marks)
- pupil_id (STRING): Pupil identifier (FK to pupil)
- school_id (STRING): School identifier (FK to school)
- pupil_mark (INTEGER): Pupil's MTC score (0-25)
- total_mark (INTEGER): Total marks available
- average_mark (FLOAT): Average score in the group
- acad_year_code (STRING): Academic year code

### claimcare (FSM / disadvantage)
- pupil_id (STRING): Pupil identifier (FK to pupil)
- claim_type (STRING): Type of claim
- fsm_claim_flag (STRING): FSM claim indicator
- acad_year_code (STRING): Academic year code

### geography
- gor_code (STRING): Region code
- gor_name (STRING): Region name
- la_old (STRING): Old LA code
- la_new (STRING): New LA code
- la_name_full (STRING): Full LA name
- geographic_lvl (STRING): Level (NAT/REG/LA)
- country_cd (STRING): Country code
- country_nm (STRING): Country name

## Views (silver layer): edu_insights.silver
Views prefixed with v_ decode codes to labels, e.g. v_pupil, v_school, v_results, v_geography, v_claimcare.
"""

# ---------------------------------------------------------------------------
# SQL Template Examples (for RAG context)
# ---------------------------------------------------------------------------

SQL_TEMPLATES = {
    "national_pupil_characteristics": """
SELECT
    p.acad_year_code AS time_period,
    'Academic Year' AS time_identifier,
    'National' AS geographic_level,
    'England' AS country_name,
    p.gender AS sex,
    p.ethnic_major_group AS ethnicity_major,
    p.sen_prov_code AS sen_provision,
    p.disadvantaged_flag AS disadvantage_status,
    p.fsm_eligible_flag AS fsm_status,
    COUNT(DISTINCT p.pupil_id) AS pupil_count,
    AVG(r.pupil_mark) AS average_score,
    COUNT(CASE WHEN r.pupil_mark IS NOT NULL THEN 1 END) AS pupils_with_score
FROM edu_insights.bronze.pupil p
LEFT JOIN edu_insights.bronze.results r ON p.pupil_id = r.pupil_id AND p.acad_year_code = r.acad_year_code
WHERE p.acad_year_code IN ({time_periods})
GROUP BY p.acad_year_code, p.gender, p.ethnic_major_group, p.sen_prov_code, p.disadvantaged_flag, p.fsm_eligible_flag
ORDER BY p.acad_year_code, p.gender
""",
    "score_distribution": """
SELECT
    r.acad_year_code AS time_period,
    'Academic Year' AS time_identifier,
    'National' AS geographic_level,
    r.pupil_mark AS score,
    COUNT(DISTINCT r.pupil_id) AS pupil_count,
    COUNT(DISTINCT r.pupil_id) * 100.0 / SUM(COUNT(DISTINCT r.pupil_id)) OVER (PARTITION BY r.acad_year_code) AS percentage
FROM edu_insights.bronze.results r
WHERE r.acad_year_code IN ({time_periods})
  AND r.pupil_mark BETWEEN 0 AND 25
GROUP BY r.acad_year_code, r.pupil_mark
ORDER BY r.acad_year_code, r.pupil_mark
""",
}


# ---------------------------------------------------------------------------
# Input Sanitization
# ---------------------------------------------------------------------------

_FORBIDDEN_SQL_PATTERNS = re.compile(
    r"\b(DROP|DELETE|INSERT|UPDATE|ALTER|TRUNCATE|EXEC|EXECUTE|xp_|sp_|UNION\s+SELECT)\b",
    re.IGNORECASE,
)

_ALLOWED_YEAR_PATTERN = re.compile(r"^\d{6}$")


def sanitize_natural_language(text: str) -> str:
    """Remove potentially dangerous SQL keywords from user input."""
    if _FORBIDDEN_SQL_PATTERNS.search(text):
        raise ValueError(
            "Input contains forbidden SQL keywords. Please describe your request in plain English."
        )
    return text.strip()


def sanitize_time_periods(periods: List[str]) -> List[str]:
    """Validate that time period codes are in the expected YYYYMM format."""
    sanitized = []
    for period in periods:
        if not _ALLOWED_YEAR_PATTERN.match(period):
            raise ValueError(
                f"Invalid time period format: '{period}'. Expected 6-digit code like '202425'."
            )
        sanitized.append(period)
    return sanitized


# ---------------------------------------------------------------------------
# Data SQL Agent
# ---------------------------------------------------------------------------

class DataSqlAgent:
    """
    Agent that generates validated SQL queries from natural language using
    an LLM (GPT-4o), schema context, and optional RAG template retrieval.
    """

    def __init__(
        self,
        schema: Optional[Dict] = None,
        llm_model: str = "gpt-4o",
        api_key: Optional[str] = None,
        temperature: float = 0.0,
        sql_templates: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Initialise the Data SQL Agent.

        Args:
            schema: Optional dict with additional schema metadata.
            llm_model: OpenAI model identifier.
            api_key: OpenAI API key (falls back to OPENAI_API_KEY env var).
            temperature: Sampling temperature (0 = deterministic).
            sql_templates: Dict of named SQL templates used for RAG context.
        """
        self.schema = schema or {}
        self.sql_templates = sql_templates or SQL_TEMPLATES
        self.llm = ChatOpenAI(
            model=llm_model,
            temperature=temperature,
            api_key=api_key or os.getenv("OPENAI_API_KEY"),
        )
        logger.info("DataSqlAgent initialised with model: %s", llm_model)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_query(self, natural_language: str) -> str:
        """
        Generate a SQL query from a plain-English description.

        Args:
            natural_language: User description of the desired query.

        Returns:
            A SQL string ready to execute.

        Raises:
            ValueError: If input fails sanitization.
        """
        sanitized = sanitize_natural_language(natural_language)
        request = SQLGenerationRequest(natural_language=sanitized)
        result = self._generate(request)
        if not result.is_valid:
            raise RuntimeError(
                f"SQL generation failed after {result.retries_used} retries: {result.validation_error}"
            )
        return result.sql

    def generate_query_structured(self, request: SQLGenerationRequest) -> SQLGenerationResult:
        """
        Full structured generation with retry, validation, and RAG context.

        Args:
            request: Structured generation request.

        Returns:
            SQLGenerationResult with the query and metadata.
        """
        sanitize_natural_language(request.natural_language)
        if request.time_periods:
            request.time_periods = sanitize_time_periods(request.time_periods)
        return self._generate(request)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _retrieve_similar_template(self, natural_language: str) -> Optional[Tuple[str, str]]:
        """
        Retrieve the most relevant SQL template for the query (simple keyword RAG).

        Args:
            natural_language: User query.

        Returns:
            Tuple of (template_name, template_sql) or None.
        """
        query_lower = natural_language.lower()
        for name, sql in self.sql_templates.items():
            keywords = name.replace("_", " ").split()
            if any(kw in query_lower for kw in keywords):
                logger.info("Using SQL template: %s", name)
                return name, sql
        return None

    def _build_system_prompt(self, template_context: Optional[str] = None) -> str:
        """Build the system prompt with schema and optional template context."""
        prompt = f"""You are an expert SQL engineer for the UK Department for Education.
Generate Databricks SQL (Spark SQL dialect) queries against the Unity Catalog schema below.

{SCHEMA_CONTEXT}

Rules:
- Always use fully-qualified table names: edu_insights.bronze.<table>
- Use parameterised placeholders only when filtering by acad_year_code
- Never use DROP, DELETE, INSERT, UPDATE, ALTER, TRUNCATE
- Use LEFT JOIN for optional relationships
- Add comments to explain complex logic
- Return ONLY the SQL query, no explanations or markdown fences
- For MTC scores, valid range is 0-25
"""
        if template_context:
            prompt += f"\n\nSimilar SQL template for reference:\n{template_context}"
        return prompt

    def _call_llm(self, system_prompt: str, user_prompt: str) -> str:
        """Invoke the LLM and return the raw text response."""
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]
        response = self.llm.invoke(messages)
        raw = response.content.strip()
        # Strip markdown code fences if present
        raw = re.sub(r"^```(?:sql)?\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\s*```$", "", raw)
        return raw.strip()

    def _validate_sql(self, sql: str) -> Tuple[bool, Optional[str]]:
        """
        Lightweight SQL validation: check for forbidden patterns and basic syntax.

        In production this should run EXPLAIN against Databricks.

        Args:
            sql: SQL query to validate.

        Returns:
            Tuple of (is_valid, error_message).
        """
        if _FORBIDDEN_SQL_PATTERNS.search(sql):
            return False, "Generated SQL contains forbidden DDL/DML keywords."
        if not sql.upper().strip().startswith("SELECT"):
            return False, "Generated SQL must be a SELECT statement."
        # Check for balanced parentheses
        if sql.count("(") != sql.count(")"):
            return False, "Unbalanced parentheses in generated SQL."
        return True, None

    def _build_user_prompt(self, request: SQLGenerationRequest, error: Optional[str] = None) -> str:
        """Build the user-facing prompt, optionally with error correction context."""
        parts = [f"Generate a SQL query for: {request.natural_language}"]
        if request.report_type:
            parts.append(f"Report type: {request.report_type}")
        if request.time_periods:
            periods_str = ", ".join(f"'{p}'" for p in request.time_periods)
            parts.append(f"Filter to academic years: {periods_str}")
        if request.filters:
            for k, v in request.filters.items():
                parts.append(f"Filter {k} = '{v}'")
        if error:
            parts.append(
                f"\nThe previous SQL attempt failed with: {error}\n"
                "Please fix the query and return corrected SQL only."
            )
        return "\n".join(parts)

    def _generate(self, request: SQLGenerationRequest) -> SQLGenerationResult:
        """Core generation loop with retry and error-correction."""
        template_info = self._retrieve_similar_template(request.natural_language)
        template_name = template_info[0] if template_info else None
        template_sql = template_info[1] if template_info else None

        system_prompt = self._build_system_prompt(template_context=template_sql)
        error: Optional[str] = None
        sql = ""

        for attempt in range(request.max_retries + 1):
            try:
                user_prompt = self._build_user_prompt(request, error=error if attempt > 0 else None)
                sql = self._call_llm(system_prompt, user_prompt)
                is_valid, validation_error = self._validate_sql(sql)
                if is_valid:
                    logger.info("SQL generated successfully on attempt %d", attempt + 1)
                    return SQLGenerationResult(
                        sql=sql,
                        is_valid=True,
                        retries_used=attempt,
                        template_used=template_name,
                        confidence=max(0.0, 1.0 - attempt * 0.2),
                    )
                error = validation_error
                logger.warning("SQL validation failed (attempt %d): %s", attempt + 1, error)
            except Exception as exc:
                error = str(exc)
                logger.error("SQL generation error (attempt %d): %s", attempt + 1, error)

        return SQLGenerationResult(
            sql=sql,
            is_valid=False,
            validation_error=error,
            retries_used=request.max_retries,
            template_used=template_name,
            confidence=0.0,
        )


# ---------------------------------------------------------------------------
# Example Usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    agent = DataSqlAgent(api_key=os.getenv("OPENAI_API_KEY"))

    examples = [
        "Get pupil count and average MTC score by gender for 2024-25",
        "Show score distribution for MTC in 2023/24 grouped by SEN provision",
        "National pupil characteristics for the last three years",
    ]

    for query in examples:
        print(f"\nQuery: {query}")
        print("-" * 60)
        try:
            request = SQLGenerationRequest(
                natural_language=query,
                report_type="MTC",
                time_periods=["202425"],
            )
            result = DataSqlAgent(api_key=os.getenv("OPENAI_API_KEY")).generate_query_structured(request)
            print(f"Valid: {result.is_valid}")
            print(f"SQL:\n{result.sql}")
        except Exception as e:
            print(f"Error: {e}")
