"""
Commentary Agent - DfE-Style Narrative Insights Generation
===========================================================
Purpose: Generate written commentary from educational report data,
following DfE publication style guidelines.

Key Responsibilities:
1. Statistical analysis (trends, gaps, year-on-year comparisons)
2. Gender, SEND, FSM gap analysis
3. Regional variation detection
4. Natural language narrative generation
5. Year-over-year comparison logic
"""

import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

class CommentaryRequest(BaseModel):
    """Input for commentary generation."""

    report_type: str = Field(description="Report type, e.g. MTC")
    report_subtype: str = Field(description="Report subtype, e.g. national_pupil_characteristics")
    time_periods: List[str] = Field(description="Academic year codes present in the data")
    data_summary: Dict[str, Any] = Field(description="Pre-computed statistical summary")
    focus_areas: List[str] = Field(
        default_factory=lambda: ["overall_trend", "gender_gap", "send_gap", "fsm_gap"],
        description="Which areas to focus commentary on",
    )
    style: str = Field(default="dfe_statistical_release", description="Writing style")


class CommentaryResult(BaseModel):
    """Output from commentary generation."""

    headline: str = Field(description="One-sentence headline finding")
    key_findings: List[str] = Field(description="Bullet-point key findings")
    detailed_commentary: str = Field(description="Full narrative commentary")
    statistical_highlights: Dict[str, Any] = Field(
        default_factory=dict, description="Key statistics referenced in commentary"
    )
    data_quality_notes: List[str] = Field(
        default_factory=list, description="Notes about data quality or limitations"
    )


# ---------------------------------------------------------------------------
# Statistical Analysis Engine
# ---------------------------------------------------------------------------

class StatisticalAnalyser:
    """Computes statistics from a pandas DataFrame for commentary generation."""

    @staticmethod
    def compute_summary(df: pd.DataFrame, value_col: str = "average_score") -> Dict[str, Any]:
        """
        Compute a statistical summary suitable for passing to the LLM.

        Args:
            df: Report DataFrame (must include time_period column).
            value_col: Name of the numeric value column to analyse.

        Returns:
            Dict of summary statistics.
        """
        summary: Dict[str, Any] = {}

        if df.empty or value_col not in df.columns:
            return summary

        # Overall stats
        summary["overall_mean"] = round(float(df[value_col].mean()), 2)
        summary["overall_min"] = round(float(df[value_col].min()), 2)
        summary["overall_max"] = round(float(df[value_col].max()), 2)

        # Year-on-year trend
        if "time_period" in df.columns:
            by_year = (
                df.groupby("time_period")[value_col]
                .mean()
                .sort_index()
            )
            summary["by_year"] = {str(k): round(float(v), 2) for k, v in by_year.items()}
            if len(by_year) >= 2:
                years = list(by_year.index)
                latest = by_year.iloc[-1]
                previous = by_year.iloc[-2]
                change = round(float(latest - previous), 2)
                pct_change = round(float((latest - previous) / previous * 100), 1) if previous != 0 else 0.0
                summary["yoy_change"] = change
                summary["yoy_pct_change"] = pct_change
                summary["latest_year"] = str(years[-1])
                summary["previous_year"] = str(years[-2])

        # Gender gap
        if "sex" in df.columns:
            gender_means = df.groupby("sex")[value_col].mean()
            summary["gender_gap"] = {
                str(k): round(float(v), 2) for k, v in gender_means.items()
            }
            if "Boys" in gender_means and "Girls" in gender_means:
                summary["gender_gap_pp"] = round(
                    float(gender_means["Girls"] - gender_means["Boys"]), 2
                )

        # SEND gap
        if "sen_provision" in df.columns:
            send_means = df.groupby("sen_provision")[value_col].mean()
            summary["send_gap"] = {
                str(k): round(float(v), 2) for k, v in send_means.items()
            }

        # FSM gap
        if "fsm_status" in df.columns:
            fsm_means = df.groupby("fsm_status")[value_col].mean()
            summary["fsm_gap"] = {
                str(k): round(float(v), 2) for k, v in fsm_means.items()
            }
            if "FSM eligible" in fsm_means and "Not known to be FSM eligible" in fsm_means:
                summary["fsm_gap_pp"] = round(
                    float(
                        fsm_means["Not known to be FSM eligible"]
                        - fsm_means["FSM eligible"]
                    ),
                    2,
                )

        # Regional variation
        if "region_name" in df.columns:
            region_means = df.groupby("region_name")[value_col].mean()
            summary["regional_range"] = round(
                float(region_means.max() - region_means.min()), 2
            )
            summary["highest_region"] = str(region_means.idxmax())
            summary["lowest_region"] = str(region_means.idxmin())

        return summary

    @staticmethod
    def detect_trends(by_year: Dict[str, float]) -> str:
        """
        Detect the overall trend direction from year-keyed values.

        Args:
            by_year: Dict mapping year strings to float values.

        Returns:
            'increasing', 'decreasing', 'stable', or 'mixed'.
        """
        if len(by_year) < 2:
            return "stable"
        values = [v for _, v in sorted(by_year.items())]
        diffs = [values[i + 1] - values[i] for i in range(len(values) - 1)]
        positives = sum(1 for d in diffs if d > 0.05)
        negatives = sum(1 for d in diffs if d < -0.05)
        if positives == len(diffs):
            return "increasing"
        if negatives == len(diffs):
            return "decreasing"
        if positives == 0 and negatives == 0:
            return "stable"
        return "mixed"


# ---------------------------------------------------------------------------
# Commentary Agent
# ---------------------------------------------------------------------------

class CommentaryAgent:
    """
    Generates DfE-style narrative commentary from educational report data.
    """

    _DFE_STYLE_GUIDE = """
You are an expert statistical writer for the UK Department for Education (DfE).
Your role is to produce clear, factual, and impartial commentary for statistical releases.

Style rules:
- Use plain English suitable for a general audience
- Lead with the most important finding
- Use precise percentages and percentage-point differences
- Avoid jargon; define abbreviations on first use
- Use past tense for completed academic years
- Acknowledge limitations and data quality issues
- Do not speculate beyond the data
- Structure: headline -> key findings -> detailed paragraphs
- Refer to 'pupils' not 'students' for primary/KS2 context
- MTC = Multiplication Tables Check (define on first use)
"""

    def __init__(
        self,
        llm_model: str = "gpt-4o",
        api_key: Optional[str] = None,
        temperature: float = 0.2,
    ) -> None:
        """
        Initialise the Commentary Agent.

        Args:
            llm_model: OpenAI model identifier.
            api_key: OpenAI API key (falls back to OPENAI_API_KEY env var).
            temperature: Sampling temperature.
        """
        self.llm = ChatOpenAI(
            model=llm_model,
            temperature=temperature,
            api_key=api_key or os.getenv("OPENAI_API_KEY"),
        )
        self.analyser = StatisticalAnalyser()
        logger.info("CommentaryAgent initialised with model: %s", llm_model)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_from_dataframe(
        self,
        df: pd.DataFrame,
        report_type: str,
        report_subtype: str,
        value_col: str = "average_score",
        focus_areas: Optional[List[str]] = None,
    ) -> CommentaryResult:
        """
        Generate commentary directly from a DataFrame.

        Args:
            df: Report DataFrame.
            report_type: e.g. 'MTC'.
            report_subtype: e.g. 'national_pupil_characteristics'.
            value_col: Column to use for statistical analysis.
            focus_areas: List of analysis areas to focus on.

        Returns:
            CommentaryResult with narrative and statistics.
        """
        summary = self.analyser.compute_summary(df, value_col=value_col)
        time_periods = sorted(df["time_period"].unique().tolist()) if "time_period" in df.columns else []

        request = CommentaryRequest(
            report_type=report_type,
            report_subtype=report_subtype,
            time_periods=time_periods,
            data_summary=summary,
            focus_areas=focus_areas or ["overall_trend", "gender_gap", "send_gap", "fsm_gap"],
        )
        return self.generate(request)

    def generate(self, request: CommentaryRequest) -> CommentaryResult:
        """
        Generate structured commentary from a CommentaryRequest.

        Args:
            request: Structured commentary request.

        Returns:
            CommentaryResult.
        """
        logger.info("Generating commentary for %s/%s", request.report_type, request.report_subtype)

        user_prompt = self._build_user_prompt(request)
        try:
            structured_llm = self.llm.with_structured_output(CommentaryResult)
            result: CommentaryResult = structured_llm.invoke(
                [
                    SystemMessage(content=self._DFE_STYLE_GUIDE),
                    HumanMessage(content=user_prompt),
                ]
            )
            logger.info("Commentary generated successfully")
            return result
        except Exception as exc:
            logger.error("Commentary generation failed: %s", exc)
            return self._fallback_commentary(request)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_user_prompt(self, request: CommentaryRequest) -> str:
        """Build the prompt sent to the LLM."""
        summary = request.data_summary
        lines = [
            f"Generate {request.style} commentary for {request.report_type} "
            f"({request.report_subtype}) covering years: {', '.join(request.time_periods)}.",
            "",
            "Statistical summary:",
        ]

        if "overall_mean" in summary:
            lines.append(f"- Overall average score: {summary['overall_mean']}")
        if "yoy_change" in summary:
            lines.append(
                f"- Year-on-year change ({summary.get('previous_year')} to {summary.get('latest_year')}): "
                f"{summary['yoy_change']:+.2f} ({summary.get('yoy_pct_change', 0):+.1f}%)"
            )
        if "gender_gap_pp" in summary:
            lines.append(f"- Gender gap (Girls minus Boys): {summary['gender_gap_pp']:+.2f} pp")
        if "fsm_gap_pp" in summary:
            lines.append(f"- FSM gap (non-FSM minus FSM): {summary['fsm_gap_pp']:+.2f} pp")
        if "regional_range" in summary:
            lines.append(
                f"- Regional range: {summary['regional_range']:.2f} pp "
                f"(highest: {summary.get('highest_region')}, lowest: {summary.get('lowest_region')})"
            )

        lines += [
            "",
            f"Focus areas: {', '.join(request.focus_areas)}",
            "",
            "Return a structured CommentaryResult with headline, key_findings (list), "
            "detailed_commentary (paragraphs), statistical_highlights (dict), "
            "and data_quality_notes (list).",
        ]
        return "\n".join(lines)

    def _fallback_commentary(self, request: CommentaryRequest) -> CommentaryResult:
        """Return a minimal commentary when LLM call fails."""
        summary = request.data_summary
        trend = "stable"
        if "by_year" in summary:
            trend = StatisticalAnalyser.detect_trends(summary["by_year"])

        headline = (
            f"MTC results for {', '.join(request.time_periods)} show a {trend} trend."
        )
        findings = []
        if "overall_mean" in summary:
            findings.append(f"Overall average score: {summary['overall_mean']}")
        if "gender_gap_pp" in summary:
            findings.append(f"Gender gap: {summary['gender_gap_pp']:+.2f} percentage points")
        if "fsm_gap_pp" in summary:
            findings.append(f"FSM disadvantage gap: {summary['fsm_gap_pp']:+.2f} percentage points")

        return CommentaryResult(
            headline=headline,
            key_findings=findings,
            detailed_commentary=headline,
            statistical_highlights=summary,
            data_quality_notes=["Commentary generated without LLM due to an error."],
        )


# ---------------------------------------------------------------------------
# Example Usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import numpy as np

    rng = np.random.default_rng(42)
    n = 200
    df = pd.DataFrame(
        {
            "time_period": rng.choice(["202324", "202425"], n),
            "sex": rng.choice(["Boys", "Girls"], n),
            "sen_provision": rng.choice(["No SEN provision", "SEN support / SEN without an EHC plan"], n),
            "fsm_status": rng.choice(["FSM eligible", "Not known to be FSM eligible"], n),
            "region_name": rng.choice(["London", "South East", "North West", "Yorkshire"], n),
            "average_score": rng.normal(19.5, 4.0, n).clip(0, 25),
        }
    )

    agent = CommentaryAgent(api_key=os.getenv("OPENAI_API_KEY"))
    result = agent.generate_from_dataframe(df, "MTC", "national_pupil_characteristics")
    print("Headline:", result.headline)
    print("\nKey Findings:")
    for f in result.key_findings:
        print(f"  - {f}")
    print("\nCommentary:")
    print(result.detailed_commentary)
