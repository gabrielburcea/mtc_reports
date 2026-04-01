"""
Orchestrator Agent - Natural Language Query Understanding & Task Routing
=========================================================================
Purpose: Entry point for all user requests. Parses intent and routes to specialized agents.

Key Responsibilities:
1. Parse natural language queries
2. Extract structured parameters (report type, time periods, filters)
3. Route to appropriate specialized agents
4. Handle multi-step workflows
5. Return unified responses to users

Technology:
- LLM: GPT-4o or Databricks DBRX Instruct
- Framework: LangGraph for stateful orchestration
- Tools: Function calling for structured output
"""

import json
import logging
from typing import Dict, List, Optional, TypedDict, Annotated
from datetime import datetime
from enum import Enum

# LangChain/LangGraph imports
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI  # or use Databricks LLM
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from pydantic import BaseModel, Field

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# DATA MODELS
# ============================================================================

class Intent(str, Enum):
    """Possible user intents"""
    GENERATE_REPORT = "generate_report"
    EXPLAIN_REPORT = "explain_report"
    COMPARE_PERIODS = "compare_periods"
    ADD_COMMENTARY = "add_commentary"
    LIST_REPORTS = "list_reports"
    CHECK_STATUS = "check_status"
    UNKNOWN = "unknown"


class ReportType(str, Enum):
    """Available report types"""
    MTC = "MTC"
    KS2 = "KS2"
    ATTENDANCE = "Attendance"
    EXCLUSIONS = "Exclusions"
    UNKNOWN = "unknown"


class ParsedQuery(BaseModel):
    """Structured output from query parsing"""
    intent: Intent = Field(description="User's primary intent")
    report_type: Optional[ReportType] = Field(default=None, description="Type of report requested")
    report_subtype: Optional[str] = Field(default=None, description="Specific report variant (e.g., 'pupil_characteristics')")
    time_periods: List[str] = Field(default_factory=list, description="Academic years in YYYYMM format (e.g., ['202324', '202425'])")
    filters: Dict[str, str] = Field(default_factory=dict, description="Additional filters (region, demographic, etc.)")
    natural_language_query: str = Field(description="Original user query")
    confidence: float = Field(default=0.0, description="Confidence score 0-1")
    clarification_needed: Optional[str] = Field(default=None, description="What needs clarification, if any")


class OrchestratorState(TypedDict):
    """Shared state across orchestrator workflow"""
    user_query: str
    parsed_query: Optional[ParsedQuery]
    context: Dict
    next_agent: Optional[str]
    final_response: Optional[str]
    errors: List[str]


# ============================================================================
# ORCHESTRATOR AGENT
# ============================================================================

class OrchestratorAgent:
    """
    Main orchestrator that understands user requests and coordinates specialized agents
    """
    
    def __init__(self,
        llm_model: str = "gpt-4o",
        api_key: Optional[str] = None,
        metadata_connection: Optional[object] = None,
        temperature: float = 0.1
    ):
        """
        Initialize the Orchestrator Agent
        
        Args:
            llm_model: Model name (gpt-4o, dbrx-instruct, etc.)
            api_key: API key for LLM provider
            metadata_connection: Connection to metadata repository
            temperature: LLM sampling temperature (lower = more deterministic)
        """
        self.llm = ChatOpenAI(
            model=llm_model,
            temperature=temperature,
            api_key=api_key
        )
        
        self.metadata_connection = metadata_connection
        
        # Load available reports from metadata
        self.available_reports = self._load_available_reports()
        self.available_time_periods = self._load_available_time_periods()
        
        # Build the orchestration graph
        self.graph = self._build_graph()
        
        logger.info(f"Orchestrator Agent initialized with model: {llm_model}")
    
    
    def _load_available_reports(self) -> List[Dict]:
        """
        Load available report definitions from metadata repository
        Returns mock data if no connection
        """
        if self.metadata_connection:
            # TODO: Query report_registry table
            pass
        
        # Mock data for development
        return [
            {
                "report_id": "MTC_NATIONAL_PUPIL_CHARS_V1",
                "report_name": "MTC National Pupil Characteristics",
                "report_type": "MTC",
                "report_subtype": "national_pupil_characteristics",
                "keywords": ["mtc", "pupil", "characteristics", "national", "demographics"]
            },
            {
                "report_id": "MTC_REGIONAL_LA_PUPIL_CHARS_V1",
                "report_name": "MTC Regional and LA Pupil Characteristics",
                "report_type": "MTC",
                "report_subtype": "regional_la_pupil_characteristics",
                "keywords": ["mtc", "regional", "local authority", "pupil", "characteristics"]
            },
            {
                "report_id": "MTC_NATIONAL_SCHOOL_CHARS_V1",
                "report_name": "MTC National School Characteristics",
                "report_type": "MTC",
                "report_subtype": "national_school_characteristics",
                "keywords": ["mtc", "school", "characteristics", "establishment"]
            },
            {
                "report_id": "MTC_SCORE_DIST_PUPIL_V1",
                "report_name": "MTC Score Distribution by Pupil Characteristics",
                "report_type": "MTC",
                "report_subtype": "score_distribution_pupil",
                "keywords": ["mtc", "score", "distribution", "pupil"]
            },
            {
                "report_id": "MTC_SCORE_DIST_SCHOOL_V1",
                "report_name": "MTC Score Distribution by School Characteristics",
                "report_type": "MTC",
                "report_subtype": "score_distribution_school",
                "keywords": ["mtc", "score", "distribution", "school"]
            }
        ]
    
    
    def _load_available_time_periods(self) -> List[str]:
        """
        Load available time periods from data
        Returns mock data if no connection
        """
        if self.metadata_connection:
            # TODO: Query distinct time_period from source tables
            pass
        
        # Mock data
        return ["202122", "202223", "202324", "202425", "202526"]
    
    
    def _build_system_prompt(self) -> str:
        """Build context-aware system prompt"""
        
        reports_context = "\n".join([
            f"- {r['report_name']} (type: {r['report_type']}, subtype: {r['report_subtype']})"
            for r in self.available_reports
        ])
        
        periods_context = ", ".join(self.available_time_periods)
        
        return f"""
You are an intelligent orchestrator for the UK Department for Education's Educational Insights Platform.

Your role is to understand user requests about educational data and reports, then extract structured information.

# AVAILABLE REPORTS
{reports_context}

# AVAILABLE TIME PERIODS
Academic years: {periods_context}
Format: YYYYMM (e.g., 202324 = 2023/24 academic year)

# YOUR TASK
Parse the user's query and extract:
1. **Intent**: What does the user want to do?
   - generate_report: Create a new report
   - explain_report: Understand an existing report's data
   - compare_periods: Compare data across years
   - add_commentary: Generate insights/commentary
   - list_reports: Show available reports
   - check_status: Check if a report exists
   
2. **Report Type**: Which domain? (MTC, KS2, Attendance, etc.)

3. **Report Subtype**: Which specific report? (pupil_characteristics, score_distribution, etc.)

4. **Time Periods**: Which academic years? Convert natural language to YYYYMM format.
   Examples:
   - "2024-25" → ["202425"]
   - "last three years" → ["202223", "202324", "202425"]
   - "2022 to 2025" → ["202223", "202324", "202425"]

5. **Filters**: Any additional constraints? (region, gender, SEND status, etc.)

6. **Confidence**: How confident are you in your parsing? (0.0 to 1.0)

7. **Clarification Needed**: If anything is ambiguous, note what needs clarification.

# GUIDELINES
- Be precise with time period conversions
- If the user mentions "MTC", they likely mean the Multiplication Tables Check
- If unsure about report subtype, ask for clarification
- Default to the most recent year if not specified
- Preserve any specific filters mentioned (e.g., "for girls", "in London", "SEND pupils")

Return your analysis as structured JSON matching the ParsedQuery schema."""
    
    
    def parse_query(self, user_query: str) -> ParsedQuery:
        """
        Parse natural language query into structured format
        
        Args:
            user_query: Raw user input
            
        Returns:
            ParsedQuery object with extracted information
        """
        system_prompt = self._build_system_prompt()
        
        # Use structured output (function calling)
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"User query: {user_query}")
        ]
        
        # Use with_structured_output for guaranteed schema compliance
        structured_llm = self.llm.with_structured_output(ParsedQuery)
        
        try:
            parsed = structured_llm.invoke(messages)
            logger.info(f"Parsed query: {parsed}")
            return parsed
            
        except Exception as e:
            logger.error(f"Query parsing failed: {e}")
            return ParsedQuery(
                intent=Intent.UNKNOWN,
                natural_language_query=user_query,
                confidence=0.0,
                clarification_needed=f"Failed to parse query: {str(e)}"
            )
    
    
    def route_to_agent(self, parsed_query: ParsedQuery) -> str:
        """
        Determine which specialized agent should handle this request
        
        Args:
            parsed_query: Parsed user intent
            
        Returns:
            Name of the agent to invoke
        """
        intent = parsed_query.intent
        
        routing_map = {
            Intent.GENERATE_REPORT: "report_generation_agent",
            Intent.EXPLAIN_REPORT: "commentary_agent",
            Intent.COMPARE_PERIODS: "comparison_agent",
            Intent.ADD_COMMENTARY: "commentary_agent",
            Intent.LIST_REPORTS: "metadata_agent",
            Intent.CHECK_STATUS: "metadata_agent",
            Intent.UNKNOWN: "clarification_agent"
        }
        
        next_agent = routing_map.get(intent, "clarification_agent")
        logger.info(f"Routing intent '{intent}' to agent '{next_agent}'")
        
        return next_agent
    
    
    def generate_clarification(self, parsed_query: ParsedQuery) -> str:
        """
        Generate a clarification question for the user
        
        Args:
            parsed_query: Partially parsed query
            
        Returns:
            Clarification question as string
        """
        if parsed_query.clarification_needed:
            return parsed_query.clarification_needed
        
        # Build clarification based on what's missing
        missing_info = []
        
        if not parsed_query.report_type or parsed_query.report_type == ReportType.UNKNOWN:
            missing_info.append("which report type (MTC, KS2, Attendance, etc.)")
        
        if not parsed_query.report_subtype:
            # Suggest options based on report type
            if parsed_query.report_type == ReportType.MTC:
                subtypes = [r['report_subtype'] for r in self.available_reports if r['report_type'] == 'MTC']
                missing_info.append(f"which specific report: {', '.join(subtypes)}")
        
        if not parsed_query.time_periods:
            missing_info.append("which academic year(s)")
        
        if missing_info:
            return f"I need clarification on: {'; '.join(missing_info)}. Could you provide more details?"
        
        return "I'm not sure I understood your request. Could you rephrase it?"
    
    
    # ========================================================================
    # LANGGRAPH WORKFLOW NODES
    # ========================================================================
    
    def parse_node(self, state: OrchestratorState) -> OrchestratorState:
        """LangGraph node: Parse user query"""
        logger.info("Executing parse_node")
        
        parsed = self.parse_query(state["user_query"])
        state["parsed_query"] = parsed
        
        # Check if clarification is needed
        if parsed.confidence < 0.7 or parsed.clarification_needed:
            state["next_agent"] = "clarification"
        else:
            state["next_agent"] = self.route_to_agent(parsed)
        
        return state
    
    
    def clarification_node(self, state: OrchestratorState) -> OrchestratorState:
        """LangGraph node: Generate clarification"""
        logger.info("Executing clarification_node")
        
        parsed = state["parsed_query"]
        clarification = self.generate_clarification(parsed)
        
        state["final_response"] = clarification
        state["next_agent"] = None  # End workflow
        
        return state
    
    
    def route_node(self, state: OrchestratorState) -> OrchestratorState:
        """LangGraph node: Route to appropriate agent"""
        logger.info(f"Executing route_node -> {state['next_agent']}")
        
        # In a full implementation, this would invoke the actual specialized agent
        # For now, just prepare the routing information
        
        parsed = state["parsed_query"]
        agent_name = state["next_agent"]
        
        state["context"] = {
            "target_agent": agent_name,
            "parsed_query": parsed.dict(),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        state["final_response"] = (
            f"✓ Query understood!\n"
            f"Intent: {parsed.intent.value}\n"
            f"Report: {parsed.report_type.value if parsed.report_type else 'N/A'} - {parsed.report_subtype or 'N/A'}\n"
            f"Time periods: {', '.join(parsed.time_periods) if parsed.time_periods else 'N/A'}\n"
            f"Routing to: {agent_name}\n"
            f"Confidence: {parsed.confidence:.0%}"
        )
        
        return state
    
    
    def _build_graph(self) -> StateGraph:
        """
        Build the LangGraph orchestration workflow.

        Workflow:
        1. Parse query
        2. If low confidence -> Clarification
        3. If high confidence -> Route to specialized agent
        """
        workflow = StateGraph(OrchestratorState)
        
        # Add nodes
        workflow.add_node("parse", self.parse_node)
        workflow.add_node("clarification", self.clarification_node)
        workflow.add_node("route", self.route_node)
        
        # Define edges
        workflow.set_entry_point("parse")
        
        # Conditional routing after parse
        workflow.add_conditional_edges(
            "parse",
            lambda state: state["next_agent"],
            {
                "clarification": "clarification",
                "report_generation_agent": "route",
                "commentary_agent": "route",
                "comparison_agent": "route",
                "metadata_agent": "route",
            }
        )
        
        # Both clarification and route lead to END
        workflow.add_edge("clarification", END)
        workflow.add_edge("route", END)
        
        return workflow.compile()
    
    
    def process_query(self, user_query: str, context: Optional[Dict] = None) -> Dict:
        """
        Main entry point: Process a user query end-to-end.

        Args:
            user_query: Natural language input from user
            context: Optional additional context (user info, session data, etc.)

        Returns:
            Dict with response and metadata
        """
        logger.info(f"Processing query: {user_query}")
        
        initial_state: OrchestratorState = {
            "user_query": user_query,
            "parsed_query": None,
            "context": context or {},
            "next_agent": None,
            "final_response": None,
            "errors": []
        }
        
        try:
            # Execute the graph
            final_state = self.graph.invoke(initial_state)
            
            return {
                "status": "success",
                "response": final_state["final_response"],
                "parsed_query": final_state["parsed_query"].dict() if final_state["parsed_query"] else None,
                "next_agent": final_state["next_agent"],
                "context": final_state["context"]
            }
            
        except Exception as e:
            logger.error(f"Orchestrator failed: {e}", exc_info=True)
            return {
                "status": "error",
                "response": f"I encountered an error processing your request: {str(e)}",
                "error": str(e)
            }


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Initialize agent (requires OpenAI API key)
    import os
    
    agent = OrchestratorAgent(
        llm_model="gpt-4o",
        api_key=os.getenv("OPENAI_API_KEY")
    )
    
    # Test queries
    test_queries = [
        "Generate MTC pupil characteristics for 2024-25",
        "Show me MTC results for the last three years",
        "Compare girls vs boys performance in MTC 2023/24",
        "What reports do you have available?",
        "Create a report",  # Should ask for clarification
    ]
    
    print("=" * 80)
    print("ORCHESTRATOR AGENT TEST")
    print("=" * 80)
    
    for query in test_queries:
        print(f"\n📝 Query: {query}")
        print("-" * 80)
        
        result = agent.process_query(query)
        
        print(f"Status: {result['status']}")
        print(f"\nResponse:\n{result['response']}")
        
        if result.get('parsed_query'):
            print(f"\nParsed Query:\n{json.dumps(result['parsed_query'], indent=2)}")
        
        print("\n" + "=" * 80)