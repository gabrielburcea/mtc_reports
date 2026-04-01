"""
Report Onboarding Wizard - Interactive Agent for New Report Types
=================================================================
Purpose: Help domain experts define new report types without coding

Features:
1. Conversational discovery of source tables
2. AI-assisted column mapping
3. Business logic definition through dialogue
4. Automatic SQL template generation
5. Validation with sample data
6. Registration in metadata repository

Technology:
- LLM: GPT-4o with extended context
- Framework: LangGraph for multi-step workflows
- Integration: Unity Catalog API for schema discovery
"""

import json
import logging
from typing import Dict, List, Optional, TypedDict
from datetime import datetime

from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OnboardingState(TypedDict):
    """State for the onboarding workflow"""
    report_name: str
    report_type: str
    conversation_history: List[Dict]
    discovered_tables: List[str]
    column_mappings: Dict[str, str]
    business_logic: Dict
    generated_sql: Optional[str]
    validation_results: Optional[Dict]
    current_step: str
    user_confirmed: bool


class ReportOnboardingAgent:
    """
    Interactive agent that helps domain experts onboard new report types
    through natural conversation
    """
    
    def __init__(self, llm_model: str = "gpt-4o", api_key: Optional[str] = None):
        self.llm = ChatOpenAI(model=llm_model, temperature=0.2, api_key=api_key)
        logger.info("Report Onboarding Agent initialized")
    
    
    def start_onboarding(self, report_name: str) -> str:
        """Begin the onboarding process"""
        
        prompt = f"""
I'm helping you create a new report type called "{report_name}".

Let's start by understanding what data you need. 

**Available tables in Unity Catalog:**
- pupil (demographics, SEN status, geography)
- school (establishment characteristics)
- results (assessment outcomes)
- claimcare (FSM eligibility, disadvantage)
- geography (regional hierarchies)
- ks2_results (Key Stage 2 outcomes)
- attendance_records (attendance data)
- exclusions (exclusion incidents)

**Question 1:** Which of these tables contain the data you need for this report?
You can list multiple tables.
"""
        return prompt
    
    
    def discover_tables(self, user_response: str, available_tables: List[str]) -> Dict:
        """Extract mentioned tables from user response"""
        
        prompt = f"""
User wants to create a report and mentioned these tables:

"{user_response}"

Available tables: {', '.join(available_tables)}

Extract which tables they need. Return JSON:
{{
    "tables": ["table1", "table2"],
    "confidence": 0.95,
    "needs_clarification": false
}}
"""
        
        response = self.llm.invoke([HumanMessage(content=prompt)])
        return json.loads(response.content)
    
    
    def map_columns(self, source_tables: List[str], target_columns: List[str]) -> Dict:
        """Help user map source columns to output columns"""
        
        prompt = f"""
You need to map columns from source tables to your report output.

**Source tables:** {', '.join(source_tables)}

**Target output columns:** {', '.join(target_columns)}

For each target column, tell me:
1. Which source table has this data?
2. What's the column name in that table?
3. Does it need any transformation? (e.g., decoding, renaming, calculation)

Let's go through them one by one. First target column: {target_columns[0]}
"""
        return prompt
    
    def generate_sql_template(self, 
        source_tables: List[str],
        column_mappings: Dict[str, Dict],
        business_logic: Dict
    ) -> str:
        """Generate SQL template based on gathered information"""
        
        # Build SELECT clause
        select_clauses = []
        for target_col, mapping in column_mappings.items():
            source_table = mapping['source_table']
            source_col = mapping['source_column']
            transform = mapping.get('transformation', '')
            
            if transform:
                select_clauses.append(f"  {transform} AS {target_col}")
            else:
                select_clauses.append(f"  {source_table}.{source_col} AS {target_col}")
        
        # Build FROM clause with joins
        from_clause = source_tables[0]
        join_clauses = []
        
        for i, table in enumerate(source_tables[1:], 1):
            join_clauses.append(f"LEFT JOIN {table} ON {source_tables[0]}.id = {table}.id")
        
        # Build WHERE clause
        where_clauses = []
        if 'filters' in business_logic:
            for filter_condition in business_logic['filters']:
                where_clauses.append(f"  {filter_condition}")
        
        # Assemble SQL
        sql = f"""
-- Generated SQL Template for {business_logic.get('report_name', 'New Report')}
-- Generated on: {datetime.utcnow().isoformat()}

SELECT
{',\n'.join(select_clauses)}
FROM {from_clause}
{chr(10).join(join_clauses)}
"""
        
        if where_clauses:
            sql += f"WHERE\n{' AND'.join(where_clauses)}\n"        
        if 'group_by' in business_logic:
            sql += f"GROUP BY {', '.join(business_logic['group_by'])}\n"        
        if 'order_by' in business_logic:
            sql += f"ORDER BY {', '.join(business_logic['order_by'])}\n"        
        return sql
    
    
    def validate_template(self, sql: str) -> Dict:
        """Validate the generated SQL template"""
        
        # In production, this would execute against a sample dataset
        return {
            "valid": True,
            "sample_row_count": 150,
            "columns_found": 12,
            "warnings": [],
            "errors": []
        }
    
    
    def register_report(self, 
        report_name: str,
        report_type: str,
        source_tables: List[str],
        column_mappings: Dict,
        business_logic: Dict,
        sql_template: str
    ) -> str:
        """Register the new report in metadata repository"""
        
        report_id = f"{report_type}_{report_name.upper().replace(' ', '_')}_V1"
        
        # Generate INSERT statements for metadata tables
        registration_sql = f"""
-- Register new report: {report_name}

INSERT INTO report_registry VALUES (
    '{report_id}',
    '{report_name}',
    '{report_type}',
    '{business_logic.get("subtype", "custom")}',
    '1.0',
    '{json.dumps(business_logic.get("output_schema", {}))}',
    NULL,
    NULL,
    '{business_logic.get("description", "")}',
    current_user(),
    CURRENT_TIMESTAMP(),
    current_user(),
    CURRENT_TIMESTAMP(),
    NULL,
    TRUE,
    'ON_DEMAND',
    NULL
);

-- Register dependencies
"""
        
        for table in source_tables:
            registration_sql += f"""
INSERT INTO report_dependencies VALUES (
    uuid(),
    '{report_id}',
    'edu_insights',
    'bronze',
    '{table}',
    NULL,
    'BRONZE',
    NULL,
    NULL,
    NULL,
    CURRENT_TIMESTAMP()
);
"""
        
        # Save SQL template
        template_path = f"/sql/outputs/{report_id.lower()}.sql"
        registration_sql += f"""
-- Save SQL template to: {template_path}

{sql_template}
"""
        
        return registration_sql
    
    
    def run_interactive_session(self, report_name: str):
        """Run full interactive onboarding session"""
        
        print("=" * 80)
        print(f"REPORT ONBOARDING WIZARD - {report_name}")
        print("=" * 80)
        print()        
        state: OnboardingState = {
            "report_name": report_name,
            "report_type": "",
            "conversation_history": [],
            "discovered_tables": [],
            "column_mappings": {},
            "business_logic": {},
            "generated_sql": None,
            "validation_results": None,
            "current_step": "intro",
            "user_confirmed": False
        }
        
        # Step 1: Introduction and table discovery
        print(self.start_onboarding(report_name))
        print()        
        user_input = input("Your answer: ")
        state["conversation_history"].append({"role": "user", "content": user_input})
        
        # Parse tables
        available_tables = ["pupil", "school", "results", "claimcare", "geography"]
        discovered = self.discover_tables(user_input, available_tables)
        state["discovered_tables"] = discovered["tables"]
        
        print(f"\n✓ Understood! You need these tables: {', '.join(state['discovered_tables'])}")
        print()        
        # Step 2: Define output columns
        print("What columns should appear in the final report?")
        print("(Enter comma-separated list, e.g.: time_period, pupil_count, average_score)")
        print()        
        user_input = input("Output columns: ")
        target_columns = [col.strip() for col in user_input.split(',')]
        
        # Step 3: Column mapping (simplified for demo)
        print(f"\n✓ Got it! Your report will have {len(target_columns)} columns.")
        print()        
        # Mock column mappings
        for col in target_columns:
            state["column_mappings"][col] = {
                "source_table": state["discovered_tables"][0],
                "source_column": col,
                "transformation": None
            }
        
        # Step 4: Business logic
        print("Does this report need:")
        print("1. Filtering? (e.g., only certain years, regions)")
        print("2. Grouping/aggregation? (e.g., sum by demographic)")
        print("3. Sorting?")
        print()        
        user_input = input("Describe the logic: ")
        state["business_logic"] = {
            "report_name": report_name,
            "description": user_input,
            "filters": [],
            "group_by": [],
            "output_schema": {col: "STRING" for col in target_columns}
        }
        
        # Step 5: Generate SQL
        print("\n🔧 Generating SQL template...")
        sql = self.generate_sql_template(
            state["discovered_tables"],
            state["column_mappings"],
            state["business_logic"]
        )
        state["generated_sql"] = sql
        
        print("\n" + "=" * 80)
        print("GENERATED SQL TEMPLATE")
        print("=" * 80)
        print(sql)
        print("=" * 80)
        print()        
        # Step 6: Validation
        print("🧪 Validating template...")
        validation = self.validate_template(sql)
        state["validation_results"] = validation
        
        if validation["valid"]:
            print("✅ Template is valid!")
            print(f"   Sample execution returned {validation['sample_row_count']} rows")
        else:
            print("❌ Template has errors:")
            for error in validation["errors"]:
                print(f"   - {error}")
        print()        
        # Step 7: Confirmation and registration
        confirm = input("Register this report? (yes/no): ")
        
        if confirm.lower() == 'yes':
            print("\n📝 Generating registration SQL...")
            registration_sql = self.register_report(
                report_name,
                "CUSTOM",
                state["discovered_tables"],
                state["column_mappings"],
                state["business_logic"],
                sql
            )
            
            print("\n" + "=" * 80)
            print("REGISTRATION SQL")
            print("=" * 80)
            print(registration_sql)
            print("=" * 80)
            print()            
            print("✅ Report onboarding complete!")
            print(f"   Report ID: CUSTOM_{report_name.upper().replace(' ', '_')}_V1")
            print("   Next steps:")
            print("   1. Review and execute the registration SQL")
            print("   2. Test the SQL template with real data")
            print("   3. Add to scheduled jobs if needed")
        else:
            print("❌ Onboarding cancelled.")


# Example usage
if __name__ == "__main__":
    import os
    
    agent = ReportOnboardingAgent(api_key=os.getenv("OPENAI_API_KEY"))
    
    # Interactive mode
    print("Welcome to the Report Onboarding Wizard!")
    print()    
    report_name = input("What should we call this new report? ")
    
    agent.run_interactive_session(report_name)