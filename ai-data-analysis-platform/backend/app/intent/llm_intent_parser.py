import json
import os
from typing import Optional, List, Dict, Any
import google.generativeai as genai
from dotenv import load_dotenv
import logging

from app.db.session import SessionLocal
from app.services.metadata_service import MetadataService
from app.intent.models import UserIntent
from app.intent.exceptions import IntentParsingError
from app.validator.metadata_validator import MetadataValidator

logger = logging.getLogger(__name__)
load_dotenv()

class GeminiIntentParser:
    """
    Data-agnostic parser that translates natural language to structured intent
    using dynamic schema injection and semantic rule enforcement.
    """
# gemini-2.0-flash-lite
    def __init__(self, api_key: Optional[str] = None, model: str = "gemini-3-flash-preview"):
        actual_key = api_key or os.getenv("GEMINI_API_KEY")
        if not actual_key:
            raise IntentParsingError("Gemini API Key is missing.")
        genai.configure(api_key=actual_key)
        self.model = genai.GenerativeModel(model)

    def parse(self, user_query: str, dataset_id: str) -> UserIntent:
        # 1. Fetch Schema Context dynamically
        schema_info = self._get_schema_context(dataset_id)
        prompt = self._build_generic_prompt(user_query, schema_info)

        try:
            response = self.model.generate_content(
                prompt,
                generation_config={
                    "temperature": 0,
                    "response_mime_type": "application/json",
                },
            )

            intent_dict = json.loads(response.text)
            
            # 2. Post-Processing & Normalization
            intent_dict["dataset_id"] = dataset_id
            intent_dict.setdefault("raw_query", user_query)
            
            normalized_intent = self._refine_intent_logic(intent_dict, dataset_id)
            
            print(f"[DEBUG] Final Parsed Intent: {normalized_intent}")
            return UserIntent(**normalized_intent)

        except Exception as e:
            logger.exception("Intent Parsing failed")
            raise IntentParsingError(f"Failed to parse query: {str(e)}")

    def _get_schema_context(self, dataset_id: str) -> Dict[str, Any]:
        """Dynamically extracts schema without hardcoding column names."""
        db = SessionLocal()
        metadata = MetadataService.get_dataset_metadata(db, dataset_id)
        db.close()

        columns_desc = []
        categorical_cols = []
        numeric_cols = []

        for table in metadata.get("tables", []):
            for col in table.get("columns", []):
                name = col['name']
                sem_type = col['semantic_type']
                
                # Filter out junk columns
                if name.lower().startswith("unnamed"):
                    continue
                    
                columns_desc.append(f"- {name} (Type: {sem_type})")
                if sem_type == "categorical":
                    categorical_cols.append(name)
                elif sem_type == "number":
                    numeric_cols.append(name)

        return {
            "full_desc": "\n".join(columns_desc),
            "categorical": categorical_cols,
            "numeric": numeric_cols
        }

    def _refine_intent_logic(self, intent_dict: Dict, dataset_id: str) -> Dict:
        """Applies data-agnostic guardrails to the LLM output."""
        validator = MetadataValidator(dataset_id)
        
        # 1. Capture useful schema groups from validator
        numeric_cols = [c for c, m in validator.columns.items() if m["semantic_type"] == "number"]
        all_cols = list(validator.columns.keys())
    
        # 2. Fix Measures (The fix for your Pydantic Error)
        function_map = {"average": "avg", "mean": "avg", "total": "sum", "count": "count"}
        
        refined_measures = []
        for m in intent_dict.get("measures", []):
            func = function_map.get(m.get("function", "").lower(), m.get("function", "count"))
            col = m.get("column")
    
            # FIX: If column is missing or null, provide a logical fallback
            if not col or col == "None":
                if func == "count":
                    col = "*"
                elif numeric_cols:
                    # Fallback to the first numeric column if they asked for math but forgot the col
                    col = numeric_cols[0]
                else:
                    # If no numeric columns exist, we can't do math; force to count
                    func = "count"
                    col = "*"
            
            # Validation: If the LLM guessed a column name that doesn't exist
            if col != "*" and col not in all_cols:
                # Try a fuzzy match or fallback
                col = "*" if func == "count" else (numeric_cols[0] if numeric_cols else "*")
    
            refined_measures.append({"column": col, "function": func})
        
        intent_dict["measures"] = refined_measures
    
        # 3. Fix Dimensions (Minimalism Rule)
        dims = intent_dict.get("dimensions", [])
        valid_dims = [d for d in dims if d in all_cols]
        
        if len(valid_dims) > 1:
            # Prefer descriptive columns over IDs (e.g., 'category' over 'category_id')
            refined = [d for d in valid_dims if "id" not in d.lower() and "name" not in d.lower()]
            intent_dict["dimensions"] = [refined[0]] if refined else [valid_dims[0]]
        else:
            intent_dict["dimensions"] = valid_dims
    
        # 4. Visualization Safety
        if intent_dict.get("intent_type") == "visualization" and not intent_dict.get("chart_type"):
            intent_dict["chart_type"] = "bar" # Default chart
    
        return intent_dict

    def _build_generic_prompt(self, query: str, schema: Dict) -> str:
        """A strictly data-agnostic prompt using schema-injection and role-based instructions."""
        return f"""
    You are an expert Data Analyst AI. Your goal is to translate a User Query into a structured JSON Intent based ONLY on the provided schema.
    
    ### DATA SCHEMA:
    {schema['full_desc']}
    
    ### INSTRUCTIONS:
    1. **Dimensions**: Look for categorical attributes used to group, slice, or describe the data (e.g., "by category", "per city").
    2. **Measures**: Identify the numeric calculation requested.
       - If the user asks "how many", "count", or "frequency", use function: "count" and column: "*".
       - If the user asks for "average", "total", "sum", or "max/min", you MUST select the most relevant Numeric column from the schema.
       - **CRITICAL**: The "column" field in measures must NEVER be null or empty.
    3. **Intent Type**: 
       - Use "aggregation" for text-based answers or lists.
       - Use "visualization" if the user explicitly mentions "chart", "plot", "graph", or "visualize".
    4. **Filters**: Map user conditions (e.g., "above 100") to standard operators: =, !=, >, <, >=, <=, in.
    
    ### CONSTRAINT RULES:
    - Use ONLY the column names exactly as they appear in the SCHEMA.
    - If the user asks for a calculation on a column that is NOT numeric, fallback to function: "count" and column: "*".
    - Limit results to 10 unless specified otherwise.
    
    ### OUTPUT FORMAT (JSON):
    {{
      "intent_type": "aggregation" | "visualization",
      "chart_type": "bar" | "line" | "pie" | "scatter" | null,
      "dimensions": ["exact_column_name"],
      "measures": [{{ "column": "exact_column_name", "function": "avg" | "sum" | "count" | "min" | "max" }}],
      "filters": [{{ "column": "col", "operator": ">", "value": 100 }}],
      "limit": 10
    }}
    
    USER QUERY: "{query}"
    """