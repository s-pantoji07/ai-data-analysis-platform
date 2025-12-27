import json
import os
from typing import Optional
import google.generativeai as genai
from app.intent.models import UserIntent
from app.intent.exceptions import IntentParsingError
from app.validator.metadata_validator import MetadataValidator
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()
class GeminiIntentParser:
    def __init__(self, api_key: Optional[str] = None, model: str = "gemini-2.5-flash-lite"):
        actual_key = api_key or os.getenv("GEMINI_API_KEY")
        if not actual_key:
            raise IntentParsingError("Gemini API Key is missing.")
        genai.configure(api_key=actual_key)
        self.model = genai.GenerativeModel(model)

    # app/intent/llm_intent_parser.py

    def parse(self, user_query: str, dataset_id: str) -> UserIntent:
        prompt = self._build_prompt(user_query, dataset_id)

        try:
            response = self.model.generate_content(
                prompt,
                generation_config={
                "temperature": 0,
                "response_mime_type": "application/json",
            },
        )

            res_text = response.text
            intent_dict = json.loads(res_text)

        # 1. Standardize the basics
            intent_dict["dataset_id"] = dataset_id
            if "raw_query" not in intent_dict or not    intent_dict["raw_query"]:
                intent_dict["raw_query"] = user_query

        # 2. FIX: Operator Normalization Map
            op_map = {
            "eq": "=", "equal": "=", "equals": "=",
            "neq": "!=", "not_equal": "!=",
            "gt": ">", "greater_than": ">",
            "gte": ">=", "greater_than_or_equal": ">=",
            "lt": "<", "less_than": "<",
            "lte": "<=", "less_than_or_equal": "<=",
            "contains": "in", "like": "in"
        }

            if "filters" in intent_dict:
                for f in intent_dict["filters"]:
                    raw_op = f.get("operator", "").lower()
                    f["operator"] = op_map.get(raw_op, raw_op) # Replace if in map, else keep raw

        # 3. Normalize functions (already in your code)
            function_map = {"average": "avg", "mean": "avg", "total": "sum"}
            if "measures" in intent_dict:
                for m in intent_dict["measures"]:
                    func_val = m.get("function") or m.get("func", "count")
                    m["function"] = function_map.get(func_val.lower(), func_val.lower())

            return UserIntent(**intent_dict)

        except Exception as e:
            logger.error(f"LLM Parsing failed: {str(e)}")
            raise IntentParsingError(f"LLM Parsing failed: {str(e)}")

    def _build_prompt(self, query: str, dataset_id: str) -> str:
        validator = MetadataValidator(dataset_id)
    
    # Create a dynamic column description for the LLM
        col_descriptions = []
        for name, meta in validator.columns.items():
            col_descriptions.append(f"- {name} ({meta['semantic_type']}). Samples: {meta.get('samples', 'N/A')}")
    
        schema_context = "\n".join(col_descriptions)

        return f"""
    TASK: Act as a Data Scientist. Convert the user query into a structured JSON analytics plan.
    DATASET_ID: {dataset_id}

    AVAILABLE COLUMNS & SAMPLES:
    {schema_context}

    CRITICAL RULES:
    1. Only use column names exactly as listed above.
    2. Identify the core measure the user wants (e.g., 'sales', 'total', 'average'). 
    3. If 'sales' is requested, map it to the numeric column that contains sales data (e.g., Global_Sales), NOT identifiers like 'Rank'.
    4. Dimensions are categorical columns (Genre, Platform, etc.).
    5. Only use these operators for filters: '=', '!=', '<', '<=', '>', '>=', 'in', 'not in'. 
   NEVER use words like 'eq' or 'equals'.

    MANDATORY JSON STRUCTURE:
    {{
      "dataset_id": "{dataset_id}",
      "intent_type": "aggregation", 
      "dimensions": ["Column1", "Column2"],
      "measures": [{{ "column": "NumericColumn", "function": "sum|avg|count|min|max" }}],
      "filters": [],
      "limit": 100
    }}

    USER QUERY: "{query}"
    """