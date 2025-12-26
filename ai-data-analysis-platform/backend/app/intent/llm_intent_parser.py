import json
import os
from typing import Optional
import google.generativeai as genai
from app.intent.models import UserIntent
from app.intent.exceptions import IntentParsingError
from app.validator.metadata_validator import MetadataValidator
import logging


logger = logging.getLogger(__name__)
class GeminiIntentParser:
    def __init__(self, api_key: Optional[str] = None, model: str = "gemini-2.5-flash"):
        actual_key = api_key or os.getenv("GEMINI_API_KEY")
        if not actual_key:
            raise IntentParsingError("Gemini API Key is missing.")
        genai.configure(api_key=actual_key)
        self.model = genai.GenerativeModel(model)

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

            # FORCE injection of dataset_id if the LLM forgot it
            intent_dict["dataset_id"] = dataset_id
            
            # Ensure raw_query is preserved
            if "raw_query" not in intent_dict or not intent_dict["raw_query"]:
                intent_dict["raw_query"] = user_query

            # Normalize functions
            function_map = {"average": "avg", "mean": "avg", "total": "sum"}
            if "measures" in intent_dict:
                for m in intent_dict["measures"]:
                    # Handle cases where LLM might use 'func' instead of 'function'
                    func_val = m.get("function") or m.get("func", "count")
                    m["function"] = function_map.get(func_val.lower(), func_val.lower())

            return UserIntent(**intent_dict)

        except Exception as e:
            logger.error(f"LLM Parsing failed. Raw response: {response.text if 'response' in locals() else 'No response'}")
            raise IntentParsingError(f"LLM Parsing failed: {str(e)}")

    def _build_prompt(self, query: str, dataset_id: str) -> str:
        validator = MetadataValidator(dataset_id)
        schema_context = ", ".join([f"{name} ({meta['semantic_type']})" for name, meta in validator.columns.items()])

        return f"""
        TASK: Convert user query to structured JSON.
        DATASET_ID: {dataset_id}
        COLUMNS: {schema_context}

        MANDATORY JSON STRUCTURE:
        {{
          "dataset_id": "{dataset_id}",
          "intent_type": "aggregation", 
          "dimensions": ["column_name"],
          "measures": [{{ "column": "column_name", "function": "avg|sum|count|min|max" }}],
          "filters": [],
          "order_by": null,
          "limit": 100,
          "raw_query": "{query}"
        }}

        CRITICAL RULES:
        1. "intent_type" must be one of: aggregation, ranking, filter, profiling.
        2. Use ONLY the column names provided in the COLUMNS list.
        3. Do not use keys like 'aggregate' or 'func'. Use 'measures' and 'function'.

        USER QUERY: "{query}"
        """