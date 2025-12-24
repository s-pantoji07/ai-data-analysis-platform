from typing import Optional
import json
import logging

from app.intent.models import UserIntent
from app.intent.exceptions import IntentParsingError

# Gemini SDK (you can swap impl later)
import google.generativeai as genai

logger = logging.getLogger(__name__)


class GeminiIntentParser:
    """
    Converts natural language → UserIntent using Gemini.
    NO planning, NO metadata, NO execution.
    """

    def __init__(self, api_key: str, model: str = "gemini-2.5-flash"):
        genai.configure(api_key=api_key)
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

            raw_text = response.text
            logger.debug("Gemini raw output: %s", raw_text)
            intent_dict = json.loads(raw_text)

            # --- START OF ADDITION: Normalization ---
            # Ensure function names are lowercase to satisfy Pydantic Literals
            if "measures" in intent_dict and isinstance(intent_dict["measures"], list):
                for measure in intent_dict["measures"]:
                    if "function" in measure and isinstance(measure["function"], str):
                        measure["function"] = measure["function"].lower()
            # --- END OF ADDITION ---

            # Now Pydantic will receive 'avg' instead of 'AVG'
            intent = UserIntent(**intent_dict)
            return intent

        except json.JSONDecodeError:
            raise IntentParsingError("LLM response was not valid JSON")
        except Exception as e:
            # This captures the ValidationError if normalization fails or other issues occur
            raise IntentParsingError(str(e))

    # -----------------------------
    # Prompt Engineering
    # -----------------------------
    def _build_prompt(self, query: str, dataset_id: str) -> str:
        return f"""
You are an analytics intent parser.

Convert the user's question into a STRICT JSON object
that matches the following schema exactly:

UserIntent:
- dataset_id: string
- intent_type: one of ["aggregation", "ranking", "filter", "profiling"]
- dimensions: list of strings (or empty)
- measures: list of {{ column, function }}
- filters: list of {{ column, operator, value }} (or empty)
- order_by: string or null
- limit: integer or null
- raw_query: original user text

Rules:
- DO NOT invent columns
- DO NOT include explanations
- Output JSON ONLY

Dataset ID: "{dataset_id}"

User Query:
"{query}"
"""
