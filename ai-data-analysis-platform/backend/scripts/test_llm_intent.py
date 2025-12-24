from app.intent.llm_intent_parser import GeminiIntentParser
import json
import os
parser = GeminiIntentParser(api_key="AIzaSyCHykSF97UIUAQY--SGiK9zv1VK1JJjWN4")

intent = parser.parse(
    user_query="give me the avg length of sepal width",
    dataset_id="iris"
)
print("\n--- Parsed Intent ---")
# print(intent)
print(json.dumps(intent.model_dump(), indent=2))