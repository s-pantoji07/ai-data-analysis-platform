from typing import Dict, Any
from app.agents.intent_classifier import classify_intent, AgentIntent
from app.tools.analytics_tool import analytics_tool
from app.tools.profile_tool import profile_tool
from app.tools.preview_tool import preview_tool

def run_agent(dataset_id: str, user_input: str) -> Dict[str, Any]:
    # 1. Classify the high-level intent
    intent_result = classify_intent(user_input)
    # Ensure the raw query is attached for the detailed parser later
    intent_result.raw_query = user_input 

    # 2. Route to the correct tool
    if intent_result.intent == AgentIntent.PROFILE:
        return profile_tool(dataset_id)

    if intent_result.intent == AgentIntent.PREVIEW:
        return preview_tool(dataset_id)

    # Both Analytics and Visualization require the same SQL planning logic
    if intent_result.intent in [AgentIntent.ANALYTICS, AgentIntent.VISUALIZATION]:
        # Pass the structured IntentResult object
        return analytics_tool(dataset_id=dataset_id, intent_obj=intent_result)

    return {"type": "error", "message": "Invalid query intent."}