from typing import Dict,Any
from app.agents.intent_classifier import classify_intent,AgentIntent
from app.tools.analytics_tool import analytics_tool
from app.tools.profile_tool import profile_tool
from app.agents.prompts import INVALID_QUERY_MESSAGE

def run_agent(dataset_id:str,user_input:str)->Dict[str,Any]:

    intent = classify_intent(user_input)

    if intent == AgentIntent.PROFILE:
        return profile_tool(dataset_id)
    
    if intent == AgentIntent.ANALYTICS:
        result = analytics_tool(
            dataset_id=dataset_id,
            intent=user_input
        )
        if result.get("confidence",1)<0.4:
            return{
                "type":"clarification",
                "message":"I'm not confident about this query ,can you rephrase this"

            }
        return result

    if intent == AgentIntent.PREVIEW:
        return {
            "type":"preview",
            "message":"preview tool not yet implemented"
        }
    
    return INVALID_QUERY_MESSAGE

