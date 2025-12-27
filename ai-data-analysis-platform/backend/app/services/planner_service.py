from app.intent.llm_intent_parser import GeminiIntentParser
from app.intent.intent_mapper import map_intent_to_query
from app.validator.metadata_validator import MetadataValidator
from app.validator.confidence_gate import ConfidenceGate, ExecutionAction
from app.planner.exceptions import QueryPlanningError
from  app.analytics.engine import AnalyticsEngine

# app/services/planner_service.py

def plan_query(user_intent):
    # 1. Map intent to a structured AnalyticsQuery
    validation_result = map_intent_to_query(user_intent)

    # 2. Confidence decision
    decision = ConfidenceGate.decide(validation_result)

    if decision.action == ExecutionAction.BLOCK:
        raise QueryPlanningError(decision.message)

    # Return the validation result object so the caller can use the corrected_query
    return validation_result
