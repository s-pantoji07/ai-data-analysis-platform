from app.intent.llm_intent_parser import GeminiIntentParser
from app.intent.intent_mapper import map_intent_to_query
from app.validator.metadata_validator import MetadataValidator
from app.validator.confidence_gate import ConfidenceGate, ExecutionAction
from app.planner.exceptions import QueryPlanningError

def plan_query(dataset_id: str, intent: str):
    # 1. LLM Parsing
    parser = GeminiIntentParser()
    user_intent = parser.parse(user_query=intent, dataset_id=dataset_id)

    # 2. Map & Validate (Everything happens here now)
    # validation_result is now a ValidationResult object
    validation_result = map_intent_to_query(user_intent)

    # 3. Confidence gate logic
    decision = ConfidenceGate.decide(validation_result)

    if decision.action == ExecutionAction.BLOCK:
        raise QueryPlanningError(decision.message)

    # 4. Return clean dictionary
    return {
        "query": validation_result.corrected_query, 
        "confidence": validation_result.confidence_score,
        "corrections": [vars(c) for c in (validation_result.corrections or [])],
        "warnings": decision.message
    }