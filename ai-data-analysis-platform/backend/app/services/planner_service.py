from app.intent.llm_intent_parser import GeminiIntentParser
from app.validator.metadata_validator import MetadataValidator
from app.validator.confidence_gate import ConfidenceGate, ExecutionAction
from app.planner.exceptions import QueryPlanningError
from app.db.session import SessionLocal
from app.services.metadata_service import MetadataService
from app.planner.planner import QueryPlanner

def plan_query(dataset_id: str, user_query_text: str):
    # 1. Convert String to Intent Object (LLM)
    parser = GeminiIntentParser()
    intent_obj = parser.parse(user_query_text, dataset_id) 

    # 2. Fetch Metadata
    db = SessionLocal()
    try:
        metadata = MetadataService.get_dataset_metadata(db, dataset_id)
    finally:
        db.close()

    # 3. Pass the INTENT OBJECT to the Planner (NOT the string)
    planner = QueryPlanner()
    planned_query = planner.plan(metadata, intent_obj)

    # 4. Validate
    validator = MetadataValidator(dataset_id)
    validation_result = validator.validate(planned_query)

    # 5. Confidence Gate
    decision = ConfidenceGate.decide(validation_result)
    if decision.action == ExecutionAction.BLOCK:
        raise QueryPlanningError(f"Query blocked: {decision.message}")

    return {
        "planned_query": validation_result.corrected_query, 
        "intent_obj": intent_obj, 
        "confidence": validation_result.confidence_score,
        "corrections": [c.message for c in validation_result.corrections]
    }