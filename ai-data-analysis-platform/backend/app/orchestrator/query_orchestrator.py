from app.planner.planner_service import plan_query
from app.validator.metadata_validator import MetadataValidator
from app.execution.confidence_gate import ConfidenceGate
from app.execution.execution_decision import ExecutionAction
from app.analytics.engine import AnalyticsEngine
from app.orchestrator.exceptions import QueryBlockedError

engine = AnalyticsEngine()


def run_query(dataset_id: str, intent: str):
    # 1️⃣ Plan
    query = plan_query(dataset_id, intent)

    # 2️⃣ Validate
    validator = MetadataValidator(query.dataset_id)
    validation_result = validator.validate(query)

    # 3️⃣ Confidence gate
    decision = ConfidenceGate.decide(validation_result)

    if decision.action == ExecutionAction.BLOCK:
        raise QueryBlockedError(decision.message)

    if decision.action == ExecutionAction.EXECUTE_WITH_WARNING:
        print(f"⚠️ {decision.message}")

    # 4️⃣ Execute
    result = engine.execute(query)

    return {
        "status": "success",
        "confidence": validation_result.confidence_score,
        "corrections": validation_result.corrections,
        "row_count": len(result),
        "data": result,
    }
