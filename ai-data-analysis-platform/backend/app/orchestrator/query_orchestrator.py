from app.services.planner_service import plan_query
from app.validator.metadata_validator import MetadataValidator
from app.execution.confidence_gate import ConfidenceGate
from app.execution.execution_decision import ExecutionAction
from app.analytics.engine import AnalyticsEngine
# from app.orchestrator.exceptions import QueryBlockedError

engine = AnalyticsEngine()


def run_query(dataset_id: str, intent: str):
    # 1️⃣ Plan (Already validates and checks confidence gate internally)
    plan_dict = plan_query(dataset_id, intent)

    # 2️⃣ Execute
    result = engine.execute(plan_dict["planned_query"])

    return {
        "status": "success",
        "confidence": plan_dict["confidence"],
        "corrections": plan_dict["corrections"],
        "row_count": len(result),
        "data": result,
    }
