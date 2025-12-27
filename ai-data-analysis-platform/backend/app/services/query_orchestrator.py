from app.services.planner_service import plan_query
from app.analytics.engine import AnalyticsEngine
from app.validator.confidence_gate import ConfidenceGate, ExecutionAction

engine = AnalyticsEngine()

def execute_from_intent(dataset_id: str, intent: str):
    # 1. Get the plan (Note: plan_query already handles validation/blocking)
    plan_dict = plan_query(dataset_id, intent)
    
    # 2. Log warnings if they exist (passed from planner_service)
    if plan_dict.get("warnings"):
        print(f"⚠️ {plan_dict['warnings']}")

    # 3. Execute the planned_query object
    # In your planner_service, this is validation_result.corrected_query
    return engine.execute(plan_dict["planned_query"])