from app.services.planner_service import plan_query
from app.analytics.engine import AnalyticsEngine

engine = AnalyticsEngine()

def run_query(dataset_id: str, intent_text: str):
    # 1️⃣ Plan (Now correctly passes the string to be parsed)
    # This result now contains the intent_obj and the planned_query
    plan_dict = plan_query(dataset_id, intent_text)
    
    # 2️⃣ Execute
    # Access the actual query object from the dictionary
    result = engine.execute(plan_dict["planned_query"])

    # 3️⃣ Return structured response
    return {
        "status": "success",
        "type": plan_dict.get("type", "query_result"),
        "confidence": plan_dict["confidence"],
        "data": result,
        "chart": plan_dict.get("chart_config") # If visualization
    }