from fastapi import APIRouter, HTTPException
from app.services.planner_service import plan_query
from app.schemas.plan import PlanRequest

router = APIRouter()

@router.post("/")
def plan(request: PlanRequest):
    try:
        planned_query = plan_query(
            dataset_id=request.dataset_id,
            intent=request.intent
        )

        # FIX: Check if planned_query is already a dict or a Pydantic model
        if hasattr(planned_query, "model_dump"):
            query_data = planned_query.model_dump()
        else:
            query_data = planned_query

        return {
            "status": "success",
            "planned_query": query_data
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
