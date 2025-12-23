from fastapi import APIRouter, HTTPException
from app.planner.models import PlannerRequest
from app.services.planner_service import plan_query
from app.planner.exceptions import QueryPlanningError

router = APIRouter(tags=["Query Planner"])


@router.post("/")
def plan(request: PlannerRequest):
    try:
        return plan_query(
            dataset_id=request.dataset_id,
            intent=request.intent
        )
    except QueryPlanningError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error")
