# api/v1/query.py
from fastapi import APIRouter, HTTPException
from app.schemas.query import QueryRequest
from app.agents.orchestrator import run_agent  # <-- use the orchestrator

router = APIRouter()

@router.post("/")
def query_endpoint(request: QueryRequest):
    """
    This endpoint handles:
    - Analytics queries (aggregations, averages, counts, etc.)
    - Visualization requests (bar/line charts)
    """
    try:
        result = run_agent(
            dataset_id=request.dataset_id,
            user_input=request.intent
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
