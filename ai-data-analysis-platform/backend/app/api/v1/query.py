from fastapi import APIRouter, HTTPException
from app.schemas.query import QueryRequest
from app.services.query_service import execute_query
from app.analytics.exceptions import AnalyticsExecutionError

router = APIRouter(tags=["Analytics"])


@router.post("/")
def run_query(query: QueryRequest):
    try:
        return execute_query(query)
    except AnalyticsExecutionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error")
