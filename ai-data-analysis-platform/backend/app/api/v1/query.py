from fastapi import APIRouter, HTTPException
from typing import Any
from app.analytics.query_models import AnalyticsQuery
from app.tools.analytics import run_analytics
from app.analytics.exceptions import AnalyticsExecutionError

router = APIRouter(tags=["Analytics"])

@router.post("/", response_model=Any)
def run_query(query: AnalyticsQuery):
    """
    Endpoint to execute analytics queries or return dataset profiling.
    
    If the query body is empty (only dataset_id), it returns a profiling dictionary.
    Otherwise, it returns a list of data records.
    """
    try:
        # Pass the Pydantic model directly to the engine wrapper
        result = run_analytics(query)
        return result
    except AnalyticsExecutionError as e:
        # Use 400 for bad queries (e.g., column doesn't exist)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Log the actual error to your terminal for debugging
        print(f"Internal Error: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")