from fastapi import APIRouter,HTTPException
from app.schemas.query import QueryRequest
from app.services.query_service import execute_intent_query


router = APIRouter()

@router.post("/")
def query_endpoint(request: QueryRequest):
    try:
        return execute_intent_query(
            dataset_id = request.dataset_id,
            intent = request.intent
        )
    except Exception as e:
        raise HTTPException(status_code=400,detail=str(e))