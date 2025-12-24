from app.orchestrator.query_orchestrator import run_query
from app.schemas.query import QueryRequest


def execute_query(query_request: QueryRequest):
    return run_query(
        dataset_id=query_request.dataset_id,
        intent=query_request.intent,
    )
