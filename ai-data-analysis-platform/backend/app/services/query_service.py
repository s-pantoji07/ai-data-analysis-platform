from app.analytics.engine import AnalyticsEngine
from app.schemas.query import QueryRequest

engine = AnalyticsEngine()


def execute_query(query_request: QueryRequest):
    analytics_query = query_request.to_analytics_query()
    result = engine.execute(analytics_query)

    return {
        "status": "success",
        "row_count": len(result),
        "data": result
    }
