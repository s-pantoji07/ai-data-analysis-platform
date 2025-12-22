from app.tools.analytics import run_analytics

@router.post("/")
def query_data(query: QuerySchema):
    return run_analytics(query.to_analytics_query())
