from typing import Dict,Any
from app.analytics.engine import AnalyticsEngine
from app.analytics.query_models import AnalyticsQuery

def preview_tools(
        dataset_id:str,
        mode:str="head",
        limit:int=5,
)->Dict[str,Any]:
    

    engine=AnalyticsEngine()

    query=AnalyticsQuery(
        dataset_id=dataset_id,
        limit=limit
    )

    if mode =="tail":
        query.order_direction = "desc"

    result = engine.execute(query)

    return{
        "type":"preview",
        "mode":mode,
        "row_count":result.row_count,
        "data":result.data
    }