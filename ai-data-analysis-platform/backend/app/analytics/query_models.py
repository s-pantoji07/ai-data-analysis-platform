from typing import Any, List, Optional
from pydantic import BaseModel

class Filter(BaseModel):
    column:str
    operator:str
    value:Any

class Aggregation(BaseModel):
    column:str
    function:str

class AnalyticsQuery(BaseModel):
    dataset_id: str
    select:Optional[List[str]] = None
    filters:Optional[List[Filter]]=[]
    group_by:Optional[List[str]] =[]
    aggregations:Optional[List[Aggregation]] =[]
    limit:Optional[int] = 100