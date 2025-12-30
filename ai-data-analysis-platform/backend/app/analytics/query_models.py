from typing import Any, List, Optional, Literal
from pydantic import BaseModel

class Filter(BaseModel):
    column:str
    operator: Literal["=", "!=", "<", "<=", ">", ">=", "in", "not in"] 
    value: Any
class Aggregation(BaseModel):
    column:str
    function:str

class AnalyticsQuery(BaseModel):
    dataset_id: str
    select:Optional[List[str]] = None
    filters:Optional[List[Filter]]=None
    group_by:Optional[List[str]] =None
    order_by:Optional[str] = None
    order_direction:Optional[str] = "desc"
    aggregations:Optional[List[Aggregation]] =None
    limit:Optional[int] = 100

