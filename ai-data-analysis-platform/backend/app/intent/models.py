from pydantic import BaseModel
from typing import List, Optional, Any,Literal

class IntentFilter(BaseModel):
    column: str
    operator: Literal['=', '!=', '<', '<=', '>', '>=', 'in', 'not in']
    value: Any


class IntentAggregation(BaseModel):
    column: str
    function: Literal['sum', 'avg', 'min', 'max', 'count']

class UserIntent(BaseModel):
    dataset_id: str

    intent_type:Literal[
        "aggregation",
        "comparison",
        "trend",
        "ranking",
        "distribution"
    ]
#optinal components of the intent
    dimensions: Optional[List[str]] = None
    measures: Optional[List[IntentAggregation]] = None
    filters: Optional[List[IntentFilter]] = None
#ranking specific

    order_by : Optional[str] = None
    order_direction : Optional[Literal["asc", "desc"]] = "desc"
    limit: Optional[int] = 10

    raw_query :Optional[str] = None

    
