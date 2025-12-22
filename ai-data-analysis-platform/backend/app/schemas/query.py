from typing import List, Optional, Any
from pydantic import BaseModel
from app.analytics.query_models import AnalyticsQuery, Filter, Aggregation


class FilterSchema(BaseModel):
    column: str
    operator: str
    value: Any


class AggregationSchema(BaseModel):
    column: str
    function: str


class QueryRequest(BaseModel):
    dataset_id: str
    select: Optional[List[str]] = None
    filters: Optional[List[FilterSchema]] = []
    group_by: Optional[List[str]] = []
    aggregations: Optional[List[AggregationSchema]] = []
    limit: Optional[int] = 100

    def to_analytics_query(self) -> AnalyticsQuery:
        return AnalyticsQuery(
            dataset_id=self.dataset_id,
            select=self.select,
            filters=[Filter(**f.dict()) for f in self.filters],
            group_by=self.group_by,
            aggregations=[Aggregation(**a.dict()) for a in self.aggregations],
            limit=self.limit
        )
