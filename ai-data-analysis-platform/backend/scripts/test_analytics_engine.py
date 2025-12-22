from app.analytics.engine import AnalyticsEngine
from app.analytics.query_models import AnalyticsQuery, Aggregation

engine = AnalyticsEngine()

query = AnalyticsQuery(
    dataset_id="08fc4bbb-c912-4113-a7ad-195a2c8bcdb8",
    aggregations=[Aggregation(column="Global_Sales", function="sum")],
    group_by=["Year"]
)

result = engine.execute(query)
print(result)
