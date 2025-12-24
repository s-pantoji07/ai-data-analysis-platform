from app.intent.models import UserIntent, IntentAggregation
from app.intent.intent_mapper import map_intent_to_query

intent = UserIntent(
    dataset_id="vgsales",
    intent_type="ranking",
    dimensions=["Genre"],
    measures=[IntentAggregation(column="Global_Sales", function="sum")],
    order_by="Global_Sales",
    limit=5,
    raw_query="Top 5 genres by global sales"
)

query = map_intent_to_query(intent)
print(query)
