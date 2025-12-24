from app.intent.models import UserIntent
from app.analytics.query_models import AnalyticsQuery, Aggregation


def map_intent_to_query(intent: UserIntent) -> AnalyticsQuery:
    """
    Converts UserIntent into AnalyticsQuery for execution.
    """

    aggregations = None
    if intent.measures:
        aggregations = [
            Aggregation(
                column=measure.column,
                function=measure.function
            )
            for measure in intent.measures
        ]

    query = AnalyticsQuery(
        dataset_id=intent.dataset_id,
        group_by=intent.dimensions,
        aggregations=aggregations,
        filters=intent.filters,
        limit=intent.limit
    )

    # Ranking support
    if intent.intent_type == "ranking" and intent.order_by:
        query.order_by = intent.order_by
        query.order_direction = intent.order_direction

    return query
