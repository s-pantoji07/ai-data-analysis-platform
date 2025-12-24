from app.intent.models import UserIntent
from app.analytics.query_models import AnalyticsQuery, Aggregation
from app.validator.metadata_validator import MetadataValidator

def map_intent_to_query(intent: UserIntent) -> AnalyticsQuery:
    """
    Converts UserIntent into AnalyticsQuery for execution and validates against metadata.
    """
    
    # 1. Map IntentAggregations to Analytics Aggregations
    # We use a list comprehension to ensure type compatibility with AnalyticsQuery
    aggregations = [
        Aggregation(column=m.column, function=m.function)
        for m in intent.measures
    ] if intent.measures else []

    # 2. Construct the AnalyticsQuery object
    query = AnalyticsQuery(
        dataset_id=intent.dataset_id,
        group_by=intent.dimensions,
        aggregations=aggregations,
        filters=intent.filters,
        limit=intent.limit,
    )

    # 3. Handle Ranking/Sorting fields if applicable
    if intent.intent_type == "ranking" and intent.order_by:
        query.order_by = intent.order_by
        query.order_direction = intent.order_direction

    # 4. 🔥 NEW: Metadata Validation
    # This checks the query against the actual schema of the dataset
    validator = MetadataValidator(intent.dataset_id)
    validated_query = validator.validate(query)

    return validated_query