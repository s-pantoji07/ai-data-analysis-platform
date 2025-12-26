from app.intent.models import UserIntent
from app.analytics.query_models import AnalyticsQuery, Aggregation
from app.validator.metadata_validator import MetadataValidator
from app.validator.validation_result import ValidationResult
# app/intent/intent_mapper.py

def map_intent_to_query(intent: UserIntent) -> ValidationResult: # <-- Change Return Type
    """
    Converts UserIntent into AnalyticsQuery and validates it immediately.
    """
    
    # 1. Map measures to Aggregation objects
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

    if intent.intent_type == "ranking" and intent.order_by:
        query.order_by = intent.order_by
        query.order_direction = intent.order_direction

    # 3. Validate and return the ValidationResult (NOT just the query)
    validator = MetadataValidator(intent.dataset_id)
    return validator.validate(query)