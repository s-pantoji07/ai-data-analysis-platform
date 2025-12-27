from app.intent.models import UserIntent
from app.analytics.query_models import AnalyticsQuery, Aggregation, Filter # Add Filter
from app.validator.metadata_validator import MetadataValidator
from app.validator.validation_result import ValidationResult

# app/intent/intent_mapper.py

def map_intent_to_query(intent: UserIntent) -> ValidationResult:
    # 1. Map measures to dictionaries
    aggregations = [
        {"column": m.column, "function": m.function}
        for m in intent.measures
    ] if intent.measures else []

    # 2. Map filters to dictionaries (This solves the model_type error)
    filters = [
        {"column": f.column, "operator": f.operator, "value": f.value}
        for f in intent.filters
    ] if intent.filters else []

    # 3. Construct the AnalyticsQuery object using these dictionaries
    query = AnalyticsQuery(
        dataset_id=intent.dataset_id,
        group_by=intent.dimensions or [],
        aggregations=aggregations, # Pydantic will convert dict -> Aggregation
        filters=filters,           # Pydantic will convert dict -> Filter
        limit=intent.limit or 100,
    )

    if intent.intent_type == "ranking" and intent.order_by:
        query.order_by = f"{intent.measures[0].function.upper()}(\"{intent.measures[0].column}\")"
        query.order_direction = "desc"

    validator = MetadataValidator(intent.dataset_id)
    return validator.validate(query)