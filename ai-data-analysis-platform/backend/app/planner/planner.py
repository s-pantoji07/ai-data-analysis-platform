from typing import Any, List
from app.analytics.query_models import AnalyticsQuery, Aggregation
from app.planner.exceptions import QueryPlanningError
from app.planner.rules import is_numeric, is_categorical

class QueryPlanner:
    def plan(self, metadata: dict, intent: Any) -> AnalyticsQuery:
        # 1. Normalize Intent
        if hasattr(intent, "raw_query"):
            intent_str = intent.raw_query.lower()
        elif isinstance(intent, dict):
            intent_str = intent.get("raw_query", "").lower()
        else:
            intent_str = str(intent).lower()

        dataset_id = metadata["dataset_id"]
        columns = metadata.get("tables", [{}])[0].get("columns", [])

        found_measures = []
        found_dimensions = []

        # 2. Semantic Mapping: Find columns mentioned in intent
        for col in columns:
            col_name = col["name"].lower()
            # Check if column name or any aliases are in the intent string
            if col_name in intent_str or any(a.lower() in intent_str for a in col.get("aliases", [])):
                if is_numeric(metadata, col["name"]):
                    found_measures.append(col["name"])
                else:
                    found_dimensions.append(col["name"])

        # 3. Determine Aggregation Function
        func = "sum" # default
        if any(word in intent_str for word in ["average", "avg", "mean"]):
            func = "avg"
        elif "count" in intent_str:
            func = "count"

        # 4. Construct Query
        if not found_measures and not found_dimensions:
            raise QueryPlanningError(f"Could not map any columns from intent: '{intent_str}'")

        return AnalyticsQuery(
            dataset_id=dataset_id,
            group_by=found_dimensions,
            aggregations=[
                Aggregation(column=m, function=func) for m in found_measures
            ]
        )