# app/planner/planner.py
from typing import Any
from app.analytics.query_models import AnalyticsQuery, Aggregation, Filter
from app.planner.exceptions import QueryPlanningError
from app.intent.models import UserIntent

class QueryPlanner:
    def plan(self, metadata: dict, intent: UserIntent) -> AnalyticsQuery:
        """
        Converts the LLM-parsed UserIntent into a concrete AnalyticsQuery.
        This version is strictly data-agnostic and handles all intent components.
        """
        dataset_id = metadata["dataset_id"]
        
        # 1. Handle Aggregations (Measures)
        aggregations = []
        for m in (intent.measures or []):
            aggregations.append(
                Aggregation(column=m.column, function=m.function)
            )

        # 2. Handle Filters (WHERE clause) - THE FIX
        # We map IntentFilter -> AnalyticsQuery.Filter
        filters = []
        if intent.filters:
            for f in intent.filters:
                filters.append(
                    Filter(
                        column=f.column,
                        operator=f.operator,
                        value=f.value
                    )
                )

        # 3. Determine Order By Aliasing (Matches Engine Logic)
        # Your engine uses: "{FUNCTION}_{COLUMN}"
        if aggregations:
            primary_agg = aggregations[0]
            func_name = primary_agg.function.upper()
            col_clean = primary_agg.column.replace(" ", "_").replace("*", "total")
            order_by_alias = f"{func_name}_{col_clean}"
        else:
            order_by_alias = intent.order_by

        # 4. Handle Dimensions (GROUP BY)
        dimensions = intent.dimensions if intent.dimensions else None

        # 5. Semantic Direction Override
        intent_str = (intent.raw_query or "").lower()
        order_direction = intent.order_direction or "desc"
        if any(w in intent_str for w in ["lowest", "least", "bottom", "minimum"]):
            order_direction = "asc"

        return AnalyticsQuery(
            dataset_id=dataset_id,
            filters=filters,           # Pass filters to the engine
            group_by=dimensions,       # Pass group by columns
            aggregations=aggregations, # Pass calculations
            order_by=order_by_alias,   # Pass the alias for sorting
            order_direction=order_direction,
            limit=intent.limit or 10
        )