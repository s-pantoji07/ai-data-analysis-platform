from typing import Any
from app.analytics.query_models import AnalyticsQuery, Aggregation
from app.planner.exceptions import QueryPlanningError


class QueryPlanner:
    def plan(self, metadata: dict, intent: Any) -> AnalyticsQuery:
        intent_str = (
            intent.raw_query.lower()
            if hasattr(intent, "raw_query")
            else str(intent).lower()
        )

        dataset_id = metadata["dataset_id"]
        columns = metadata.get("profiling_summary", {}).get("columns", [])

        measures = []
        candidate_dimensions = []

        # ----------------------------------
        # STEP 1: Semantic mapping
        # ----------------------------------
        for col in columns:
            name = col["name"]
            tags = col.get("semantic_tags", [])

            if "metric" in tags:
                measures.append(name)

            if "dimension" in tags:
                candidate_dimensions.append(name)

        if not measures:
            raise QueryPlanningError(
                "No numeric measures found in dataset to answer this query."
            )

        # ----------------------------------
        # STEP 2: Infer filtered columns
        # ----------------------------------
        filtered_columns = set()

        for col in columns:
            col_name = col["name"].lower()
            if col_name in intent_str:
                filtered_columns.add(col["name"])

        # ----------------------------------
        # STEP 3: Remove filtered dimensions
        # ----------------------------------
        dimensions = [
            d for d in candidate_dimensions
            if d not in filtered_columns
        ]

        # ----------------------------------
        # STEP 4: Aggregation
        # ----------------------------------
        func = "sum"
        if any(w in intent_str for w in ["avg", "average", "mean"]):
            func = "avg"
        elif "count" in intent_str:
            func = "count"

        # ----------------------------------
        # STEP 5: Ranking
        # ----------------------------------
        order_direction = "desc"
        if any(w in intent_str for w in ["lowest", "least", "bottom"]):
            order_direction = "asc"

        order_by = measures[0]

        # ----------------------------------
        # STEP 6: Limit
        # ----------------------------------
        limit = None
        for token in intent_str.split():
            if token.isdigit():
                limit = int(token)
                break

        return AnalyticsQuery(
            dataset_id=dataset_id,
            group_by=dimensions if dimensions else None,
            aggregations=[
                Aggregation(column=measures[0], function=func)
            ],
            order_by=order_by,
            order_direction=order_direction,
            limit=limit
        )
