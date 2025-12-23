from app.analytics.query_models import AnalyticsQuery, Aggregation
from app.planner.exceptions import QueryPlanningError
from app.planner.rules import is_numeric, is_categorical


class QueryPlanner:
    """
    Converts high-level intent into AnalyticsQuery.
    No DB access. No execution.
    """

    def plan(self, metadata: dict, intent: str) -> AnalyticsQuery:
        intent = intent.lower()

        dataset_id = metadata["dataset_id"]

        # ---- RULE 1: Total sales by year ----
        if "total" in intent and "sales" in intent and "year" in intent:
            if not is_numeric(metadata, "Global_Sales"):
                raise QueryPlanningError("Global_Sales must be numeric")

            if not is_numeric(metadata, "Year"):
                raise QueryPlanningError("Year must be numeric or date-like")

            return AnalyticsQuery(
                dataset_id=dataset_id,
                group_by=["Year"],
                aggregations=[
                    Aggregation(column="Global_Sales", function="sum")
                ]
            )

        # ---- RULE 2: Total sales by genre ----
        if "total" in intent and "sales" in intent and "genre" in intent:
            if not is_categorical(metadata, "Genre"):
                raise QueryPlanningError("Genre must be categorical")

            return AnalyticsQuery(
                dataset_id=dataset_id,
                group_by=["Genre"],
                aggregations=[
                    Aggregation(column="Global_Sales", function="sum")
                ]
            )

        # ---- FALLBACK ----
        raise QueryPlanningError("Could not understand intent")
