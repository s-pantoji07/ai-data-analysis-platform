from app.validator.validation_result import ValidationResult
from app.analytics.query_models import AnalyticsQuery, Aggregation


class QueryValidator:
    """
    Validates AnalyticsQuery against dataset metadata.
    Performs:
    - Safety checks
    - Auto-corrections
    - Confidence scoring
    """

    def validate(self, query: AnalyticsQuery, metadata: dict) -> ValidationResult:
        confidence = 1.0
        corrections = []
        errors = []
        follow_ups = []

        columns = metadata.get("columns", {})

        # 1️⃣ Validate GROUP BY columns
        for col in query.group_by or []:
            if col not in columns:
                errors.append(f"Unknown group-by column: {col}")
                confidence -= 0.4

        # 2️⃣ Validate aggregations
        for agg in query.aggregations or []:
            if agg.column != "*" and agg.column not in columns:
                errors.append(f"Unknown aggregation column: {agg.column}")
                confidence -= 0.4

            # Numeric check for aggregation
            if agg.function.lower() in {"sum", "avg", "min", "max"}:
                if agg.column != "*" and not columns[agg.column]["is_numeric"]:
                    errors.append(
                        f"Aggregation {agg.function} requires numeric column, got {agg.column}"
                    )
                    confidence -= 0.3

        # 3️⃣ Auto-correction: missing GROUP BY
        if query.group_by and not query.aggregations:
            corrections.append("Auto-added COUNT(*) aggregation")
            query.aggregations.append(
                Aggregation(column="*", function="count")
            )
            confidence -= 0.1

        # 4️⃣ Limit safety clamp
        MAX_LIMIT = 1000
        if query.limit and query.limit > MAX_LIMIT:
            corrections.append(
                f"Limit reduced from {query.limit} to {MAX_LIMIT}"
            )
            query.limit = MAX_LIMIT
            confidence -= 0.05

        # 5️⃣ Confidence floor
        confidence = max(confidence, 0.0)

        # 6️⃣ Clarification trigger
        if confidence < 0.6:
            follow_ups.append(
                "Can you clarify the columns or aggregation you want?"
            )

        return ValidationResult(
            is_valid=len(errors) == 0,
            confidence=round(confidence, 2),
            corrections=corrections,
            errors=errors,
            follow_up_questions=follow_ups
        )
