from app.db import SessionLocal  # Or where your DB session is defined
from app.services.metadata_service import MetadataService
from app.analytics.exceptions import AnalyticsExecutionError
from app.analytics.engine import AnalyticsQuery  # Assuming this is your query model
from app.validator.validation_result import ValidationResult
class QueryValidator:
    @staticmethod
    def validate(query: AnalyticsQuery) -> ValidationResult:
        db = SessionLocal()
        corrections = []
        confidence = 1.0

        try:
            metadata = MetadataService.get_dataset_metadata(db, query.dataset_id)
            if not metadata:
                raise AnalyticsExecutionError("Dataset metadata not found")

            columns = {
                col['name']: col
                for table in metadata['tables']
                for col in table['columns']
            }

            # ---- SELECT ----
            if query.select:
                for col in query.select:
                    if col not in columns:
                        raise AnalyticsExecutionError(
                            f"Select column '{col}' does not exist"
                        )

            # ---- FILTERS ----
            for f in query.filters or []:
                if f.column not in columns:
                    raise AnalyticsExecutionError(
                        f"Filter column '{f.column}' does not exist"
                    )

            # ---- AGGREGATIONS ----
            for agg in query.aggregations or []:
                if agg.column not in columns:
                    raise AnalyticsExecutionError(
                        f"Aggregation column '{agg.column}' does not exist"
                    )

            # ---- GROUP BY ----
            group_fix = QueryValidator._auto_correct_group_by(query)
            if group_fix:
                corrections.append(group_fix)
                confidence -= 0.2

            # ---- ORDER BY ----
            order_fix = QueryValidator._auto_correct_order_by(query)
            if order_fix:
                corrections.append(order_fix)
                confidence -= 0.15

            return ValidationResult(
                is_valid=True,
                query=query,
                corrections=corrections,
                confidence_score=max(confidence, 0.0),
            )

        finally:
            db.close()
