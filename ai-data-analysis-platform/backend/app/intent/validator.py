from typing import List
from app.analytics.query_models import AnalyticsQuery, Filter, Aggregation
from app.services.metadata_service import MetadataService
from app.db.session import SessionLocal
from app.analytics.exceptions import AnalyticsExecutionError

class QueryValidator:
    @staticmethod
    def validate(query: AnalyticsQuery):
        db = SessionLocal()
        try:
            metadata = MetadataService.get_dataset_metadata(db, query.dataset_id)
            if not metadata:
                raise AnalyticsExecutionError("Dataset metadata not found")

            # Flatten columns
            columns = {col['name']: col for table in metadata['tables'] for col in table['columns']}

            # Validate select
            if query.select:
                for col in query.select:
                    if col not in columns:
                        raise AnalyticsExecutionError(f"Select column '{col}' does not exist in dataset")

            # Validate filters
            for f in query.filters or []:
                if f.column not in columns:
                    raise AnalyticsExecutionError(f"Filter column '{f.column}' does not exist")
                dtype = columns[f.column]['semantic_type']
                if dtype == 'numeric' and f.operator not in ['=', '!=', '<', '<=', '>', '>=']:
                    raise AnalyticsExecutionError(f"Invalid operator '{f.operator}' for numeric column '{f.column}'")
                if dtype == 'categorical' and f.operator not in ['=', '!=', 'IN', 'NOT IN']:
                    raise AnalyticsExecutionError(f"Invalid operator '{f.operator}' for categorical column '{f.column}'")

            # Validate aggregations
            for agg in query.aggregations or []:
                if agg.column not in columns:
                    raise AnalyticsExecutionError(f"Aggregation column '{agg.column}' does not exist")
                dtype = columns[agg.column]['semantic_type']
                if agg.function.lower() in ['sum', 'avg', 'min', 'max'] and dtype != 'numeric':
                    raise AnalyticsExecutionError(f"Aggregation '{agg.function}' cannot be applied to non-numeric column '{agg.column}'")

            # Validate group_by
            for g in query.group_by or []:
                if g not in columns:
                    raise AnalyticsExecutionError(f"Group by column '{g}' does not exist")

            # Validate order_by
            if query.order_by and query.order_by not in columns:
                raise AnalyticsExecutionError(f"Order by column '{query.order_by}' does not exist")

            return True
        finally:
            db.close()
