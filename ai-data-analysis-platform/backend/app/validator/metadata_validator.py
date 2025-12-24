from app.db.session import SessionLocal
from app.db.models.column import DataColumn
from app.analytics.query_models import AnalyticsQuery
from app.validator.validation_errors import (
    ColumnNotFoundError,
    InvalidAggregationError,
)

NUMERIC_TYPES = {"int", "int64", "float", "float64", "numeric"}


class MetadataValidator:
    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id
        self.columns = self._load_metadata()

    def _load_metadata(self) -> dict:
        db = SessionLocal()
        try:
            cols = (
                db.query(DataColumn)
                .join(DataColumn.table)
                .filter(DataColumn.table.has(dataset_id=self.dataset_id))
                .all()
            )

            return {
                col.name: {
                    "dtype": col.dtype,
                    "semantic_type": col.semantic_type,
                }
                for col in cols
            }
        finally:
            db.close()

    # -----------------------------
    # Validation Entry Point
    # -----------------------------
    def validate(self, query: AnalyticsQuery) -> AnalyticsQuery:
        self._validate_columns(query)
        self._validate_aggregations(query)
        return query

    # -----------------------------
    # Column Validation
    # -----------------------------
    def _validate_columns(self, query: AnalyticsQuery):
        all_columns = self.columns.keys()

        for col in (query.select or []) + (query.group_by or []):
            if col not in self.columns:
                raise ColumnNotFoundError(col, list(all_columns))

        for f in query.filters or []:
            if f.column not in self.columns:
                raise ColumnNotFoundError(f.column, list(all_columns))

    # -----------------------------
    # Aggregation Validation
    # -----------------------------
    def _validate_aggregations(self, query: AnalyticsQuery):
        for agg in query.aggregations or []:
            col_meta = self.columns.get(agg.column)

            if not col_meta:
                raise ColumnNotFoundError(
                    agg.column, list(self.columns.keys())
                )

            if col_meta["semantic_type"] not in NUMERIC_TYPES:
                raise InvalidAggregationError(
                    agg.column,
                    col_meta["semantic_type"],
                    agg.function,
                )
