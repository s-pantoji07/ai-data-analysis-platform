from app.db.session import SessionLocal
from app.db.models.column import DataColumn
from app.analytics.query_models import AnalyticsQuery, Aggregation
from app.validator.validation_result import ValidationResult, Correction

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
    def validate(self, query: AnalyticsQuery) -> ValidationResult:
        corrections = []
        errors = []
        confidence = 1.0

        # -----------------------------
        # Auto-correction hooks (Order: Inference -> Group By -> Order By)
        # -----------------------------
        
        # 1. Aggregation inference
        agg_fix = self._auto_infer_aggregation(query)
        if agg_fix:
            corrections.append(agg_fix)
            confidence -= 0.25

        # 2. GROUP BY auto-correction
        group_by_fix = self._auto_correct_group_by(query)
        if group_by_fix:
            corrections.append(group_by_fix)
            confidence -= 0.2

        # 3. ORDER BY auto-correction
        order_by_fix = self._auto_correct_order_by(query)
        if order_by_fix:
            corrections.append(order_by_fix)
            confidence -= 0.15

        # -----------------------------
        # Validation Checks
        # -----------------------------

        # Column checks
        errors.extend(self._validate_columns(query))

        # Aggregation checks
        errors.extend(self._validate_aggregations(query))

        is_valid = len(errors) == 0

        return ValidationResult(
            is_valid=is_valid,
            corrected_query=query.model_dump(),
            corrections=corrections or None,
            errors=errors or None,
            confidence_score=round(max(confidence, 0.0), 2),
        )

    # -----------------------------
    # Column Validation
    # -----------------------------
    def _validate_columns(self, query: AnalyticsQuery) -> list[str]:
        errors = []
        all_columns = self.columns.keys()

        for col in (query.select or []) + (query.group_by or []):
            if col not in self.columns:
                errors.append(
                    f"Column '{col}' not found. Available columns: {list(all_columns)}"
                )

        for f in query.filters or []:
            if f.column not in self.columns:
                errors.append(
                    f"Filter column '{f.column}' not found. Available columns: {list(all_columns)}"
                )

        return errors

    # -----------------------------
    # Aggregation Validation
    # -----------------------------
    def _validate_aggregations(self, query: AnalyticsQuery) -> list[str]:
        errors = []

        for agg in query.aggregations or []:
            col_meta = self.columns.get(agg.column)

            if not col_meta:
                if agg.column == "*":  # Allow count(*)
                    continue
                errors.append(
                    f"Aggregation column '{agg.column}' not found in dataset"
                )
                continue

            if col_meta["semantic_type"] not in NUMERIC_TYPES and agg.function.lower() != "count":
                errors.append(
                    f"Invalid aggregation: cannot apply {agg.function} "
                    f"on non-numeric column '{agg.column}' "
                    f"(type={col_meta['semantic_type']})"
                )

        return errors

    # -----------------------------
    # Auto-Correction Rules
    # -----------------------------
    def _auto_infer_aggregation(self, query: AnalyticsQuery) -> Correction | None:
        """
        Rule:
        If no aggregation is specified but numeric columns are selected,
        infer SUM() aggregation for numeric measures.
        """
        # Do nothing if aggregation already exists
        if query.aggregations:
            return None

        inferred_aggs = []

        for col in query.select or []:
            col_meta = self.columns.get(col)
            if not col_meta:
                continue

            if col_meta["semantic_type"] in NUMERIC_TYPES:
                # Append as Pydantic models (Aggregation)
                inferred_aggs.append(
                    Aggregation(column=col, function="sum")
                )

        # If no numeric columns → COUNT(*)
        if not inferred_aggs:
            inferred_aggs.append(
                Aggregation(column="*", function="count")
            )

        # Apply inference
        query.aggregations = inferred_aggs

        return Correction(
            field="aggregations",
            original=None,
            corrected=[{"column": a.column, "function": a.function} for a in inferred_aggs],
            reason="Aggregation inferred automatically based on selected numeric columns",
        )

    def _auto_correct_group_by(self, query: AnalyticsQuery) -> Correction | None:
        """
        Rule:
        If aggregations exist and select contains non-aggregated categorical columns,
        automatically add them to group_by.
        """
        if not query.aggregations or not query.select:
            return None

        missing_group_by = []

        for col in query.select:
            if col in self.columns:
                col_type = self.columns[col]["semantic_type"]

                # Only group by non-numeric (dimensions)
                if col_type not in NUMERIC_TYPES:
                    if col not in (query.group_by or []):
                        missing_group_by.append(col)

        if not missing_group_by:
            return None

        # Apply correction
        query.group_by = (query.group_by or []) + missing_group_by

        return Correction(
            field="group_by",
            original=None,
            corrected=query.group_by,
            reason="GROUP BY is required for non-aggregated selected columns",
        )

    def _auto_correct_order_by(self, query: AnalyticsQuery) -> Correction | None:
        """
        Rule:
        If order_by references a raw column but an aggregation exists on that column,
        order by the aggregated expression instead.
        """
        if not query.order_by or not query.aggregations:
            return None

        for agg in query.aggregations:
            if query.order_by == agg.column:
                corrected = f"{agg.function.upper()}({agg.column})"
                query.order_by = corrected

                return Correction(
                    field="order_by",
                    original=agg.column,
                    corrected=corrected,
                    reason="Ordering by aggregated metric requires aggregation function",
                )

        return None