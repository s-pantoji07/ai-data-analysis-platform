from app.db.session import SessionLocal
from app.db.models.column import DataColumn
from app.analytics.query_models import AnalyticsQuery, Aggregation
from app.validator.validation_result import ValidationResult, Correction

NUMERIC_TYPES = {"int", "int64", "float", "float64", "numeric"}

# Define semantic synonyms for common data analysis terms
COLUMN_SYNONYMS = {
    "revenue": ["revenue", "sales", "income", "turnover", "earnings", "amount"],
    "count": ["count", "number", "total", "quantity"],
}


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

        # ---------------------------------------------------------
        # Auto-correction hooks
        # Sequence: Synonyms -> Inference -> Group By -> Order By
        # ---------------------------------------------------------

        # 1. Resolve column synonyms (e.g., 'revenue' -> 'Global_Sales')
        synonym_fixes = self._auto_resolve_column_synonyms(query)
        if synonym_fixes:
            corrections.extend(synonym_fixes)
            confidence -= 0.2

        # 2. Aggregation inference
        agg_fix = self._auto_infer_aggregation(query)
        if agg_fix:
            corrections.append(agg_fix)
            confidence -= 0.25

        # 3. GROUP BY auto-correction
        group_by_fix = self._auto_correct_group_by(query)
        if group_by_fix:
            corrections.append(group_by_fix)
            confidence -= 0.2

        # 4. ORDER BY auto-correction
        order_by_fix = self._auto_correct_order_by(query)
        if order_by_fix:
            corrections.append(order_by_fix)
            confidence -= 0.15

        # -----------------------------
        # Validation Checks
        # -----------------------------

        # Column existence checks
        errors.extend(self._validate_columns(query))

        # Data type and function compatibility checks
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
    # Auto-Correction Rules
    # -----------------------------

    def _auto_resolve_column_synonyms(self, query: AnalyticsQuery) -> list[Correction]:
        """
        Resolve semantic synonyms like 'revenue', 'sales', 'count'
        into actual dataset columns using metadata.
        """
        corrections = []

        # Identify available numeric columns for resolution
        numeric_columns = [
            col for col, meta in self.columns.items()
            if meta["semantic_type"] in NUMERIC_TYPES
        ]

        def resolve_numeric_column():
            # Heuristic: search for the best business metric match
            for col in numeric_columns:
                name = col.lower()
                if any(k in name for k in ["sale", "revenue", "amount", "price", "income"]):
                    return col
            return numeric_columns[0] if numeric_columns else None

        # -------- RESOLVE SELECT --------
        if query.select:
            new_select = []
            for col in query.select:
                # If column is missing but matches a synonym
                if col not in self.columns and col.lower() in COLUMN_SYNONYMS["revenue"]:
                    resolved = resolve_numeric_column()
                    if resolved:
                        new_select.append(resolved)
                        corrections.append(
                            Correction(
                                field="select",
                                original=col,
                                corrected=resolved,
                                reason=f"Resolved synonym '{col}' to dataset column '{resolved}'",
                            )
                        )
                        continue
                new_select.append(col)
            query.select = new_select

        # -------- RESOLVE AGGREGATIONS --------
        if query.aggregations:
            for agg in query.aggregations:
                # Resolve the column name
                if agg.column not in self.columns and agg.column.lower() in COLUMN_SYNONYMS["revenue"]:
                    resolved = resolve_numeric_column()
                    if resolved:
                        corrections.append(
                            Correction(
                                field="aggregations.column",
                                original=agg.column,
                                corrected=resolved,
                                reason="Resolved revenue/sales synonym to numeric dataset column",
                            )
                        )
                        agg.column = resolved

                # Resolve the function name (e.g., 'total' -> 'sum' or 'number' -> 'count')
                if agg.function.lower() in COLUMN_SYNONYMS["count"]:
                    agg.function = "count"
                    agg.column = "*"

        return corrections

    def _auto_infer_aggregation(self, query: AnalyticsQuery) -> Correction | None:
        if query.aggregations:
            return None

        inferred_aggs = []
        for col in query.select or []:
            col_meta = self.columns.get(col)
            if col_meta and col_meta["semantic_type"] in NUMERIC_TYPES:
                inferred_aggs.append(Aggregation(column=col, function="sum"))

        if not inferred_aggs:
            inferred_aggs.append(Aggregation(column="*", function="count"))

        query.aggregations = inferred_aggs
        return Correction(
            field="aggregations",
            original=None,
            corrected=[{"column": a.column, "function": a.function} for a in inferred_aggs],
            reason="Aggregation inferred automatically based on selected numeric columns",
        )

    def _auto_correct_group_by(self, query: AnalyticsQuery) -> Correction | None:
        if not query.aggregations or not query.select:
            return None

        missing_group_by = []
        for col in query.select:
            if col in self.columns:
                if self.columns[col]["semantic_type"] not in NUMERIC_TYPES:
                    if col not in (query.group_by or []):
                        missing_group_by.append(col)

        if not missing_group_by:
            return None

        query.group_by = (query.group_by or []) + missing_group_by
        return Correction(
            field="group_by",
            original=None,
            corrected=query.group_by,
            reason="GROUP BY is required for non-aggregated selected columns",
        )

    def _auto_correct_order_by(self, query: AnalyticsQuery) -> Correction | None:
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

    # -----------------------------
    # Validation Checks
    # -----------------------------
    def _validate_columns(self, query: AnalyticsQuery) -> list[str]:
        errors = []
        all_columns = list(self.columns.keys())

        for col in (query.select or []) + (query.group_by or []):
            if col not in self.columns:
                errors.append(f"Column '{col}' not found. Available: {all_columns}")

        for f in query.filters or []:
            if f.column not in self.columns:
                errors.append(f"Filter column '{f.column}' not found. Available: {all_columns}")

        return errors

    def _validate_aggregations(self, query: AnalyticsQuery) -> list[str]:
        errors = []
        for agg in query.aggregations or []:
            col_meta = self.columns.get(agg.column)
            if not col_meta:
                if agg.column == "*": continue
                errors.append(f"Aggregation column '{agg.column}' not found")
                continue

            if col_meta["semantic_type"] not in NUMERIC_TYPES and agg.function.lower() != "count":
                errors.append(f"Cannot apply {agg.function} to non-numeric '{agg.column}'")
        return errors