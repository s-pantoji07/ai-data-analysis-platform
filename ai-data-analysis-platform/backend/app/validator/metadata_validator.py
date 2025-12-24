from app.db.session import SessionLocal
from app.db.models.column import DataColumn
from app.analytics.query_models import AnalyticsQuery, Aggregation
from app.validator.validation_result import ValidationResult, Correction

NUMERIC_TYPES = {"int", "int64", "float", "float64", "numeric"}

# Generic semantic labels used to map user intent to dataset-specific columns
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

        # 1. Resolve column synonyms using metadata-driven matching
        # This replaces generic terms with actual physical column names
        synonym_fixes = self._auto_resolve_column_synonyms(query)
        if synonym_fixes:
            corrections.extend(synonym_fixes)
            confidence -= 0.15

        # 2. Aggregation inference (Updated with Preferred Metrics logic)
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
        errors.extend(self._validate_columns(query))
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

    def _resolve_column_synonym(self, term: str) -> str | None:
        """
        Metadata-aware synonym resolver.
        Matches terms against actual column names and semantic types.
        """
        term_l = term.lower()

        for col_name, meta in self.columns.items():
            col_l = col_name.lower()

            # Direct partial name match (e.g., "sales" matches "Global_Sales")
            if term_l in col_l or col_l in term_l:
                return col_name

            # Semantic match for common business terms
            if meta["semantic_type"] in NUMERIC_TYPES:
                if term_l in COLUMN_SYNONYMS["revenue"]:
                    return col_name

        return None

    def _auto_resolve_column_synonyms(self, query: AnalyticsQuery) -> list[Correction]:
        """
        Iterates through query fields to correct invalid columns using synonyms.
        """
        corrections = []

        # 1. Resolve SELECT columns
        if query.select:
            new_select = []
            for col in query.select:
                if col not in self.columns:
                    resolved = self._resolve_column_synonym(col)
                    if resolved:
                        new_select.append(resolved)
                        corrections.append(Correction(
                            field="select", original=col, corrected=resolved,
                            reason="Resolved column synonym using dataset metadata"
                        ))
                        continue
                new_select.append(col)
            query.select = new_select

        # 2. Resolve AGGREGATION columns and functions
        if query.aggregations:
            for agg in query.aggregations:
                if agg.column not in self.columns and agg.column != "*":
                    resolved = self._resolve_column_synonym(agg.column)
                    if resolved:
                        original = agg.column
                        agg.column = resolved
                        corrections.append(Correction(
                            field="aggregations.column", original=original, corrected=resolved,
                            reason="Mapped synonym to physical dataset column"
                        ))
                
                # Handle function synonyms (e.g., "total" -> "sum")
                if agg.function.lower() in COLUMN_SYNONYMS["count"]:
                    agg.function = "count"

        return corrections

    def _auto_infer_aggregation(self, query: AnalyticsQuery) -> Correction | None:
        if query.aggregations:
            return None

        numeric_columns = [c for c, m in self.columns.items() if m["semantic_type"] in NUMERIC_TYPES]
        if not numeric_columns:
            return None

        # Heuristic: Prefer common metrics
        preferred = [c for c in numeric_columns if any(k in c.lower() for k in ["sales", "amount", "revenue"])]
        target = preferred[0] if preferred else numeric_columns[0]

        query.aggregations = [Aggregation(column=target, function="sum")]
        return Correction(
            field="aggregations", original=None, corrected=[{"column": target, "function": "sum"}],
            reason="Aggregation inferred from numeric measure and grouping context"
        )

    def _auto_correct_group_by(self, query: AnalyticsQuery) -> Correction | None:
        if not query.aggregations or not query.select:
            return None

        missing_group_by = []
        for col in query.select:
            if col in self.columns and self.columns[col]["semantic_type"] not in NUMERIC_TYPES:
                if col not in (query.group_by or []):
                    missing_group_by.append(col)

        if not missing_group_by:
            return None

        query.group_by = (query.group_by or []) + missing_group_by
        return Correction(
            field="group_by", original=None, corrected=query.group_by,
            reason="GROUP BY is required for non-aggregated selected columns"
        )

    def _auto_correct_order_by(self, query: AnalyticsQuery) -> Correction | None:
        if not query.order_by or not query.aggregations:
            return None
        for agg in query.aggregations:
            if query.order_by == agg.column:
                corrected = f"{agg.function.upper()}({agg.column})"
                query.order_by = corrected
                return Correction(
                    field="order_by", original=agg.column, corrected=corrected,
                    reason="Ordering by aggregated metric requires aggregation function"
                )
        return None

    # -----------------------------
    # Validation Checks
    # -----------------------------
    def _validate_columns(self, query: AnalyticsQuery) -> list[str]:
        errors = []
        all_cols = list(self.columns.keys())
        for col in (query.select or []) + (query.group_by or []):
            if col not in self.columns:
                errors.append(f"Column '{col}' not found. Available: {all_cols}")
        return errors

    def _validate_aggregations(self, query: AnalyticsQuery) -> list[str]:
        errors = []
        for agg in query.aggregations or []:
            meta = self.columns.get(agg.column)
            if not meta:
                if agg.column == "*": continue
                errors.append(f"Aggregation column '{agg.column}' not found")
                continue
            if meta["semantic_type"] not in NUMERIC_TYPES and agg.function.lower() != "count":
                errors.append(f"Cannot apply {agg.function} to non-numeric '{agg.column}'")
        return errors