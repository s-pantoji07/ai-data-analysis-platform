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

        # 1. Normalize Column Casing/Spacing (New Rule)
        # This handles "sepal width" -> "sepal_width"
        column_fixes = self._auto_correct_column_names(query)
        if column_fixes:
            corrections.extend(column_fixes)
            confidence -= 0.1

        # 2. Resolve column synonyms (e.g., "sales" -> "Global_Sales")
        synonym_fixes = self._auto_resolve_column_synonyms(query)
        if synonym_fixes:
            corrections.extend(synonym_fixes)
            confidence -= 0.15

        # 3. Aggregation inference
        agg_fix = self._auto_infer_aggregation(query)
        if agg_fix:
            corrections.append(agg_fix)
            confidence -= 0.25

        # 4. GROUP BY auto-correction
        group_by_fix = self._auto_correct_group_by(query)
        if group_by_fix:
            corrections.append(group_by_fix)
            confidence -= 0.2

        # 5. ORDER BY auto-correction
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

    def _auto_correct_column_names(self, query: AnalyticsQuery) -> list[Correction]:
        """
        Normalizes column names by removing underscores and spaces for a fuzzy match.
        Example: "sepal width" -> "sepal_width"
        """
        corrections = []
        
        # Create a map of {normalized_name: actual_name}
        # e.g., {"sepalwidth": "sepal_width"}
        normalized_map = {
            col.lower().replace("_", "").replace(" ", ""): col
            for col in self.columns.keys()
        }

        # Fix aggregations
        if query.aggregations:
            for agg in query.aggregations:
                key = agg.column.lower().replace("_", "").replace(" ", "")
                if key in normalized_map and agg.column != normalized_map[key]:
                    original = agg.column
                    agg.column = normalized_map[key]
                    corrections.append(Correction(
                        field="aggregation.column",
                        original=original,
                        corrected=agg.column,
                        reason="Normalized column name using metadata spacing/casing rules"
                    ))

        # Fix dimensions/select
        if query.select:
            new_select = []
            for col in query.select:
                key = col.lower().replace("_", "").replace(" ", "")
                if key in normalized_map and col != normalized_map[key]:
                    new_select.append(normalized_map[key])
                    corrections.append(Correction(
                        field="select",
                        original=col,
                        corrected=normalized_map[key],
                        reason="Normalized column name"
                    ))
                else:
                    new_select.append(col)
            query.select = new_select

        return corrections

    def _resolve_column_synonym(self, term: str) -> str | None:
        term_l = term.lower()
        for col_name, meta in self.columns.items():
            col_l = col_name.lower()
            if term_l in col_l or col_l in term_l:
                return col_name
            if meta["semantic_type"] in NUMERIC_TYPES:
                if term_l in COLUMN_SYNONYMS["revenue"]:
                    return col_name
        return None

    def _auto_resolve_column_synonyms(self, query: AnalyticsQuery) -> list[Correction]:
        corrections = []
        if query.select:
            new_select = []
            for col in query.select:
                if col not in self.columns:
                    resolved = self._resolve_column_synonym(col)
                    if resolved:
                        new_select.append(resolved)
                        corrections.append(Correction(
                            field="select", original=col, corrected=resolved,
                            reason="Resolved column synonym"
                        ))
                        continue
                new_select.append(col)
            query.select = new_select

        if query.aggregations:
            for agg in query.aggregations:
                if agg.column not in self.columns and agg.column != "*":
                    resolved = self._resolve_column_synonym(agg.column)
                    if resolved:
                        original = agg.column
                        agg.column = resolved
                        corrections.append(Correction(
                            field="aggregations.column", original=original, corrected=resolved,
                            reason="Mapped synonym to physical column"
                        ))
                if agg.function.lower() in COLUMN_SYNONYMS["count"]:
                    agg.function = "count"
        return corrections

    def _auto_infer_aggregation(self, query: AnalyticsQuery) -> Correction | None:
        if query.aggregations:
            return None
        numeric_columns = [c for c, m in self.columns.items() if m["semantic_type"] in NUMERIC_TYPES]
        if not numeric_columns:
            return None
        preferred = [c for c in numeric_columns if any(k in c.lower() for k in ["sales", "amount", "revenue"])]
        target = preferred[0] if preferred else numeric_columns[0]
        query.aggregations = [Aggregation(column=target, function="sum")]
        return Correction(
            field="aggregations", original=None, corrected=[{"column": target, "function": "sum"}],
            reason="Aggregation inferred from numeric measure"
        )

    def _auto_correct_group_by(self, query: AnalyticsQuery) -> Correction | None:
        if not query.aggregations or not query.select:
            return None
        missing_group_by = [col for col in query.select if col in self.columns and self.columns[col]["semantic_type"] not in NUMERIC_TYPES and col not in (query.group_by or [])]
        if not missing_group_by:
            return None
        query.group_by = (query.group_by or []) + missing_group_by
        return Correction(
            field="group_by", original=None, corrected=query.group_by,
            reason="GROUP BY auto-added for non-aggregated columns"
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
                    reason="Corrected order_by to use aggregate function"
                )
        return None

    def _validate_columns(self, query: AnalyticsQuery) -> list[str]:
        errors = []
        for col in (query.select or []) + (query.group_by or []):
            if col not in self.columns:
                errors.append(f"Column '{col}' not found.")
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