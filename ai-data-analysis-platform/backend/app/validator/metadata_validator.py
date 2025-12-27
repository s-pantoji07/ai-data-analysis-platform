import difflib
from typing import Any, List, Union, Optional
from app.db.session import SessionLocal
from app.db.models.column import DataColumn
from app.analytics.query_models import AnalyticsQuery, Aggregation
from app.validator.validation_result import ValidationResult, Correction

NUMERIC_TYPES = {"int", "int64", "float", "float64", "numeric", "decimal", "numeric"}

class MetadataValidator:
    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id
        self.columns = self._load_metadata()
        self._norm_to_real = {
            self._normalize_str(col): col for col in self.columns.keys()
        }

    def _load_metadata(self) -> dict:
        db = SessionLocal()
        try:
            cols = (
                db.query(DataColumn)
                .join(DataColumn.table)
                .filter(DataColumn.table.has(dataset_id=self.dataset_id))
                .all()
            )
            # We assume your DataColumn model has a 'sample_data' or similar field
            return {
                col.name: {
                    "dtype": str(col.dtype).lower(),
                    "semantic_type": str(col.semantic_type).lower(),
                    "samples": getattr(col, 'samples', "N/A") 
                }
                for col in cols
            }
        except Exception:
            return {} 
        finally:
            db.close()

    def _normalize_str(self, s: str) -> str:
        if not s: return ""
        s = s.lower().replace("_", "").replace(" ", "").strip()
        suffixes = ["cm", "mm", "id", "amount", "qty", "val"]
        for suffix in suffixes:
            if s.endswith(suffix) and len(s) > len(suffix):
                s = s[:-len(suffix)]
        return s

    def _get_closest_match(self, term: str) -> Optional[str]:
        if not term or term == "*": return term
        if term in self.columns: return term
        norm_term = self._normalize_str(term)
        if norm_term in self._norm_to_real:
            return self._norm_to_real[norm_term]
        matches = difflib.get_close_matches(term, self.columns.keys(), n=1, cutoff=0.7)
        return matches[0] if matches else None

    def validate(self, query: Union[AnalyticsQuery, dict]) -> ValidationResult:
        if isinstance(query, dict):
            query_obj = AnalyticsQuery(**query)
        else:
            query_obj = query

        corrections = []
        errors = []
        confidence = 1.0

        # Step 1: Normalize column names
        corrections.extend(self._apply_column_normalization(query_obj))
        
        # Step 2: Auto-correct structural requirements (GROUP BY, ORDER BY)
        grp_fix = self._auto_correct_group_by(query_obj)
        if grp_fix: corrections.append(grp_fix)
    
        ord_fix = self._auto_correct_order_by(query_obj)
        if ord_fix: corrections.append(ord_fix)

        # Step 3: Check validity
        errors.extend(self._validate_columns(query_obj))
        errors.extend(self._validate_aggregations(query_obj))
    
        confidence -= (len(corrections) * 0.1)
    
        return ValidationResult(
            is_valid=len(errors) == 0,
            corrected_query=query_obj.model_dump(), 
            corrections=corrections,
            errors=errors,
            confidence_score=round(max(confidence, 0.1), 2),
        )

    def _apply_column_normalization(self, query: AnalyticsQuery) -> List[Correction]:
        corrections = []
        def fix(val: str, path: str) -> str:
            match = self._get_closest_match(val)
            if match and match != val:
                corrections.append(Correction(field=path, original=val, corrected=match, reason="Normalized to schema"))
                return match
            return val

        if query.select:
            query.select = [fix(c, "select") for c in query.select]
        if query.aggregations:
            for agg in query.aggregations:
                agg.column = fix(agg.column, "aggregations.column")
        if query.filters:
            for f in query.filters:
                f.column = fix(f.column, "filters.column")
        if query.group_by:
            query.group_by = [fix(c, "group_by") for c in query.group_by]
        return corrections

    def _auto_correct_group_by(self, query: AnalyticsQuery) -> Optional[Correction]:
        if not query.aggregations: return None
        
        # SQL Rule: Any non-aggregated column in SELECT must be in GROUP BY
        dims = []
        if query.select:
            dims = [c for c in query.select if c in self.columns and self.columns[c]["semantic_type"] not in NUMERIC_TYPES]
        
        # Also ensure columns in group_by are actually valid
        current_gb = query.group_by or []
        missing = [d for d in dims if d not in current_gb]
        
        if not missing: return None
        query.group_by = list(set(current_gb + missing))
        return Correction(field="group_by", original=None, corrected=query.group_by, reason="Added missing group_by")

    def _auto_correct_order_by(self, query: AnalyticsQuery) -> Optional[Correction]:
        if not query.order_by or not query.aggregations: return None
        for agg in query.aggregations:
            if query.order_by == agg.column:
                query.order_by = f"{agg.function.upper()}({agg.column})"
                return Correction(field="order_by", original=agg.column, corrected=query.order_by, reason="Aggregated order_by")
        return None

    def _validate_columns(self, query: AnalyticsQuery) -> List[str]:
        errors = []
        cols = set(query.select or []) | set(query.group_by or [])
        if query.filters: cols |= {f.column for f in query.filters}
        for col in cols:
            if col not in self.columns: errors.append(f"Column '{col}' not found.")
        return errors

    def _validate_aggregations(self, query: AnalyticsQuery) -> List[str]:
        errors = []
        for agg in (query.aggregations or []):
            if agg.column == "*": continue
            if agg.column not in self.columns:
                errors.append(f"Unknown aggregation column '{agg.column}'.")
            elif self.columns[agg.column]["semantic_type"] not in NUMERIC_TYPES and agg.function.lower() not in ["count", "distinct_count"]:
                errors.append(f"Cannot apply {agg.function} to non-numeric {agg.column}.")
        return errors