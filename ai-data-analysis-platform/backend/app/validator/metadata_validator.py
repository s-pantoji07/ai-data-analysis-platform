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
                for col in cols if not col.name.lower().startswith("unnamed")
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
        # ---------- Normalize input ----------
        query_obj = (
            AnalyticsQuery(**query)
            if isinstance(query, dict)
            else query
        )
    
        corrections: list[str] = []
        errors: list[str] = []
        confidence: float = 1.0
    
        # ---------- Step 1: Column normalization ----------
        col_fixes = self._apply_column_normalization(query_obj)
        if col_fixes:
            corrections.extend(col_fixes)
    
        # ---------- Step 2: Structural auto-corrections ----------
        grp_fix = self._auto_correct_group_by(query_obj)
        if grp_fix:
            corrections.append(grp_fix)
    
        ord_fix = self._auto_correct_order_by(query_obj)
        if ord_fix:
            corrections.append(ord_fix)
    
        # ---------- Step 3: Validation ----------
        column_errors = self._validate_columns(query_obj)
        agg_errors = self._validate_aggregations(query_obj)
    
        errors.extend(column_errors)
        errors.extend(agg_errors)
    
        # ---------- Step 4: Confidence scoring ----------
        if corrections:
            confidence -= min(0.3, len(corrections) * 0.05)
    
        if errors:
            confidence -= min(0.5, len(errors) * 0.15)
    
        confidence = round(max(confidence, 0.1), 2)
    
        # ---------- Final result ----------
        return ValidationResult(
            is_valid=len(errors) == 0,
            corrected_query=query_obj,
            corrections=corrections,
            errors=errors,
            confidence_score=confidence,
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

    def _auto_correct_order_by(self, query: AnalyticsQuery):
        if not query.aggregations:
            return None
    
        agg = query.aggregations[0]
        
        # 🛡️ FIX: Use the same logic as the engine to predict the alias
        col_name = "total" if (agg.column == "*" or agg.column.lower().startswith("unnamed")) else agg.column.replace(" ", "_")
        func_name = "COUNT" if agg.column.lower().startswith("unnamed") else agg.function.upper()
        
        alias = f"{func_name}_{col_name}"
    
        if query.order_by != alias:
            original = query.order_by
            query.order_by = alias
            return Correction(
                field="order_by",
                original=original,
                corrected=alias,
                reason="Aligned order_by with calculated alias"
            )
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
            if agg.column == "*": 
                continue
                
            # 🛡️ AUTO-HEAL: If LLM picked an index column, fix it to *
            if agg.column.lower().startswith("unnamed"):
                agg.column = "*"
                continue 
    
            if agg.column not in self.columns:
                errors.append(f"Unknown aggregation column '{agg.column}'.")
        return errors
    
    def _remove_filtered_group_by(self, query: AnalyticsQuery) -> Optional[Correction]:
        if not query.group_by or not query.filters:
            return None
    
        filtered_cols = {f.column for f in query.filters}
        new_group_by = [c for c in query.group_by if c not in filtered_cols]
    
        if new_group_by != query.group_by:
            original = query.group_by
            query.group_by = new_group_by if new_group_by else None
            return Correction(
                field="group_by",
                original=original,
                corrected=query.group_by,
                reason="Removed filtered columns from GROUP BY"
            )
        return None
    