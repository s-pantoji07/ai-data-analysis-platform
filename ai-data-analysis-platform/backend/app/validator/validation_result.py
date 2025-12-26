from typing import List, Optional, Any, Union
from pydantic import BaseModel, Field

class Correction(BaseModel):
    field: str
    original: Any
    corrected: Any
    reason: Optional[str] = None

class ValidationResult(BaseModel):
    is_valid: bool
    confidence_score: float 
    corrected_query: Optional[Union[dict, Any]] = None 
    corrections: List[Correction] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    follow_up_questions: List[str] = Field(default_factory=list)

    @property
    def aggregations(self):
        """Allows ConfidenceGate to access aggregations directly."""
        if not self.corrected_query:
            return []
        if isinstance(self.corrected_query, dict):
            return self.corrected_query.get("aggregations", [])
        return getattr(self.corrected_query, "aggregations", [])