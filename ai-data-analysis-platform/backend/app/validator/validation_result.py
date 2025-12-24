from typing import List, Optional
from app.analytics.query_models import AnalyticsQuery

class ValidationResult:
    def __init__(
        self,
        is_valid: bool,
        query: AnalyticsQuery,
        corrections: Optional[List[str]] = None,
        confidence_score: float = 1.0,
    ):
        self.is_valid = is_valid
        self.query = query
        self.corrections = corrections or []
        self.confidence_score = confidence_score
