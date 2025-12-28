# app/agent/intent_classifier.py
import re
from app.agents.intent_types import AgentIntent

class AgentIntentClassifier:
    """
    Decides WHAT the user wants, not HOW to do it.
    """

    PREVIEW_KEYWORDS = [
        "show", "display", "preview", "first", "last", "rows",
        "head", "tail", "sample"
    ]

    ANALYTICS_KEYWORDS = [
        "average", "avg", "sum", "count", "total", "group",
        "by", "top", "max", "min", "distribution"
    ]

    VISUALIZATION_KEYWORDS = [
        "plot", "chart", "graph", "visualize", "bar", "line"
    ]

    PROFILE_KEYWORDS = [
        "summary", "profile", "describe", "schema", "columns",
        "null", "missing", "statistics"
    ]

    def classify(self, text: str) -> AgentIntent:
        if not text or not text.strip():
            return AgentIntent.INVALID

        q = text.lower()

        # visualization has highest priority
        if self._contains(q, self.VISUALIZATION_KEYWORDS):
            return AgentIntent.VISUALIZATION

        # analytics (planner territory)
        if self._contains(q, self.ANALYTICS_KEYWORDS):
            return AgentIntent.ANALYTICS

        # preview (bypass planner)
        if self._contains(q, self.PREVIEW_KEYWORDS):
            return AgentIntent.PREVIEW

        # explicit profiling
        if self._contains(q, self.PROFILE_KEYWORDS):
            return AgentIntent.PROFILE

        return AgentIntent.INVALID

    def _contains(self, text: str, keywords: list[str]) -> bool:
        return any(re.search(rf"\b{k}\b", text) for k in keywords)
