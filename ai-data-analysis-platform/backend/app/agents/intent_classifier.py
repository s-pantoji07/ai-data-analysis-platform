import re
from app.agents.intent_types import AgentIntent
from app.agents.intent_result import IntentResult

class AgentIntentClassifier:
    """
    Classifies intent: PROFILE, PREVIEW, VISUALIZATION by keywords.
    Analytics is handled entirely by LLM parser.
    """

    PREVIEW_KEYWORDS = ["show", "display", "preview", "rows", "head", "tail", "sample"]
    VISUALIZATION_KEYWORDS = ["plot", "chart", "graph", "visualize", "bar", "line"]
    PROFILE_KEYWORDS = ["summary", "profile", "describe", "schema", "columns", "null", "missing", "statistics"]

    def classify(self, text: str) -> IntentResult:
        print(f"[DEBUG] classify() called with text: '{text}'")

        if not text or not text.strip():
            print("[DEBUG] Empty or whitespace text, returning INVALID")
            # 🛡️ FIX: Added raw_query=""
            return IntentResult(intent=AgentIntent.INVALID, raw_query="")

        q = text.lower()
        print(f"[DEBUG] Lowercased text: '{q}'")

        # VISUALIZATION
        if self._contains(q, self.VISUALIZATION_KEYWORDS):
            chart_type = self._extract_chart_type(q)
            print(f"[DEBUG] Detected VISUALIZATION intent, chart_type: {chart_type}")
            # 🛡️ FIX: Pass 'text' as raw_query
            return IntentResult(
                intent=AgentIntent.VISUALIZATION, 
                raw_query=text, 
                chart_type=chart_type
            )

        # PREVIEW
        if self._contains(q, self.PREVIEW_KEYWORDS):
            print("[DEBUG] Detected PREVIEW intent")
            # 🛡️ FIX: Pass 'text' as raw_query
            return IntentResult(intent=AgentIntent.PREVIEW, raw_query=text)

        # PROFILE
        if any(k in q for k in self.PROFILE_KEYWORDS):
            print("[DEBUG] Detected PROFILE intent")
            # 🛡️ FIX: Pass 'text' as raw_query
            return IntentResult(intent=AgentIntent.PROFILE, raw_query=text)

        # Fallback to analytics (LLM)
        print("[DEBUG] No keywords matched, returning ANALYTICS for LLM parser")
        # 🛡️ FIX: Pass 'text' as raw_query
        return IntentResult(intent=AgentIntent.ANALYTICS, raw_query=text)

    def _extract_chart_type(self, text: str) -> str:
        if re.search(r"\bbar\b", text): return "bar"
        if re.search(r"\bline\b", text): return "line"
        return "bar"

    def _contains(self, text: str, keywords: list[str]) -> bool:
        for k in keywords:
            if re.search(rf"\b{k}\b", text):
                print(f"[DEBUG] Keyword match found: '{k}'")
                return True
        return False


# Singleton instance
_classifier = AgentIntentClassifier()

def classify_intent(text: str) -> IntentResult:
    result = _classifier.classify(text)
    print(f"[DEBUG] classify_intent() result: {result}")
    return result