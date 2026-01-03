from dataclasses import dataclass
from typing import Optional, Dict
from app.agents.intent_types import AgentIntent

@dataclass
class IntentResult:
    intent: AgentIntent
    raw_query: str  # Added to carry the user's text to the next stage
    chart_type: Optional[str] = None
    x_column: Optional[Dict] = None
    y_column: Optional[Dict] = None