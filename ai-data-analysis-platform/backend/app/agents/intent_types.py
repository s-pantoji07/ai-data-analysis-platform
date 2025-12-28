from enum import Enum

class AgentIntent(str,Enum):
    PROFILE = "profile"
    PREVIEW = "preview"
    ANALYTICS= "analytics"
    VISUALIZATION = "visualization"
    INVALID = "invalid"

    