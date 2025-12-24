from dataclasses import dataclass
from enum import Enum

class ExecutionAction(Enum):
    EXCUTE = "execute"
    EXECUTE_WITH_WARNING = "execute_with_warning"
    BLOCK="block"


@dataclass
class ExcecutionDecision:
    action :ExecutionAction
    message :str |None = None

    