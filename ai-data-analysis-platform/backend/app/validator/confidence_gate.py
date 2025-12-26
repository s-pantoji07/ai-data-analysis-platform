from enum import Enum
from pydantic import BaseModel
from app.validator.validation_result import ValidationResult


class ExecutionAction(str, Enum):
    EXECUTE = "EXECUTE"  # Changed to uppercase to match standard BLOCK/EXECUTE patterns
    EXECUTE_WITH_WARNING = "EXECUTE_WITH_WARNING"
    BLOCK = "BLOCK"


class ExecutionDecision(BaseModel):
    action: ExecutionAction
    message: str | None = None


class ConfidenceGate:
    """
    Determines whether a query should be executed,
    executed with warning, or blocked based on confidence.
    """

    HARD_BLOCK_THRESHOLD = 0.5
    WARNING_THRESHOLD = 0.75

    @classmethod
    def decide(cls, result: ValidationResult) -> ExecutionDecision:
        """
        Decide execution strategy based on validation result.
        """

        # 1. ❌ Check for Validation Errors
        # We check if result.errors exists AND is not empty
        if result.errors and len(result.errors) > 0:
            return ExecutionDecision(
                action=ExecutionAction.BLOCK,
                message=f"Validation failed: {', '.join(result.errors)}"
            )

        # 2. Extract confidence
        # Using getattr as a safety net in case of naming mismatches
        confidence = getattr(result, "confidence_score", 0.0)

        # 3. ❌ Low confidence → block
        if confidence < cls.HARD_BLOCK_THRESHOLD:
            return ExecutionDecision(
                action=ExecutionAction.BLOCK,
                message=(
                    f"Confidence score {confidence} is too low. "
                    "The system is unsure of the column mappings or intent."
                )
            )

        # 4. ⚠️ Medium confidence → allow but warn
        if confidence < cls.WARNING_THRESHOLD:
            # Generate a summary of corrections if they exist
            correction_msg = ""
            if result.corrections:
                correction_msg = f" Applied {len(result.corrections)} auto-corrections."

            return ExecutionDecision(
                action=ExecutionAction.EXECUTE_WITH_WARNING,
                message=(
                    f"Proceeding with medium confidence ({confidence})." 
                    f"{correction_msg} Please verify the results."
                )
            )

        # 5. ✅ High confidence → safe to execute
        return ExecutionDecision(
            action=ExecutionAction.EXECUTE,
            message="Query validated successfully."
        )