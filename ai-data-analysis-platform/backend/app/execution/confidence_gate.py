from app.execution.confidence_policy import ConfidencePolicy
from app.execution.execution_decision import ExecutionDecision, ExecutionAction
from app.validator.validation_result import ValidationResult

class ConfidenceGate:

    @staticmethod
    def decide(result: ValidationResult) -> ExecutionDecision:
        score = result.confidence_score

        if score >= ConfidencePolicy.EXECUTE_THRESHOLD:
            return ExecutionDecision(
                action=ExecutionAction.EXECUTE
            )

        if score >= ConfidencePolicy.WARN_THRESHOLD:
            return ExecutionDecision(
                action=ExecutionAction.EXECUTE_WITH_WARNING,
                message=ConfidenceGate._warning_message(result)
            )

        return ExecutionDecision(
            action=ExecutionAction.BLOCK,
            message=ConfidenceGate._block_message(result)
        )

    @staticmethod
    def _warning_message(result: ValidationResult) -> str:
        return (
            "Query was auto-corrected with moderate confidence.\n"
            f"Corrections applied: {[c.reason for c in result.corrections]}"
        )

    @staticmethod
    def _block_message(result: ValidationResult) -> str:
        return (
            "Unable to safely execute query.\n"
            f"Issues detected: {result.errors or 'Ambiguous intent'}"
        )
