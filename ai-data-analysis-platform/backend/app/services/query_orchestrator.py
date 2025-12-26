from sqlalchemy.orm import Session
from typing import Any

from app.intent.llm_intent_parser import GeminiIntentParser
from app.services.audit_service import AuditService
from app.services.metadata_service import MetadataService
from app.planner.planner import QueryPlanner
from app.analytics.engine import AnalyticsEngine
from app.validator.metadata_validator import MetadataValidator
from app.planner.exceptions import QueryPlanningError

CONFIDENCE_THRESHOLD = 0.7

class QueryOrchestrator:
    def __init__(self, db: Session, gemini_api_key: str):
        self.db = db
        self.audit = AuditService(db)
        self.intent_parser = GeminiIntentParser(api_key=gemini_api_key)
        self.planner = QueryPlanner()
        self.validator = MetadataValidator # Dynamic validator
        self.engine = AnalyticsEngine()

    def execute(self, dataset_id: str, user_query: str):
        # 1. Create audit record
        audit = self.audit.create_audit(dataset_id=dataset_id, raw_query=user_query)

        try:
            # 2. Parse intent via Gemini
            intent = self.intent_parser.parse(user_query=user_query, dataset_id=dataset_id)
            self.audit.log_intent(audit.id, intent)

            # 3. Plan query logic
            metadata = MetadataService.get_dataset_metadata(self.db, dataset_id)
            if not metadata:
                raise QueryPlanningError("Dataset metadata not found")

            # We pass the intent object; the planner will handle extraction
            plan = self.planner.plan(metadata, intent)
            self.audit.log_plan(audit.id, plan)

            # 4. Run through dynamic validator
            val_instance = self.validator(dataset_id)
            validation = val_instance.validate(plan)

            self.audit.log_validation(
                audit.id,
                corrections=validation.corrections,
                validation_errors=validation.errors,
                confidence_score=validation.confidence_score
            )

            # 🚨 Confidence Gate
            if validation.confidence_score < CONFIDENCE_THRESHOLD:
                self.audit.log_execution(audit.id, status="NEEDS_CLARIFICATION")
                return {
                    "status": "needs_clarification",
                    "audit_id": audit.id,
                    "confidence": validation.confidence_score,
                    "questions": getattr(validation, "follow_up_questions", [])
                }

            # 5. Execute on DuckDB
            # We pass the corrected_query dictionary from validation
            result = self.engine.execute(validation.corrected_query)

            self.audit.log_execution(
                audit.id, 
                status="SUCCESS", 
                row_count=len(result) if isinstance(result, list) else 0
            )

            return {
                "status": "success",
                "audit_id": audit.id,
                "confidence": validation.confidence_score,
                "data": result
            }

        except Exception as e:
            self.audit.log_execution(audit.id, status="FAILED")
            raise e