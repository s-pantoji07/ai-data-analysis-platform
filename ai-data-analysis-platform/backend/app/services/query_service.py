from app.intent.llm_intent_parser import GeminiIntentParser
from app.services.planner_service import plan_query
from app.analytics.engine import AnalyticsEngine
from app.services.audit_service import AuditService
from app.db.session import SessionLocal

engine = AnalyticsEngine()
parser = GeminiIntentParser()

def execute_intent_query(dataset_id: str, intent: str):
    db = SessionLocal()
    audit_service = AuditService(db)
    audit_id = None

    try:
        audit_record = audit_service.create_audit(dataset_id, intent)
        audit_id = audit_record.id

        # Parse with LLM
        parsed_intent = parser.parse(intent, dataset_id)
        audit_service.log_intent(audit_id, parsed_intent)

        validation_result = plan_query(parsed_intent)
        planned_query = validation_result.corrected_query

        audit_service.log_plan(audit_id, planned_query)
        audit_service.log_validation(
            audit_id=audit_id,
            corrections=validation_result.corrections,
            confidence_score=validation_result.confidence_score
        )

        execution_result = engine.execute(planned_query)

        sql_generated = execution_result.get("sql_generated")
        data = execution_result.get("data", [])

        audit_service.log_sql(audit_id, sql_generated)
        audit_service.log_execution(audit_id, status="SUCCESS", row_count=len(data))

        return {
            "status": "success",
            "audit_id": audit_id,
            "confidence": validation_result.confidence_score,
            "sql": sql_generated,
            "row_count": len(data),
            "data": data,
            "type": execution_result.get("type")
        }

    except Exception as e:
        if audit_id:
            audit_service.log_execution(audit_id, status="FAILED")
        raise e

    finally:
        db.close()
