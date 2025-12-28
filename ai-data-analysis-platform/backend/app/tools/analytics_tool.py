from typing import Dict, Any
from app.planner.planner import QueryPlanner
from app.validator.metadata_validator import MetadataValidator
from app.analytics.engine import AnalyticsEngine
from app.audit.query_audit_log import QueryAuditLogger
from app.services.metadata_service import MetadataService
from app.db.session import SessionLocal

def analytics_tool(dataset_id: str, intent: str) -> Dict[str, Any]:
    planner = QueryPlanner()
    engine = AnalyticsEngine()
    validator = MetadataValidator(dataset_id)
    audit = QueryAuditLogger(dataset_id)

    # --- Load metadata correctly ---
    db = SessionLocal()
    metadata = MetadataService.get_dataset_metadata(db, dataset_id)
    db.close()

    # --- Start audit ---
    audit_id = audit.start(raw_query=intent)

    # --- Planning ---
    planned_query = planner.plan(
        metadata=metadata,
        intent=intent
    )
    audit.log_plan(planned_query)

    # --- Validation ---
    validation = validator.validate(planned_query)
    audit.log_validation(
        corrections=validation.corrections,
        errors=validation.errors,
        confidence=validation.confidence_score
    )

    if not validation.is_valid:
        audit.finalize(status="FAILED", row_count=0)
        raise ValueError(validation.errors)

    # --- Execution ---
    result = engine.execute(validation.corrected_query)
    audit.log_sql(result.sql)
    audit.finalize(status="SUCCESS", row_count=result.row_count)

    return {
        "type": "query_result",
        "audit_id": audit_id,
        "confidence": validation.confidence_score,
        "sql": result.sql,
        "row_count": result.row_count,
        "data": result.data,
    }
