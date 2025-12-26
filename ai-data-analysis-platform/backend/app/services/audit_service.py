from sqlalchemy.orm import Session
from app.db.models.query_audit import QueryAuditLog
from dataclasses import asdict
from typing import Any

class AuditService:
    def __init__(self, db: Session):
        self.db = db

    def _prepare_data(self, data: Any) -> Any:
        if data is None:
            return None
        
    # 1. If it's a dict, we still need to recurse into its values in case 
    # they contain Pydantic models or Dataclasses
        if isinstance(data, dict):
            return {k: self._prepare_data(v) for k, v in data.items()}
        
    # 2. Handle Pydantic Models
        if hasattr(data, "model_dump"):
            return data.model_dump()
        
    # 3. Handle Dataclasses
        if hasattr(data, "__dataclass_fields__"):
            return asdict(data)
        
    # 4. Handle lists
        if isinstance(data, list):
            return [self._prepare_data(item) for item in data]
        
        return data

    def create_audit(self, dataset_id: str, raw_query: str) -> QueryAuditLog:
        audit = QueryAuditLog(
            dataset_id=dataset_id,
            raw_query=raw_query,
            execution_status="PENDING" 
        )
        self.db.add(audit)
        self.db.commit()
        self.db.refresh(audit)
        return audit

    def log_intent(self, audit_id: str, parsed_intent: Any):
        self._update(audit_id, {
            "parsed_intent": self._prepare_data(parsed_intent)
        })

    def log_plan(self, audit_id: str, planned_query: Any):
        self._update(audit_id, {
            "planned_query": self._prepare_data(planned_query)
        })

    def log_validation(
            self,
            audit_id: str,
            corrections: Any = None,
            validation_errors: list = None,
            confidence_score: float = None
            ):
        self._update(audit_id, {
            "corrections": self._prepare_data(corrections),
            "validation_errors": validation_errors,
            "confidence_score": confidence_score
        })

    def log_sql(self, audit_id: str, final_sql: str):
        self._update(audit_id, {"final_sql": final_sql})
        
    def log_execution(self, audit_id: str, status: str, row_count: int = None):
        self._update(audit_id, {
            "execution_status": status,
            "row_count": row_count
        })
    
    def _update(self, audit_id: str, fields: dict):
        self.db.query(QueryAuditLog)\
            .filter(QueryAuditLog.id == audit_id)\
            .update(fields)
        self.db.commit()