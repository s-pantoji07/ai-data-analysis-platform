from app.db.session import SessionLocal
from app.services.audit_service import AuditService

class QueryAuditLogger:
    """
    Thin wrapper around AuditService for agent/tools usage.
    Owns DB session lifecycle.
    """

    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id
        self.db = SessionLocal()
        self.service = AuditService(self.db)
        self.audit = None

    def start(self, raw_query: str) -> str:
        self.audit = self.service.create_audit(
            dataset_id=self.dataset_id,
            raw_query=raw_query
        )
        return self.audit.id

    def log_plan(self, planned_query):
        self.service.log_plan(self.audit.id, planned_query)

    def log_validation(self, corrections=None, errors=None, confidence=None):
        self.service.log_validation(
            self.audit.id,
            corrections=corrections,
            validation_errors=errors,
            confidence_score=confidence
        )

    def log_sql(self, sql: str):
        self.service.log_sql(self.audit.id, sql)

    def finalize(self, status: str, row_count: int):
        self.service.log_execution(
            self.audit.id,
            status=status,
            row_count=row_count
        )
        self.db.close()
