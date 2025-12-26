from app.db.session import SessionLocal
from app.db.models.query_audit import QueryAuditLog
import json

def view_logs():
    db = SessionLocal()
    # Fetch the last 5 logs
    logs = db.query(QueryAuditLog).order_by(QueryAuditLog.created_at.desc()).limit(5).all()
    
    print("-" * 100)
    print(f"{'ID':<38} | {'STATUS':<10} | {'RAW QUERY'}")
    print("-" * 100)
    
    for log in logs:
        # Note: Using the typo name from your model 'excetution_status'
        print(f"{log.id:<38} | {log.excetution_status:<10} | {log.raw_query}")
        if log.corrections:
            print(f"  └─ Corrections: {json.dumps(log.corrections, indent=2)}")
    db.close()

if __name__ == "__main__":
    view_logs()