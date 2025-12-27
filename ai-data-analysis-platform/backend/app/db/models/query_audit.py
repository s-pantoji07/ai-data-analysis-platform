import uuid 
from datetime import datetime
from sqlalchemy import Column,String,Float,DateTime,JSON
from app.db.base import Base
class QueryAuditLog(Base):
    __tablename__ = "query_audit_logs"

    id = Column(String,primary_key=True,default = lambda :str(uuid.uuid4()))

    #dataset---
    dataset_id = Column(String,nullable = False)
    
    #userinput
    raw_query =Column(String ,nullable = False)

    #intent parser
    parsed_intent = Column(JSON,nullable = True)

    #planned query
    planned_query = Column(JSON,nullable = True)

    #validation layer
    corrections = Column(JSON,nullable = True)
    validation_errors = Column(JSON,nullable = True)
    confidence_score = Column(Float,nullable = True)


    #excution layer
    final_sql = Column(String,nullable = True)
    execution_status = Column(String ,default = "PENDING")
    row_count = Column(Float,nullable = True)

    #metadata
    created_at = Column(DateTime,default = datetime.utcnow)






