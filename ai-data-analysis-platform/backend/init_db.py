from app.db.base import Base
from app.db.session import engine
# You must import your models here so SQLAlchemy knows they exist
from app.db.models.query_audit import QueryAuditLog 

def init():
    print("Dropping existing tables...")
    # WARNING: This deletes all data in the tables!
    Base.metadata.drop_all(bind=engine) 
    
    print("Creating database tables with corrected schema...")
    Base.metadata.create_all(bind=engine)
    print("Done! Tables created correctly.")

if __name__ == "__main__":
    init() # This MUST be at the very bottom