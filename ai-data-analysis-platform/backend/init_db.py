from app.db.base import Base
from app.db.session import engine
# You must import your models here so SQLAlchemy knows they exist
from app.db.models.query_audit import QueryAuditLog 

def init():
    print("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    print("Done! Tables created.")

if __name__ == "__main__":
    init()