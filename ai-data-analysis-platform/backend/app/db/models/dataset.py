from sqlalchemy import Column, String,DateTime
from sqlalchemy.sql import func
from app.db.base import Base

class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(String, primary_key =True, index=True)
    filename = Column(String, nullable = False)
    file_path = Column(String,nullable = False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())