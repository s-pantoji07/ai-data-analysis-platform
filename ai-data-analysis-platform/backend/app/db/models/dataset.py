from sqlalchemy import Column, String, JSON
from app.db.base import Base

class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(String, primary_key=True)
    filename = Column(String)
    file_path = Column(String)

    dataset_type = Column(String, nullable=True)
    profiling_summary = Column(JSON, nullable=True)
