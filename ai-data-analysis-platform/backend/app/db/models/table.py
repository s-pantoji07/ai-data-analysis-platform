from sqlalchemy import Column,String, ForeignKey
from app.db.base import Base

class DataTable(Base):
    __tablename__ = "tables"

    id =Column(String, primary_key = True)
    name=Column(String, nullable=False)
    dataset_id = Column(String, ForeignKey("datasets.id"))