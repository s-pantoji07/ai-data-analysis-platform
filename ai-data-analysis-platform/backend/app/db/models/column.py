from sqlalchemy import Column, String
from app.db.base import Base

class DataColumn(Base):
    __tablename__ = "columns"

    id = Column(String, primary_key=True)
    name = Column(String)
    dtype = Column(String)
    semantic_type = Column(String)  # numeric / categorical / date
    table_id = Column(String)
