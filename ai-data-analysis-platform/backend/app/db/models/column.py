from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.orm import relationship
from app.db.base import Base

class DataColumn(Base):
    __tablename__ = "columns"

    id = Column(String, primary_key=True)
    name = Column(String)
    dtype = Column(String)
    semantic_type = Column(String) 
    
    # FIX 1: Add ForeignKey so SQLAlchemy knows they are linked
    table_id = Column(String, ForeignKey("tables.id"))

    # FIX 2: Define the 'table' attribute that was missing
    table = relationship("DataTable", back_populates="columns")