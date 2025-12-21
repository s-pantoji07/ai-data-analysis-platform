from sqlalchemy import String,Column,ForeignKey
from app.db.base import Base

class DataColumn(Base):
    __tablename__ = "columns"

    id = Column(String,primary_key = True)
    name= Column(String,nullable = False)
    dtype = Column(String,nullable = False)
    table_id = Column(String,ForeignKey("tables.id"))


    
                 