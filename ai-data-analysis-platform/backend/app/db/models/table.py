from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.orm import relationship
from app.db.base import Base

class DataTable(Base):
    __tablename__ = "tables"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    dataset_id = Column(String, ForeignKey("datasets.id"))

    # ADD THIS: Allows table.columns to work
    columns = relationship("DataColumn", back_populates="table", cascade="all, delete-orphan")