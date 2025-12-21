from app.db.base import Base
from app.db.session import engine
from app.db.models.dataset import Dataset
from app.db.models.table import DataTable
from app.db.models.column import DataColumn

def init_db():
    Base.metadata.create_all(bind = engine)

if __name__ =="__main__":
    init_db()