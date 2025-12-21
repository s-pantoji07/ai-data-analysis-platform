import uuid
from sqlalchemy.orm import Session
from app.db.models.dataset import Dataset
from app.db.models.table import DataTable
from app.db.models.column import DataColumn

def create_dataset_metadata(
        db: Session,
        dataset_id: str,
        filename: str,
        file_path: str,
        columns: list[dict]
):
    dataset = Dataset(
        id = dataset_id,
        filename = filename,
        file_path = file_path
    )

    table_id = str(uuid.uuid4()) # Renamed variable for clarity
    table = DataTable(
        id = table_id,
        name = filename,
        dataset_id = dataset_id
    )

    db.add(dataset)
    db.add(table)
    
    # --- IMPORTANT STEP ---
    # This sends the dataset and table to the DB buffer 
    # so the Foreign Key exists when we add columns next.
    db.flush() 

    for col in columns:
        db.add(DataColumn(
            id = str(uuid.uuid4()),
            name = col['name'],
            dtype = col['dtype'],
            table_id = table.id
        ))

    db.commit()