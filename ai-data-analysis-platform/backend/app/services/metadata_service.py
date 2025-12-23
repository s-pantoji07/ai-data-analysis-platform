import uuid
from sqlalchemy.orm import Session
from app.db.models.dataset import Dataset
from app.db.models.table import DataTable
from app.db.models.column import DataColumn

class MetadataService:
    @staticmethod
    def save_metadata(
        db: Session, 
        data: dict
    ):
        # Extract fields from the data dictionary passed from DatasetService
        dataset_id = data["dataset_id"]
        filename = data["filename"]
        file_path = data["file_path"]
        dataset_type = data["dataset_type"]
        profiling_summary = data["profiling_summary"]

        # 1. Dataset
        dataset = Dataset(
            id=dataset_id,
            filename=filename,
            file_path=file_path,
            dataset_type=dataset_type,
            profiling_summary=profiling_summary
        )

        # 2. Table
        table = DataTable(
            id=str(uuid.uuid4()),
            name=filename,
            dataset_id=dataset_id
        )

        db.add(dataset)
        db.add(table)
        db.flush()

        # 3. Columns
        for col in profiling_summary["columns"]:
            db.add(DataColumn(
                id=str(uuid.uuid4()),
                name=col["name"],
                dtype=col["dtype"],
                semantic_type=col["type"],
                table_id=table.id
            ))

        db.commit()