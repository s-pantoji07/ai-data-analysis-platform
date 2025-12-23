import uuid
from sqlalchemy.orm import Session
from app.db.models.dataset import Dataset
from app.db.models.table import DataTable
from app.db.models.column import DataColumn

class MetadataService:
    @staticmethod
    def save_metadata(db: Session, data: dict):
        # ... (Your existing save_metadata code is correct)
        dataset_id = data["dataset_id"]
        filename = data["filename"]
        file_path = data["file_path"]
        dataset_type = data["dataset_type"]
        profiling_summary = data["profiling_summary"]

        dataset = Dataset(
            id=dataset_id,
            filename=filename,
            file_path=file_path,
            dataset_type=dataset_type,
            profiling_summary=profiling_summary
        )

        table = DataTable(
            id=str(uuid.uuid4()),
            name=filename,
            dataset_id=dataset_id
        )

        db.add(dataset)
        db.add(table)
        db.flush()

        for col in profiling_summary["columns"]:
            db.add(DataColumn(
                id=str(uuid.uuid4()),
                name=col["name"],
                dtype=col["dtype"],
                semantic_type=col["type"],
                table_id=table.id
            ))
        db.commit()

    @staticmethod
    def get_dataset_metadata(db: Session, dataset_id: str):
        dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
        if not dataset:
            return None
        
        # --- ALL THE CODE BELOW MUST BE INDENTED ---
        tables = db.query(DataTable).filter(DataTable.dataset_id == dataset_id).all()
        tables_response = []
        
        for table in tables:
            columns = db.query(DataColumn).filter(DataColumn.table_id == table.id).all()
            columns_response = [
                {
                    "column_id": col.id,
                    "name": col.name,
                    "dtype": col.dtype,
                    "semantic_type": getattr(col, "semantic_type", None)
                }
                for col in columns
            ]
            tables_response.append({
                "table_id": table.id,
                "name": table.name,
                "columns": columns_response
            })

        return {
            "dataset_id": dataset.id,
            "filename": dataset.filename,
            "file_path": dataset.file_path,
            "dataset_type": getattr(dataset, "dataset_type", "tabular"),
            "profiling_summary": getattr(dataset, "profiling_summary", {}),
            "tables": tables_response
        }