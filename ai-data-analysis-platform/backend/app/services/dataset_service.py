from fastapi import UploadFile
from app.utils.file_utils import validate_file, save_file
from app.services.metadata_service import MetadataService
from app.db.session import SessionLocal

class DatasetService:
    @staticmethod
    def enrich_metadata(dataset_id: str, filename: str, file_path: str):
        # This is a placeholder for your profiling logic
        # In a real scenario, you'd analyze the CSV here
        return {
            "dataset_id": dataset_id,
            "filename": filename,
            "file_path": file_path,
            "dataset_type": "tabular",
            "profiling_summary": {
                "columns": [
                    {"name": "example_col", "dtype": "int", "type": "numeric"}
                ]
            }
        }

def handle_file_upload(file: UploadFile, user_id: str = "default_user"):
    # 1. Validate & save file
    validate_file(file)
    result = save_file(file, user_id)

    # 2. Enrich metadata
    enriched_metadata = DatasetService.enrich_metadata(
        dataset_id=result["dataset_id"],
        filename=result["filename"],
        file_path=result["file_path"],
    )

    # 3. Persist metadata
    db = SessionLocal()
    try:
        # This now matches the class and method name in metadata_service.py
        MetadataService.save_metadata(db, enriched_metadata)
    finally:
        db.close()

    return {
        "dataset_id": result["dataset_id"],
        "filename": result["filename"],
        "dataset_type": enriched_metadata["dataset_type"],
        "message": "File uploaded and metadata enriched successfully"
    }