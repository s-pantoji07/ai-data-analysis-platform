from fastapi import UploadFile
from app.utils.file_utils import validate_file, save_file
from app.services.metadata_service import MetadataService
from app.db.session import SessionLocal
from app.utils.dataframe_utils import load_dataframe
import pandas as pd

class DatasetService:
    @staticmethod
    def enrich_metadata(dataset_id: str, filename: str, file_path: str):
        # Load CSV/Excel into dataframe
        df = load_dataframe(file_path)

        columns_info = []
        numeric_columns = []
        categorical_columns = []
        missing_values = {}

        for col in df.columns:
            dtype = str(df[col].dtype)
            col_type = "numeric" if pd.api.types.is_numeric_dtype(df[col]) else "categorical"

            columns_info.append({
                "name": col,
                "dtype": dtype,
                "type": col_type
            })

            missing_values[col] = int(df[col].isna().sum())

            if col_type == "numeric":
                numeric_columns.append(col)
            else:
                categorical_columns.append(col)

        # Optional: detect date columns
        date_columns = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]

        profiling_summary = {
            "columns": columns_info,
            "missing_values": missing_values,
            "numeric_columns": numeric_columns,
            "categorical_columns": categorical_columns,
            "date_columns": date_columns
        }

        return {
            "dataset_id": dataset_id,
            "filename": filename,
            "file_path": file_path,
            "dataset_type": "tabular",
            "profiling_summary": profiling_summary
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

    # 3. Persist metadata in DB
    db = SessionLocal()
    try:
        MetadataService.save_metadata(db, enriched_metadata)
    finally:
        db.close()

    # 4. Return enriched metadata response
    return {
        "dataset_id": result["dataset_id"],
        "filename": result["filename"],
        "dataset_type": enriched_metadata["dataset_type"],
        "profiling_summary": enriched_metadata["profiling_summary"],
        "message": "File uploaded and metadata enriched successfully"
    }
