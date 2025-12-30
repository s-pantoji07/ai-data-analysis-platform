from fastapi import UploadFile
from app.utils.file_utils import validate_file, save_file
from app.services.metadata_service import MetadataService
from app.db.session import SessionLocal
from app.utils.dataframe_utils import load_dataframe
import pandas as pd

class DatasetService:
    @staticmethod
    def enrich_metadata(dataset_id: str, filename: str, file_path: str):
        df = load_dataframe(file_path)

        columns_metadata = []
        missing_values = {}

        for col in df.columns:
            series = df[col]
            dtype = str(series.dtype)
            missing_values[col] = int(series.isna().sum())

            # ---------- Base Type ----------
            if pd.api.types.is_numeric_dtype(series):
                base_type = "numeric"
            elif pd.api.types.is_datetime64_any_dtype(series):
                base_type = "datetime"
            else:
                base_type = "categorical"

            # ---------- Semantic Tags ----------
            semantic_tags = []

            # Metric vs Dimension
            if base_type == "numeric":
                semantic_tags.append("metric")
            else:
                semantic_tags.append("dimension")

            # Time detection
            if base_type == "datetime" or "date" in col.lower() or "time" in col.lower():
                semantic_tags.append("time")

            # Identifier detection
            if col.lower().endswith("id") or col.lower() in ["id", "uuid"]:
                semantic_tags.append("identifier")

            columns_metadata.append({
                "name": col,
                "dtype": dtype,
                "base_type": base_type,
                "semantic_tags": semantic_tags
            })

        profiling_summary = {
            "columns": columns_metadata,
            "missing_values": missing_values
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


def classify_dataset(dataset_id: str) -> dict:
    """
    Lightweight dataset classification for agent usage.
    Uses persisted metadata only (NO file access).
    """
    db = SessionLocal()
    try:
        metadata = MetadataService.get_dataset_metadata(db, dataset_id)
        if not metadata:
            return {"description": "Unknown dataset"}

        profiling = metadata.get("profiling_summary", {})
        all_cols = profiling.get("columns", [])
        
        # Correctly filter based on your 'base_type' logic
        numeric_cols = [c for c in all_cols if c.get("base_type") == "numeric"]
        categorical_cols = [c for c in all_cols if c.get("base_type") == "categorical"]
        date_cols = [c for c in all_cols if c.get("base_type") == "datetime"]

        description_parts = []

        if date_cols:
            description_parts.append("time-based records")
        if categorical_cols:
            description_parts.append(f"{len(categorical_cols)} categorical attributes")
        if numeric_cols:
            description_parts.append(f"{len(numeric_cols)} numeric measures")

        description = "Dataset contains " + ", ".join(description_parts) if description_parts else "Tabular dataset"

        return {
            "dataset_id": dataset_id,
            "description": description,
            "dataset_type": metadata.get("dataset_type", "tabular")
        }
    finally:
        db.close()
