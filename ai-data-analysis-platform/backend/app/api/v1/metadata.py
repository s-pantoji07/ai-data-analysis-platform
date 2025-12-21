from fastapi import APIRouter
from app.db.session import SessionLocal
from app.db.models.dataset import Dataset

router = APIRouter(tags=["Metadata"])

@router.get("/{dataset_id}")

def get_dataset_metadata(dataset_id: str):
    db = SessionLocal()

    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()

    if not dataset:
        return {"error": "Dataset not found"}
    
    return {
        "dataset_id": dataset.id,
        "filename": dataset.filename,
        "file_path": dataset.file_path,
    }