from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.services.metadata_service import MetadataService

router = APIRouter()

@router.get("/{dataset_id}")
def fetch_metadata(dataset_id: str, db: Session = Depends(get_db)):
    metadata = MetadataService.get_dataset_metadata(db, dataset_id)
    if not metadata:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return metadata
