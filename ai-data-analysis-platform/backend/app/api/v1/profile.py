from fastapi import APIRouter, Query, HTTPException
from app.services.profile_service import profile_dataset
import os

from app.db.session import SessionLocal
from app.db.models.dataset import Dataset

router = APIRouter(tags=["Profiling"])


@router.get("/")
def profile_dataset_api(dataset_id:str):
    db = SessionLocal()

    dataset=db.query(Dataset).filter(Dataset.id == dataset_id).first()
    

    return profile_dataset(dataset.file_path)
