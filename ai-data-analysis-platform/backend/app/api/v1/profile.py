from fastapi import APIRouter, Query, HTTPException
from app.services.profile_service import profile_dataset
import os

router = APIRouter(tags=["Profiling"])


@router.get("/")
def profile_dataset_api(file_path: str = Query(...)):
    if not file_path.startswith("uploads/"):
        raise HTTPException(status_code=400, detail="Invalid file path")

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    return profile_dataset(file_path)
