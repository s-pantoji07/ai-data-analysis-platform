from fastapi import APIRouter, UploadFile , File
from app.services.dataset_service import handle_file_upload

router = APIRouter(tags=["Upload"])

@router.post("/")

async def upload_dataset(file: UploadFile = File(...)):
    response = handle_file_upload(file)
    return response
