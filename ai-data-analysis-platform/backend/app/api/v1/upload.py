from fastapi import APIRouter, UploadFile, File
from app.services.dataset_service import handle_file_upload
from app.tools.preview_tool import preview_tool

router = APIRouter(tags=["Upload"])

@router.post("/")
async def upload_dataset(file: UploadFile = File(...)):
    upload_response = handle_file_upload(file)

    dataset_id = upload_response["dataset_id"]

    preview = preview_tool(dataset_id)

    return {
        **upload_response,
        "preview": preview
    }
