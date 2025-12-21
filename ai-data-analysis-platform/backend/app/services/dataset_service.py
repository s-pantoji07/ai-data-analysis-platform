from fastapi import UploadFile
from app.utils.file_utils import validate_file, save_file

def handle_file_upload(file: UploadFile, user_id:str = "default_user"):
    # Validate the uploaded file
    validate_file(file)
    result = save_file(file , user_id)

    return {
        "dataset_id": result["dataset_id"],
        "file_name": result["filename"],
        "message":"file uploaded successfully"
    }
    