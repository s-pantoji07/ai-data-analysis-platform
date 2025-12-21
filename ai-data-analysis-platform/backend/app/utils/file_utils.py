import os
import uuid
from fastapi import UploadFile, HTTPException

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}

UPLOAD_ROOT = "uploads"


def validate_file(file: UploadFile):
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail="Only CSV and Excel files are supported"
        )


def save_file(file: UploadFile, user_id: str = "default_user"):
    dataset_id = str(uuid.uuid4())

    user_dir = os.path.join(UPLOAD_ROOT, user_id, dataset_id)
    os.makedirs(user_dir, exist_ok=True)

    file_path = os.path.join(user_dir, file.filename)

    with open(file_path, "wb") as f:
        f.write(file.file.read())

    return {
        "dataset_id": dataset_id,
        "file_path": file_path,
        "filename": file.filename
    }
