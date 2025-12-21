from fastapi import UploadFile
from app.utils.file_utils import validate_file, save_file
from app.db.session import SessionLocal
from app.services.metadata_service import create_dataset_metadata
from app.utils.dataframe_utils import load_dataframe


def handle_file_upload(file: UploadFile, user_id:str = "default_user"):
    # Validate the uploaded file
    validate_file(file)
    result = save_file(file , user_id)
   
    db = SessionLocal()
    df= load_dataframe(result["file_path"])

    columns = [{"name": col, "dtype": str(df[col].dtype)} for col in df.columns]

    create_dataset_metadata(
        db=db,
        dataset_id=result["dataset_id"],
        filename=result["filename"],
        file_path=result["file_path"],
        columns=columns
    )

    return {
        "dataset_id": result["dataset_id"],
        "file_name": result["filename"],
        "message":"file uploaded successfully"
    }
    