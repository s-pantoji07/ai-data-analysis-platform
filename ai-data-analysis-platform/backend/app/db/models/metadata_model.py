from datetime import datetime
from typing import Dict , Any,Optional

def create_metadata_model(
        dataset_id: str,
        filename : str,
        file_path : str,
        dataset_type : Optional[str]=None,
        summary:Optional[Dict[str, Any]]=None,

)-> Dict[str,Any]:
    
    return{
        "dataset_id": dataset_id,
        "filename": filename,
        "file_path": file_path,
        "dataset_type": dataset_type,
        "summary": summary,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    }