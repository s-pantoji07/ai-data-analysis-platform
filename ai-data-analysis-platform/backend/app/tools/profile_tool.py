from typing import Dict ,Any
from app.services.profile_service import profile_dataset
from app.services.dataset_service import classify_dataset

def profile_tool (dataset_id:str)->Dict[str,Any]:

    profile = profile_dataset(dataset_id)
    classification = classify_dataset(dataset_id)

    return{
        "type":"dataset_profile",
        "dataset_id":dataset_id,
        "essence":classification.get("description"),
        "row_count":profile.get("row_count"),
        "columns":profile.get("columns"),
        "null_stats":profile.get("null_stats"),
        "quick_stats":profile.get("quick_stats")

    }
