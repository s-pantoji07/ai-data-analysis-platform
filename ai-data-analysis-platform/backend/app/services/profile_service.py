from app.utils.dataframe_utils import load_dataframe
from app.tools.profiling import profile_dataframe
from app.tools.dataset_classifier import classify_dataset

def profile_dataset(file_path:str)-> dict:
    df = load_dataframe(file_path)

    profile = profile_dataframe(df)
    dataset_type = classify_dataset(c["name"] for c in profile["columns"])

    return{
        "dataset_type": dataset_type,
        "summary": profile,
    }