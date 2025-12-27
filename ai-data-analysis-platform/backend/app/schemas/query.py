from pydantic import BaseModel

class QueryRequest(BaseModel):
    dataset_id : str
    intent : str