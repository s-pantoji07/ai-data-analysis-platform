from pydantic import BaseModel

class PlanRequest(BaseModel):
    dataset_id:str
    intent :str