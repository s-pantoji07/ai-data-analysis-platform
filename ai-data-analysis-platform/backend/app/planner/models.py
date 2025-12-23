from pydantic import BaseModel
# from typing import  Optional

class PlannerRequest(BaseModel):
    dataset_id:str
    intent: str
    