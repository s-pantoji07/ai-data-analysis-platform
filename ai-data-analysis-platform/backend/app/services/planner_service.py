from app.db.session import SessionLocal
from app.services.metadata_service import MetadataService
from app.planner.planner import QueryPlanner
from app.planner.exceptions import QueryPlanningError


def plan_query(dataset_id: str, intent: str):
    db = SessionLocal()
    try:
        metadata = MetadataService.get_dataset_metadata(db, dataset_id)
        if not metadata:
            raise QueryPlanningError("Dataset metadata not found")

        planner = QueryPlanner()
        return planner.plan(metadata, intent)

    finally:
        db.close()
