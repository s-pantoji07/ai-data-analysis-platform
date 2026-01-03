from typing import Dict, Any
from app.analytics.engine import AnalyticsEngine
from app.services.dataset_service import get_dataset_metadata

DEFAULT_PREVIEW_ROWS = 5

def preview_tool(dataset_id: str, limit: int = DEFAULT_PREVIEW_ROWS) -> Dict[str, Any]:
    engine = AnalyticsEngine()

    table_name = engine._load_dataset(dataset_id)

    df = engine.con.execute(
        f'SELECT * FROM "{table_name}" LIMIT {limit}'
    ).df()

    # Fetch stored profiling metadata
    metadata = get_dataset_metadata(dataset_id) or {}

    missing_summary = (
        metadata
        .get("profiling_summary", {})
        .get("missing_values", {})
    )

    missing_columns = [
        col for col, count in missing_summary.items() if count > 0
    ]

    return {
        "type": "preview_result",
        "dataset_id": dataset_id,
        "columns": list(df.columns),
        "rows": df.to_dict(orient="records"),
        "row_count": len(df),
        "column_count": len(df.columns),
        "has_missing_values": len(missing_columns) > 0,
        "missing_columns": missing_columns
    }
