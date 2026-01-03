from typing import Dict,Any
import duckdb
from app.analytics.engine import AnalyticsEngine

DEFAULT_PREVIEW_ROWS = 5

def preview_tool(dataset_id :str,limit :int = DEFAULT_PREVIEW_ROWS)->Dict[str,Any]:
    engine = AnalyticsEngine()

    table_name = engine._load_dataset(dataset_id)

    df = engine.con.execute(
        f'SELECT * FROM "{table_name}" LIMIT {limit}'
    ).df()

    return {
        "type":"preview_result",
        "dataset_id": dataset_id,
        "columns":list(df.columns),
        "rows":df.to_dict(orient ="records"),
        "row_count":len(df),
        "columns_count":len(df.columns)
    }