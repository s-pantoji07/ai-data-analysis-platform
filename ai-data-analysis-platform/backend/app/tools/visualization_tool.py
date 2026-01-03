from typing import Dict, Any, List
from app.services.chart_inference import infer_chart_types


class VisualizationTool:

    @staticmethod
    def generate_chart(
        dataset_id: str,
        x_column: Dict[str, Any],
        y_column: Dict[str, Any],
        data_points: List[Dict[str, Any]],
        requested_chart: str
    ) -> Dict[str, Any]:

        allowed_charts = infer_chart_types(x_column, y_column)

        if requested_chart not in allowed_charts:
            return {
                "type": "error",
                "message": f"Chart '{requested_chart}' not supported",
                "allowed_chart_types": allowed_charts
            }

        return {
            "type": "visualization",  # 🔹 important for frontend
            "dataset_id": dataset_id,
            "chart": {
                "chart_type": requested_chart,
                "title": f"{y_column['name']} by {x_column['name']}",
                "x_axis": {
                    "column": x_column["name"],
                    "semantic_type": x_column["semantic_type"]
                },
                "y_axis": {
                    "column": y_column["name"],
                    "semantic_type": y_column["semantic_type"]
                },
                "allowed_chart_types": allowed_charts
            },
            "data": data_points
        }
