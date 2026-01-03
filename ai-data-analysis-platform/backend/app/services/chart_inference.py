from typing import List, Dict

NUMERIC_DTYPES = {"int", "float", "integer", "number"}
TIME_DTYPES = {"date", "datetime", "year"}
CATEGORICAL_DTYPES = {"categorical", "string", "text"}

def infer_chart_types(x_col: Dict, y_col: Dict) -> List[str]:
    x_type = x_col["semantic_type"].lower()
    y_type = y_col["semantic_type"].lower()

    charts = []

    # Y must be numeric for MVP-1
    if y_type in NUMERIC_DTYPES:

        if x_type in CATEGORICAL_DTYPES:
            charts.append("bar")

        elif x_type in TIME_DTYPES:
            charts.extend(["line", "bar"])

        elif x_type in NUMERIC_DTYPES:
            charts.append("line")

    # Deduplicate + stable output
    return sorted(set(charts))


def infer_xy_from_analytics_result(columns: List[Dict]) -> Dict[str, Dict]:
    """
    Dynamically select X and Y for visualization based on column semantic types.

    Rules (MVP-1):
    - If column is numeric → candidate for Y
    - If column is categorical or time → candidate for X
    - Prefer first numeric as Y, first categorical/time as X
    """

    x_col = None
    y_col = None

    for col in columns:
        stype = col.get("semantic_type", "categorical").lower()

        if stype in NUMERIC_DTYPES and y_col is None:
            y_col = col
        elif stype not in NUMERIC_DTYPES and x_col is None:
            x_col = col

    # fallback to first two columns if heuristic fails
    if x_col is None and len(columns) >= 1:
        x_col = columns[0]
    if y_col is None and len(columns) >= 2:
        y_col = columns[1]

    return {"x": x_col, "y": y_col}
