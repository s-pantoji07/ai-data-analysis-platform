from app.analytics.engine import AnalyticsEngine
from app.analytics.query_models import AnalyticsQuery, Filter, Aggregation

# Initialize engine
engine = AnalyticsEngine()

# Dataset ID example (Ensure this exists in your DB)
DATASET_ID = "237d5fa8-c5b9-4ef6-9709-aa2ca7dbc4ed"

def print_result(title, result, limit=5):
    print(f"\n{title}")
    # Check if the engine returned a profiling dictionary or a data list
    if isinstance(result, dict):
        print("Structure: Dictionary (Profiling/Metadata)")
        # Print a snippet of the profiling summary instead of slicing the dict
        cols = result.get("profiling_summary", {}).get("columns", [])
        print(f"Columns Found: {[c['name'] for c in cols[:limit]]}...")
    elif isinstance(result, list):
        print(f"Structure: List (Query Results) - Showing first {limit}")
        for row in result[:limit]:
            print(row)
    else:
        print(result)

# -------------------------------
# 1. Select all (Triggers profiling because query is empty)
# -------------------------------
query1 = AnalyticsQuery(dataset_id=DATASET_ID)
result1 = engine.execute(query1)
print_result("=== 1. Profiling / Select All ===", result1)

# -------------------------------
# 2. Filter query
# -------------------------------
query2 = AnalyticsQuery(
    dataset_id=DATASET_ID,
    filters=[Filter(column="NA_Sales", operator=">", value=1)],
    limit=5
)
result2 = engine.execute(query2)
print_result("=== 2. Filtered Query: NA_Sales > 1 ===", result2)

# -------------------------------
# 3. Aggregation query
# -------------------------------
query3 = AnalyticsQuery(
    dataset_id=DATASET_ID,
    aggregations=[Aggregation(column="Global_Sales", function="sum")]
)
result3 = engine.execute(query3)
print_result("=== 3. Aggregation Query: SUM(Global_Sales) ===", result3)

# -------------------------------
# 4. Group By + Aggregation
# -------------------------------
query4 = AnalyticsQuery(
    dataset_id=DATASET_ID,
    group_by=["Year"],
    aggregations=[Aggregation(column="Global_Sales", function="sum")]
)
result4 = engine.execute(query4)
print_result("=== 4. Group By Year + SUM(Global_Sales) ===", result4, limit=10)

# -------------------------------
# 5. Complex query: Filter + GroupBy + Aggregation
# -------------------------------
query5 = AnalyticsQuery(
    dataset_id=DATASET_ID,
    filters=[Filter(column="NA_Sales", operator=">", value=1)],
    group_by=["Genre"],
    aggregations=[Aggregation(column="Global_Sales", function="sum")],
    limit=10
)
result5 = engine.execute(query5)
print_result("=== 5. Complex Query: NA_Sales > 1 by Genre ===", result5)