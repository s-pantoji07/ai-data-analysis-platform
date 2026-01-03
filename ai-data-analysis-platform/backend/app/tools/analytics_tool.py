from typing import Dict, Any
from app.planner.planner import QueryPlanner
from app.intent.llm_intent_parser import GeminiIntentParser
from app.validator.metadata_validator import MetadataValidator
from app.analytics.engine import AnalyticsEngine
from app.services.metadata_service import MetadataService
from app.db.session import SessionLocal

def analytics_tool(dataset_id: str, intent_obj: Any) -> Dict[str, Any]:
    planner = QueryPlanner()
    parser = GeminiIntentParser()
    engine = AnalyticsEngine()
    validator = MetadataValidator(dataset_id)

    # 1. Detailed Parsing (LLM deep dive)
    detailed_intent = parser.parse(intent_obj.raw_query, dataset_id)

    # 2. Load Metadata
    db = SessionLocal()
    metadata = MetadataService.get_dataset_metadata(db, dataset_id)
    db.close()

    # 3. Planning & 4. Validation
    planned_query = planner.plan(metadata=metadata, intent=detailed_intent)
    validation = validator.validate(planned_query)
    
    if not validation.is_valid:
        raise ValueError(f"Query validation failed: {validation.errors}")

    # 5. Execution
    result = engine.execute(validation.corrected_query)
    data = result.get("data", [])
    
    # 6. Determine Response Type
    is_visual = (intent_obj.intent.value == "visualization" or 
                 detailed_intent.intent_type == "visualization")
    
    # 7. Generate Column Metadata dynamically from data
    # This ensures semantic_type is accurate for the UI
    columns_metadata = []
    if data:
        first_row = data[0]
        for key, value in first_row.items():
            sem_type = "number" if isinstance(value, (int, float)) else "categorical"
            columns_metadata.append({"name": key, "semantic_type": sem_type})

    # 8. Format Output
    response = {
        "type": "visualization" if is_visual else "query_result",
        "confidence": validation.confidence_score,
        "sql": result.get("sql_generated"),
        "data": data,
        "row_count": result.get("row_count", 0),
        "columns": columns_metadata
    }

    # 9. Chart-Specific Metadata Refinement
    if is_visual:
      response["chart_type"] = getattr(intent_obj, "chart_type", "bar")
      
      # Identify the X-Axis
      if detailed_intent.dimensions:
          response["x_axis"] = detailed_intent.dimensions[0]
      else:
          # Fallback to the first categorical column
          cats = [c["name"] for c in columns_metadata if c["semantic_type"] == "categorical"]
          response["x_axis"] = cats[0] if cats else columns_metadata[0]["name"]
    
      # Identify the Y-Axis (The Metric)
      # 🛡️ FIX: Prioritize columns that are aggregations
      agg_indicators = ["SUM_", "AVG_", "COUNT_", "MAX_", "MIN_", "total"]
      agg_cols = [c["name"] for c in columns_metadata if any(ind in c["name"] for ind in agg_indicators)]
      
      # Filter numeric columns that are NOT the x_axis
      other_nums = [c["name"] for c in columns_metadata 
                    if c["semantic_type"] == "number" and c["name"] != response["x_axis"]]
    
      if agg_cols:
          response["y_axis"] = agg_cols[0]
      elif other_nums:
          response["y_axis"] = other_nums[0]
      else:
          response["y_axis"] = columns_metadata[1]["name"] if len(columns_metadata) > 1 else None

    return response