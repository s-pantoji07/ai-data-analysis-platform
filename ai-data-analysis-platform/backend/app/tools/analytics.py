from typing import List, Dict, Any
from app.analytics.engine import AnalyticsEngine
from app.analytics.query_models import AnalyticsQuery
from app.analytics.exceptions import AnalyticsExecutionError

# Initialize a single shared engine instance
engine = AnalyticsEngine()

def run_analytics(query: AnalyticsQuery) -> Dict[str, Any]:
    """
    Executes an analytics query or returns dataset profiling if query is empty.
    
    Args:
        query (AnalyticsQuery): The query model containing select, filters, group_by, aggregations, and limit.
    
    Returns:
        Dict[str, Any]: Result as list of dicts (for standard query) 
                        or profiling summary (if query is empty).
    
    Raises:
        AnalyticsExecutionError: If execution fails.
    """
    try:
        result = engine.execute(query)
        return result
    except AnalyticsExecutionError as e:
        # Optionally, log error here
        raise e
    except Exception as e:
        raise AnalyticsExecutionError(f"Unexpected error during analytics execution: {str(e)}")
