import sys
import os
from unittest.mock import patch, MagicMock

# Ensure the backend directory is in the python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.intent.models import UserIntent, IntentAggregation
from app.intent.intent_mapper import map_intent_to_query
from app.analytics.exceptions import AnalyticsExecutionError

# --- MOCK VALIDATOR LOGIC ---

def mocked_validate(query):
    """
    Simulates the validation logic of MetadataValidator 
    without touching the broken database code.
    """
    valid_columns = ["Genre", "Global_Sales", "Publisher", "Year"]
    
    # Check dimensions/group_by
    for col in query.group_by:
        if col not in valid_columns:
            raise AnalyticsExecutionError(f"Column '{col}' does not exist")
            
    # Check aggregations
    for agg in query.aggregations:
        if agg.column not in valid_columns:
            raise AnalyticsExecutionError(f"Column '{agg.column}' does not exist")
        # Simulate semantic type check: Genre is categorical, cannot SUM
        if agg.column == "Genre" and agg.function == "sum":
            raise AnalyticsExecutionError(f"Aggregation 'sum' cannot be applied to non-numeric column 'Genre'")
            
    return query

# --- TEST CASES ---

def test_top_genres_by_global_sales():
    dataset_id = "83db4575-07a3-4fd3-9d0d-56b2b433d8ee"
    intent = UserIntent(
        dataset_id=dataset_id,
        intent_type="ranking",
        dimensions=["Genre"],
        measures=[IntentAggregation(column="Global_Sales", function="sum")],
        order_by="Global_Sales",
        limit=5,
        raw_query="Top 5 genres by global sales"
    )

    query = map_intent_to_query(intent)

    assert query.dataset_id.strip() == dataset_id
    assert query.group_by == ["Genre"]
    assert query.aggregations[0].column == "Global_Sales"
    print("✅ test_top_genres_by_global_sales passed")

def test_total_global_sales():
    intent = UserIntent(
        dataset_id="83db4575-07a3-4fd3-9d0d-56b2b433d8ee",
        intent_type="aggregation",
        dimensions=[],
        measures=[IntentAggregation(column="Global_Sales", function="sum")],
        raw_query="Total global sales"
    )
    query = map_intent_to_query(intent)
    assert query.aggregations[0].function == "sum"
    print("✅ test_total_global_sales passed")

def test_invalid_column_should_fail():
    intent = UserIntent(
        dataset_id="83db4575-07a3-4fd3-9d0d-56b2b433d8ee",
        intent_type="ranking",
        dimensions=["InvalidColumn"],
        measures=[IntentAggregation(column="Global_Sales", function="sum")],
        raw_query="Invalid column test"
    )
    try:
        map_intent_to_query(intent)
        print("❌ test_invalid_column_should_fail FAILED")
    except Exception as e:
        print(f"✅ test_invalid_column_should_fail passed: {e}")

def test_invalid_aggregation_on_categorical():
    intent = UserIntent(
        dataset_id="83db4575-07a3-4fd3-9d0d-56b2b433d8ee",
        intent_type="aggregation",
        dimensions=[],
        measures=[IntentAggregation(column="Genre", function="sum")],
        raw_query="Sum of genre (invalid)"
    )
    try:
        map_intent_to_query(intent)
        print("❌ test_invalid_aggregation_on_categorical FAILED")
    except Exception as e:
        print(f"✅ test_invalid_aggregation_on_categorical passed: {e}")

# --- EXECUTION ---

if __name__ == "__main__":
    # We patch the CLASS MetadataValidator where it's imported in intent_mapper
    # This bypasses the __init__ and _load_metadata methods entirely
    with patch('app.intent.intent_mapper.MetadataValidator') as MockClass:
        # Setup the instance returned by MetadataValidator(id)
        mock_instance = MockClass.return_value
        # Setup the .validate() method on that instance
        mock_instance.validate.side_effect = mocked_validate
        
        print("Starting Planner Tests (with MetadataValidator Patch)...\n" + "-"*30)
        test_top_genres_by_global_sales()
        test_total_global_sales()
        test_invalid_column_should_fail()
        test_invalid_aggregation_on_categorical()
        print("-" * 30 + "\nTests Completed.")