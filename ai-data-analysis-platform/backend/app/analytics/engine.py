from typing import List, Dict, Any
import duckdb
import os
import pandas as pd
from app.analytics.query_models import AnalyticsQuery
from app.analytics.exceptions import AnalyticsExecutionError
from app.db.session import SessionLocal
from app.db.models.dataset import Dataset

class AnalyticsEngine:
    """
    Core analytics execution engine.
    Supports:
    - Standard aggregations, filters, grouping
    - Profiling queries if query is empty (returns column summary)
    """

    def __init__(self):
        self.con = duckdb.connect(database=":memory:")

    # -----------------------------
    # Dataset Loading
    # -----------------------------
    def _load_dataset(self, dataset_id: str) -> str:
        db = SessionLocal()
        try:
            dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
            if not dataset:
                raise AnalyticsExecutionError("Dataset not found")
            if not os.path.exists(dataset.file_path):
                raise AnalyticsExecutionError("Dataset file missing on disk")

            table_name = f"dataset_{dataset_id.replace('-', '_')}"
            try:
                self.con.execute(
                    f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM read_csv_auto('{dataset.file_path}')
                    """
                )
            except Exception as e:
                raise AnalyticsExecutionError(f"Failed to load dataset: {str(e)}")

            return table_name
        finally:
            db.close()

    # -----------------------------
    # Profiling
    # -----------------------------
    def _profile_dataset(self, table_name: str) -> Dict[str, Any]:
        """
        Returns column-level profiling including:
        - dtype
        - semantic type (numeric/categorical/date)
        - missing values
        - lists of numeric, categorical, and date columns
        """
        df = self.con.execute(f"SELECT * FROM {table_name}").df()

        profile = {
            "columns": [],
            "missing_values": {},
            "numeric_columns": [],
            "categorical_columns": [],
            "date_columns": []
        }

        for col in df.columns:
            dtype = str(df[col].dtype)
            missing = int(df[col].isna().sum())

            # Determine semantic type
            if pd.api.types.is_numeric_dtype(df[col]):
                sem_type = "numeric"
                profile["numeric_columns"].append(col)
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                sem_type = "date"
                profile["date_columns"].append(col)
            else:
                sem_type = "categorical"
                profile["categorical_columns"].append(col)

            profile["columns"].append({
                "name": col,
                "dtype": dtype,
                "type": sem_type
            })
            profile["missing_values"][col] = missing

        return profile

    # -----------------------------
    # SQL Construction
    # -----------------------------
    def _build_sql(self, table: str, query: AnalyticsQuery) -> str:
        quoted_table = f'"{table}"'
        
        # --- FALLBACK SELECT LOGIC ---
        # If select is empty/null but we have group_by or aggregations, 
        # we need to build a dynamic select list.
        if query.aggregations or query.group_by:
            select_parts = []
            
            # 1. Always select the group_by columns so we can see the labels
            if query.group_by:
                select_parts.extend([f'"{col}"' for col in query.group_by])
            
            # 2. Add the aggregated measures
            if query.aggregations:
                for agg in query.aggregations:
                    func = agg.function.upper()
                    col = agg.column
                    # Create a clean alias: e.g., SUM_Global_Sales
                    alias = f'"{func}_{col.replace(" ", "_")}"'
                    select_parts.append(f'{func}("{col}") AS {alias}')
            
            select_clause = ", ".join(select_parts)
            
        elif query.select:
            # Standard selection if user explicitly asked for columns
            select_clause = ", ".join([f'"{c}"' for c in query.select])
        else:
            # Total fallback: Select everything
            select_clause = "*"

        # Construct the Base Query
        sql = f"SELECT {select_clause} FROM {quoted_table}"

        # Apply Filters
        if query.filters:
            conditions = []
            for f in query.filters:
                val = f"'{f.value}'" if isinstance(f.value, str) else f.value
                conditions.append(f'"{f.column}" {f.operator} {val}')
            sql += " WHERE " + " AND ".join(conditions)

        # Apply Grouping
        if query.group_by:
             sql += " GROUP BY " + ", ".join([f'"{c}"' for c in query.group_by])

        # Apply Sorting
        if query.order_by:
            # Handle aggregated order_by strings like 'SUM(Global_Sales)' 
            # or raw column names
            if "(" in query.order_by:
                sql += f" ORDER BY {query.order_by} {query.order_direction.upper()}"
            else:
                sql += f' ORDER BY "{query.order_by}" {query.order_direction.upper()}'

        # Apply Limit
        if query.limit:
            sql += f" LIMIT {query.limit}"

        return sql

    # -----------------------------
    # Public Execution API
    # -----------------------------
    # app/analytics/engine.py

    def execute(self, query: Any) -> Dict[str, Any]:
        try:
            if isinstance(query, dict):
                query = AnalyticsQuery(**query)
        
                table_name = self._load_dataset(query.dataset_id)

        # UPDATED: A query is ONLY a profile request if EVERYTHING is empty
            is_profile_request = all([
            not query.select,
            not (query.aggregations and len(query.aggregations) > 0),
            not (query.filters and len(query.filters) > 0),
            not (query.group_by and len(query.group_by) > 0)
        ])

            if is_profile_request:
                profile = self._profile_dataset(table_name)
                return {
                "dataset_id": query.dataset_id, 
                "profiling_summary": profile,
                "type": "profiling_result"
            }

        # If we have group_by or aggregations, we proceed to SQL execution
            sql = self._build_sql(table_name, query)
            result_df = self.con.execute(sql).df()
        
            return {
            "dataset_id": query.dataset_id,
            "data": result_df.to_dict(orient="records"),
            "sql_generated": sql,
            "type": "query_result"
        }

        except AnalyticsExecutionError:
        # Re-raise known custom errors
            raise
        except Exception as e:
        # Wrap unknown errors in our domain exception
            raise AnalyticsExecutionError(f"Analytics execution failed: {str(e)}")
