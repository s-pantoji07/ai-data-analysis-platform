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
        if query.aggregations:
            select_parts = []
            if query.group_by:
                select_parts.extend(query.group_by)
            select_parts.extend(
                f"{agg.function.upper()}({agg.column}) AS {agg.function}_{agg.column}"
                for agg in query.aggregations
            )
            select_clause = ", ".join(select_parts)
        else:
            select_clause = ", ".join(query.select) if query.select else "*"

        sql = f"SELECT {select_clause} FROM {table}"

        if query.filters:
            conditions = []
            for f in query.filters:
                value = f"'{f.value}'" if isinstance(f.value, str) else f.value
                conditions.append(f"{f.column} {f.operator} {value}")
            sql += " WHERE " + " AND ".join(conditions)

        if query.group_by:
            sql += " GROUP BY " + ", ".join(query.group_by)

        if query.limit:
            sql += f" LIMIT {query.limit}"

        return sql

    # -----------------------------
    # Public Execution API
    # -----------------------------
    def execute(self, query: AnalyticsQuery) -> Dict[str, Any]:
        try:
            table_name = self._load_dataset(query.dataset_id)

            # If query is empty, return profiling
            if not query.select and not query.aggregations and not query.filters:
                profile = self._profile_dataset(table_name)
                return {"dataset_id": query.dataset_id, "profiling_summary": profile}

            # Otherwise, run SQL query
            sql = self._build_sql(table_name, query)
            result_df = self.con.execute(sql).df()
            return result_df.to_dict(orient="records")

        except AnalyticsExecutionError:
            raise
        except Exception as e:
            raise AnalyticsExecutionError(f"Analytics execution failed: {str(e)}")
