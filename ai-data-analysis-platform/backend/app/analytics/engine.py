from typing import List, Dict, Any
import duckdb
import os

from app.analytics.query_models import AnalyticsQuery
from app.analytics.exceptions import AnalyticsExecutionError
from app.db.session import SessionLocal
from app.db.models.dataset import Dataset


class AnalyticsEngine:
    """
    Core analytics execution engine.
    Responsible ONLY for executing analytical queries on datasets.
    No AI logic, no API logic.
    """

    def __init__(self):
        # In-memory DuckDB for fast analytics
        self.con = duckdb.connect(database=":memory:")

    # -----------------------------
    # Dataset Loading
    # -----------------------------
    def _load_dataset(self, dataset_id: str) -> str:
        """
        Loads dataset into DuckDB and returns table name.
        """
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
    # SQL Construction
    # -----------------------------
    def _build_sql(self, table: str, query: AnalyticsQuery) -> str:
        """
        Converts AnalyticsQuery into SQL.
        """

        # SELECT clause logic updated to include group_by columns
        if query.aggregations:
            select_parts = []
            
            # If we are grouping, we MUST include those columns in SELECT to see labels
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

        # WHERE clause
        if query.filters:
            conditions = []
            for f in query.filters:
                # Basic SQL injection safety for values (simple wrapping)
                value = f"'{f.value}'" if isinstance(f.value, str) else f.value
                conditions.append(f"{f.column} {f.operator} {value}")
            sql += " WHERE " + " AND ".join(conditions)

        # GROUP BY
        if query.group_by:
            sql += " GROUP BY " + ", ".join(query.group_by)

        # LIMIT
        if query.limit:
            sql += f" LIMIT {query.limit}"

        return sql

    # -----------------------------
    # Public Execution API
    # -----------------------------
    def execute(self, query: AnalyticsQuery) -> List[Dict[str, Any]]:
        """
        Executes an analytics query and returns structured results.
        """
        try:
            table = self._load_dataset(query.dataset_id)
            sql = self._build_sql(table, query)
            
            # Execute and return as list of dicts
            result_df = self.con.execute(sql).df()
            return result_df.to_dict(orient="records")

        except AnalyticsExecutionError:
            raise
        except Exception as e:
            raise AnalyticsExecutionError(f"Analytics execution failed: {str(e)}")