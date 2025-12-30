from typing import Dict, Any
import duckdb
import os
import pandas as pd
from app.analytics.query_models import AnalyticsQuery
from app.analytics.exceptions import AnalyticsExecutionError
from app.db.session import SessionLocal
from app.db.models.dataset import Dataset


class AnalyticsEngine:
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

            self.con.execute(
                f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_csv_auto(
                    '{dataset.file_path}',
                    nullstr=['N/A', 'nan', 'null', 'None', ''],
                    sample_size=20000
                )
                """
            )

            return table_name
        finally:
            db.close()

    # -----------------------------
    # SQL Construction
    # -----------------------------
    def _build_sql(self, table: str, query: AnalyticsQuery) -> str:
        quoted_table = f'"{table}"'

        # ---------- SELECT ----------
        if query.aggregations or query.group_by:
            select_parts = []

            if query.group_by:
                select_parts.extend([f'"{c}"' for c in query.group_by])

            if query.aggregations:
                for agg in query.aggregations:
                    func = agg.function.upper()
                    col = agg.column
                    alias = f'"{func}_{col.replace(" ", "_")}"'
                    select_parts.append(f'{func}("{col}") AS {alias}')

            select_clause = ", ".join(select_parts)

        elif query.select:
            select_clause = ", ".join([f'"{c}"' for c in query.select])
        else:
            select_clause = "*"

        sql = f"SELECT {select_clause} FROM {quoted_table}"

        # ---------- WHERE ----------
        if query.filters:
            conditions = []
            for f in query.filters:
                val = f"'{f.value}'" if isinstance(f.value, str) else f.value
                conditions.append(f'"{f.column}" {f.operator} {val}')
            sql += " WHERE " + " AND ".join(conditions)

        # ---------- GROUP BY ----------
        if query.group_by:
            sql += " GROUP BY " + ", ".join([f'"{c}"' for c in query.group_by])

        # ---------- ORDER BY (ALIAS SAFE) ----------
        if query.order_by:
            sql += f' ORDER BY "{query.order_by}" {query.order_direction.upper()}'

        # ---------- LIMIT ----------
        if query.limit:
            sql += f" LIMIT {query.limit}"

        return sql

    # -----------------------------
    # Execution
    # -----------------------------
    def execute(self, query: Any) -> Dict[str, Any]:
        try:
            if isinstance(query, dict):
                query = AnalyticsQuery(**query)

            table_name = self._load_dataset(query.dataset_id)

            is_profile_request = all([
                not query.select,
                not query.aggregations,
                not query.filters,
                not query.group_by
            ])

            if is_profile_request:
                return {
                    "dataset_id": query.dataset_id,
                    "type": "profiling_result"
                }

            sql = self._build_sql(table_name, query)
            df = self.con.execute(sql).df()

            return {
                "dataset_id": query.dataset_id,
                "data": df.to_dict(orient="records"),
                "sql_generated": sql,
                "row_count": len(df),
                "type": "query_result"
            }

        except AnalyticsExecutionError:
            raise
        except Exception as e:
            raise AnalyticsExecutionError(f"Analytics execution failed: {str(e)}")
