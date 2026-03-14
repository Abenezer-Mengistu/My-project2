"""
BigQuery task_events table schema.
Replaces TaskEvents.schema.ts.
"""
from __future__ import annotations

from typing import Literal

from google.cloud import bigquery

from config import CONFIG
from database.bigquery.bigquery_table import BigQueryTable


class TaskEvent(dict):
    """Typed dict-like structure for a task event row (for documentation purposes)."""
    task_hash_id: str
    queue_name: str
    name: str
    domain: str
    handler: str
    operation: str
    type: str
    task_payload: str
    status: Literal["queued", "started", "completed", "failed"]
    processed_count: int | None
    http_status_code: int | None
    number_of_attempts: int
    error_message: str | None
    error_stack: str | None
    error_type: str | None
    proxy: str | None
    proxy_url: str | None
    row_inserted_at: str


class TaskEventsTable(BigQueryTable[TaskEvent]):
    def _get_table_name(self) -> str:
        return f"task_events_{CONFIG['app']['node_env']}"

    def _get_schema(self) -> list[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("task_hash_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("queue_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("domain", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("handler", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("operation", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("task_payload", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("processed_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("http_status_code", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("number_of_attempts", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("error_stack", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("error_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("proxy", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("proxy_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("row_inserted_at", "TIMESTAMP", mode="REQUIRED"),
        ]

    def _get_table_options(self) -> dict:
        return {
            "timePartitioning": {"type": "DAY", "field": "row_inserted_at"},
            "clustering": {"fields": ["queue_name", "status", "task_hash_id"]},
        }
