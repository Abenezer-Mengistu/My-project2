"""
Abstract BigQuery table base class.
Replaces the TypeScript BigQueryTable generic class.
"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from google.cloud import bigquery

from monitoring.bigquery_service import BigQueryService

T = TypeVar("T")


class BigQueryTable(Generic[T]):
    """Abstract base for strongly-typed BigQuery table wrappers."""

    def _get_table_name(self) -> str:
        raise NotImplementedError

    def _get_schema(self) -> list[bigquery.SchemaField]:
        raise NotImplementedError

    def _get_table_options(self) -> dict[str, Any]:
        return {}

    def _get_full_table_id(self) -> str:
        client = BigQueryService.get_instance()
        return f"{client.project}.scraper_events.{self._get_table_name()}"

    async def create_table_if_not_exists(self) -> None:
        """Create BigQuery table if it does not already exist."""
        client = BigQueryService.get_instance()
        table_ref = bigquery.Table(self._get_full_table_id(), schema=self._get_schema())

        opts = self._get_table_options()
        if "timePartitioning" in opts:
            table_ref.time_partitioning = bigquery.TimePartitioning(
                type_=opts["timePartitioning"].get("type", "DAY"),
                field=opts["timePartitioning"].get("field"),
            )
        if "clustering" in opts:
            table_ref.clustering_fields = opts["clustering"].get("fields", [])

        try:
            client.create_table(table_ref, exists_ok=True)
        except Exception as exc:
            raise RuntimeError(f"Failed to create BigQuery table: {exc}") from exc

    def insert_row(self, row: dict) -> None:
        """Insert a single row into the BigQuery table (streaming insert)."""
        client = BigQueryService.get_instance()
        errors = client.insert_rows_json(self._get_full_table_id(), [row])
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")
