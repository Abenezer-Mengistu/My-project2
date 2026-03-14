"""
BigQuery service singleton.
Replaces bigquery.service.ts.
"""
from __future__ import annotations

import os
from google.cloud import bigquery
from utils.logger import logger


class BigQueryService:
    _instance: bigquery.Client | None = None
    _is_guest_mode: bool = os.environ.get("DEVELOPER_NAME") == "guest"

    @classmethod
    def get_instance(cls) -> bigquery.Client:
        if cls._is_guest_mode:
            raise RuntimeError("BigQuery is disabled in guest mode")

        if cls._instance is None:
            project_id = os.environ.get("GC_PROJECT_ID")
            if not project_id:
                raise RuntimeError("GC_PROJECT_ID environment variable is required for BigQuery")

            logger.info(f"Initializing BigQuery with project ID: {project_id}")
            kwargs: dict = {"project": project_id}
            key_file = os.environ.get("GC_KEY") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if key_file:
                kwargs["credentials"] = _load_credentials(key_file)

            cls._instance = bigquery.Client(**kwargs)
        return cls._instance

    @classmethod
    def is_disabled(cls) -> bool:
        return cls._is_guest_mode


def _load_credentials(key_file: str):
    from google.oauth2 import service_account
    return service_account.Credentials.from_service_account_file(key_file)
