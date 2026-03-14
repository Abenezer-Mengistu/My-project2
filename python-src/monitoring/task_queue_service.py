"""
Task Queue Service — wraps Google Cloud Tasks.
Replaces task-queue.service.ts.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone

from google.cloud import tasks_v2
from google.protobuf import duration_pb2

from config import CONFIG, __NODE_ENV_DEV
from database.bigquery.schemas.task_events_schema import TaskEventsTable
from utils.logger import logger


class TaskRunData(dict):
    """Typed dict for task tracking data."""


class TaskQueueService:
    _client: tasks_v2.CloudTasksClient | None = None

    def __init__(self):
        self.project_id: str = os.environ.get("GC_PROJECT_ID", "")
        self.location: str = os.environ.get("GC_TASKS_LOCATION", "us-central1")
        self._task_events_table = TaskEventsTable()
        self._skip_bigquery: bool = (
            os.environ.get("DEVELOPER_NAME") == "guest"
            or os.environ.get("NODE_ENV") == "staging"
        )

        if not self.project_id:
            raise RuntimeError("GC_PROJECT_ID environment variable is required")

        if TaskQueueService._client is None:
            client_options = {}
            task_url = os.environ.get("GC_TASK_URL")
            task_port = os.environ.get("GC_TASK_PORT")
            if task_url and task_port:
                client_options["api_endpoint"] = f"{task_url}:{task_port}"
                logger.info(f"Cloud Tasks client using emulator at {task_url}:{task_port}")

            key_file = os.environ.get("GC_KEY") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if key_file:
                TaskQueueService._client = tasks_v2.CloudTasksClient.from_service_account_file(
                    key_file, client_options=client_options or None
                )
            else:
                TaskQueueService._client = tasks_v2.CloudTasksClient(
                    client_options=client_options or None
                )

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _generate_task_hash_id(self, target_url: str, payload: dict) -> str:
        hash_input = target_url + json.dumps(payload, sort_keys=True)
        return hashlib.md5(hash_input.encode()).hexdigest()

    def _normalize_queue_name(self, name: str) -> str:
        name = name.lower()
        name = re.sub(r"[^a-z0-9-]", "-", name)
        name = re.sub(r"^-+|-+$", "", name)
        return name[:100]

    def _queue_path(self, queue_name: str) -> str:
        return TaskQueueService._client.queue_path(self.project_id, self.location, queue_name)  # type: ignore[union-attr]

    async def _ensure_queue(self, queue_name: str) -> None:
        client = TaskQueueService._client
        queue_path = self._queue_path(queue_name)
        try:
            client.get_queue(request={"name": queue_path})  # type: ignore[union-attr]
        except Exception as err:
            status_code = getattr(err, "code", None) or getattr(getattr(err, "grpc_status_code", None), "value", None)
            if status_code == 5:  # NOT_FOUND
                parent = client.location_path(self.project_id, self.location)  # type: ignore[union-attr]
                try:
                    client.create_queue(  # type: ignore[union-attr]
                        request={
                            "parent": parent,
                            "queue": {
                                "name": queue_path,
                                "rate_limits": {
                                    "max_dispatches_per_second": 3,
                                    "max_concurrent_dispatches": 3,
                                },
                                "retry_config": {
                                    "max_attempts": -1,
                                    "min_backoff": duration_pb2.Duration(seconds=360),
                                    "max_backoff": duration_pb2.Duration(seconds=3600),
                                    "max_doublings": 16,
                                },
                            },
                        }
                    )
                    logger.info(f"Created queue {queue_name}")
                except Exception as create_err:
                    ce_code = getattr(create_err, "code", None)
                    if ce_code != 6:  # ALREADY_EXISTS
                        raise
            else:
                raise

    # ── Public API ────────────────────────────────────────────────────────────

    async def create(self, options: dict) -> str:
        """Create a Cloud Tasks task and return its name."""
        queue_name = self._normalize_queue_name(options["queueName"])
        task_hash_id = self._generate_task_hash_id(options["targetUrl"], options["payload"])
        await self._ensure_queue(queue_name)

        parent = self._queue_path(queue_name)
        task_name = f"{parent}/tasks/{task_hash_id}"

        auth_token = CONFIG["app"]["auth_token"] or ""
        task_body = {
            "name": task_name,
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": options["targetUrl"],
                "headers": {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {auth_token}",
                    "Queue-Task-Hash-Id": task_hash_id,
                    "Queue-Task-Name": queue_name,
                },
                "body": json.dumps(options["payload"]).encode("utf-8"),
            },
        }

        try:
            response = TaskQueueService._client.create_task(  # type: ignore[union-attr]
                request={"parent": parent, "task": task_body}
            )
            logger.debug(f"Created task {response.name} in queue {queue_name}")
            await self.mark_task_queued(options)
            return response.name
        except Exception as err:
            err_code = getattr(err, "code", None)
            if err_code == 6:  # ALREADY_EXISTS
                logger.warning(f"Skipping: {task_name} already exists")
                return task_name
            raise

    async def mark_task_queued(self, options: dict, scraper=None) -> None:
        if self._skip_bigquery:
            return
        await self._insert_event_row(options, "queued", scraper)

    async def mark_task_started(self, options: dict, scraper=None) -> None:
        if self._skip_bigquery:
            return
        await self._insert_event_row(options, "started", scraper)

    async def mark_task_completed(self, options: dict, scraper=None, enriched_result=None) -> None:
        if self._skip_bigquery:
            return
        await self._insert_event_row(
            options, "completed", scraper, http_status_code=200,
            processed_count=enriched_result.get("processed_count") if enriched_result else None
        )

    async def mark_task_failed(self, options: dict, scraper=None, error: Exception | None = None) -> None:
        if self._skip_bigquery:
            return
        status_code = None
        if error:
            resp = getattr(error, "response", None)
            status_code = getattr(resp, "status_code", None)

        await self._insert_event_row(
            options, "failed", scraper,
            http_status_code=status_code,
            error_message=str(error) if error else None,
            error_stack=None,
            error_type=type(error).__name__ if error else None,
        )

    async def _insert_event_row(
        self,
        options: dict,
        status: str,
        scraper=None,
        http_status_code: int | None = None,
        processed_count: int | None = None,
        error_message: str | None = None,
        error_stack: str | None = None,
        error_type: str | None = None,
    ) -> None:
        task_hash_id = self._generate_task_hash_id(
            options.get("targetUrl", ""), options.get("payload", {})
        )
        now = datetime.now(timezone.utc).isoformat()

        proxy_key = getattr(scraper, "proxy", None) if scraper else None
        proxy_url = CONFIG["proxies"].get(proxy_key, {}).get("url") if proxy_key else None

        row = {
            "task_hash_id": task_hash_id,
            "queue_name": options.get("queueName", ""),
            "name": options.get("name", ""),
            "domain": options.get("domain", ""),
            "handler": options.get("handler", ""),
            "operation": options.get("payload", {}).get("operation", "scrape"),
            "type": options.get("type", ""),
            "task_payload": json.dumps(options.get("payload", {})),
            "status": status,
            "processed_count": processed_count,
            "http_status_code": http_status_code,
            "number_of_attempts": 0,
            "error_message": error_message,
            "error_stack": error_stack,
            "error_type": error_type,
            "proxy": proxy_key,
            "proxy_url": proxy_url,
            "row_inserted_at": now,
        }

        try:
            self._task_events_table.insert_row(row)
        except Exception as exc:
            logger.error(f"Failed to track task in BigQuery: {exc}")
