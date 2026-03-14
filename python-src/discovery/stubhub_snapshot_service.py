"""
StubHub snapshot service:
- fetch paginated JSON payloads from a StubHub endpoint
- store raw snapshots in ticket_data
"""
from __future__ import annotations

import hashlib
import json
from typing import Any
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import httpx

from database.connection import create_tables
from database.repositories.ticketing.ticket_data import get_ticket_data_repository
from utils.logger import logger


class StubHubSnapshotService:
    def __init__(self, source_url: str, page_param: str = "page"):
        self.source_url = source_url
        self.page_param = page_param

    @staticmethod
    def _with_query_param(url: str, key: str, value: int) -> str:
        parts = urlparse(url)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        query[key] = str(value)
        new_query = urlencode(query, doseq=True)
        return urlunparse(parts._replace(query=new_query))

    @staticmethod
    def _normalize_headers(headers: dict[str, str] | None) -> dict[str, str]:
        if not headers:
            return {}
        cleaned: dict[str, str] = {}
        blocked = {
            "host",
            "content-length",
            "connection",
            "accept-encoding",
            "transfer-encoding",
        }
        for k, v in headers.items():
            lk = k.lower().strip()
            if not lk or lk.startswith(":") or lk in blocked:
                continue
            cleaned[k] = v
        return cleaned

    @staticmethod
    def _response_payload(resp: httpx.Response) -> dict[str, Any]:
        content_type = resp.headers.get("content-type", "")
        text = resp.text or ""
        try:
            parsed = resp.json()
            if isinstance(parsed, dict):
                payload = dict(parsed)
            else:
                payload = {"data": parsed}
            payload.setdefault("_meta", {})
            payload["_meta"].update(
                {
                    "status_code": resp.status_code,
                    "content_type": content_type,
                    "is_json": True,
                }
            )
            return payload
        except Exception:
            return {
                "_meta": {
                    "status_code": resp.status_code,
                    "content_type": content_type,
                    "is_json": False,
                },
                "raw_text": text[:20000],
            }

    async def run(
        self,
        start_page: int = 1,
        max_pages: int = 10,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        # Ensure all mapped tables are registered in metadata before create_all.
        import database.models.ticketing  # noqa: F401

        await create_tables()
        repo = get_ticket_data_repository()
        saved_pages = 0
        page_summaries: list[dict[str, Any]] = []
        seen_hashes: set[str] = set()

        request_headers = self._normalize_headers(headers)
        async with httpx.AsyncClient(
            timeout=60,
            follow_redirects=True,
            headers=request_headers if request_headers else None,
            cookies=cookies or None,
        ) as client:
            for page in range(start_page, start_page + max_pages):
                url = self._with_query_param(self.source_url, self.page_param, page)
                resp = await client.get(url)
                payload = self._response_payload(resp)

                payload_hash = hashlib.md5(
                    json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
                ).hexdigest()
                if payload_hash in seen_hashes:
                    logger.info(f"Duplicate payload detected at page {page}; stopping pagination.")
                    break
                seen_hashes.add(payload_hash)

                items = payload.get("Items") if isinstance(payload, dict) else None
                item_count = len(items) if isinstance(items, list) else None

                await repo.add_snapshot(self.source_url, page, payload)
                saved_pages += 1
                page_summaries.append(
                    {
                        "page_number": page,
                        "status_code": resp.status_code,
                        "content_type": resp.headers.get("content-type"),
                        "is_json": payload.get("_meta", {}).get("is_json", True),
                        "items_count": item_count,
                    }
                )

                if isinstance(items, list) and len(items) == 0:
                    logger.info(f"No more items at page {page}; stopping pagination.")
                    break

        return {
            "source_url": self.source_url,
            "saved_pages": saved_pages,
            "pages": page_summaries,
        }

    async def run_single(
        self,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        # Ensure all mapped tables are registered in metadata before create_all.
        import database.models.ticketing  # noqa: F401

        await create_tables()
        repo = get_ticket_data_repository()
        request_headers = self._normalize_headers(headers)
        async with httpx.AsyncClient(
            timeout=60,
            follow_redirects=True,
            headers=request_headers if request_headers else None,
            cookies=cookies or None,
        ) as client:
            resp = await client.get(self.source_url)
            payload = self._response_payload(resp)
            await repo.add_snapshot(self.source_url, 0, payload)
            return {
                "source_url": self.source_url,
                "status_code": resp.status_code,
                "content_type": resp.headers.get("content-type"),
                "is_json": payload.get("_meta", {}).get("is_json", True),
                "saved_pages": 1,
            }
