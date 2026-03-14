"""
HTTP (Got) base class — replaces got.base.ts.
Uses httpx.AsyncClient for all HTTP scraping operations.
"""
from __future__ import annotations

import httpx
from config import CONFIG
from utils.logger import logger


SCRAPER_TYPE_HTTP = "got"
SCRAPER_TYPE_PLAYWRIGHT = "playwright"


class HttpBase:
    """Abstract base scraper using httpx (replaces GotBase with got)."""

    type: str = SCRAPER_TYPE_HTTP
    handler: str = "base-http"
    proxy: str | None = None

    def __init__(self):
        self._default_user_agent = CONFIG["app"]["default_user_agent"]
        self._default_sec_ch_ua = CONFIG["app"]["default_sec_ch_ua"]
        self._cookies: dict = {}
        self._proxy_url: str | None = None
        self._client: httpx.AsyncClient | None = None

    @classmethod
    async def init(cls, *args, **kwargs) -> "HttpBase":
        instance = cls()
        await instance._setup(*args, **kwargs)
        return instance

    async def _setup(self, *args, **kwargs) -> None:
        if kwargs.get("dry_run"):
            logger.info("Dry run: skipping HTTP client initialization.")
            return

        proxy_key = self.__class__.proxy
        self._proxy_url = CONFIG["proxies"][proxy_key]["url"] if proxy_key else None
        self._client = self._build_client()

    def _build_client(self) -> httpx.AsyncClient:
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
            "accept-encoding": "gzip, deflate, br, zstd",
            "cache-control": "max-age=0",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": self._default_user_agent,
            "sec-ch-ua": self._default_sec_ch_ua,
        }
        proxy_arg = self._proxy_url  # httpx accepts proxy URL directly

        transport = httpx.AsyncHTTPTransport(
            verify=False,
            retries=3,
            proxy=proxy_arg,
        )

        return httpx.AsyncClient(
            headers=headers,
            cookies=self._cookies,
            transport=transport,
            follow_redirects=True,
            timeout=60,
        )

    def set_proxy(self, proxy_key: str | None) -> None:
        self._proxy_url = CONFIG["proxies"][proxy_key]["url"] if proxy_key else None
        self._client = self._build_client()

    def set_user_agent(self, user_agent: str) -> None:
        self._default_user_agent = user_agent
        self._client = self._build_client()

    def set_cookies(self, cookies: dict) -> None:
        self._cookies = cookies
        self._client = self._build_client()

    async def scrape(self, *args, **kwargs):
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()
