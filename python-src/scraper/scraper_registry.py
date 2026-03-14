"""
Scraper registry — maps domain:handler to scraper class.
Replaces scraper.registry.ts.
"""
from __future__ import annotations

from typing import Type


class ScraperRegistry:
    """Type-safe registry mapping domain:handler → scraper class."""

    def __init__(self):
        self._scrapers: dict[str, type] = {}

    def register(self, domain: str, scraper_cls: type) -> None:
        key = f"{domain}:{scraper_cls.handler}"
        self._scrapers[key] = scraper_cls

    def get(self, domain: str, handler: str) -> type | None:
        return self._scrapers.get(f"{domain}:{handler}")

    def list_for_domain(self, domain: str) -> list[str]:
        prefix = f"{domain}:"
        return [
            cls.handler for key, cls in self._scrapers.items()
            if key.startswith(prefix)
        ]

    def get_domains(self) -> list[str]:
        domains: set[str] = set()
        for key in self._scrapers:
            domains.add(key.split(":")[0])
        return list(domains)

    def has_domain(self, domain: str) -> bool:
        return domain in self.get_domains()


# Global singleton
MasterScraperRegistry = ScraperRegistry()
