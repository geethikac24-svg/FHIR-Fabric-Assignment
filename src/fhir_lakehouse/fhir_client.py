"""FHIR REST client with pagination and incremental _lastUpdated filtering."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator
from urllib.parse import urlencode, urljoin

import requests

logger = logging.getLogger(__name__)


@dataclass
class FetchResult:
    """One page (or full multi-page fetch) of FHIR Bundle data."""

    resource_type: str
    bundles: list[dict[str, Any]] = field(default_factory=list)
    entries: list[dict[str, Any]] = field(default_factory=list)
    api_urls: list[str] = field(default_factory=list)
    api_call_timestamp: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    page_count: int = 0
    entry_count: int = 0


class FhirClient:
    def __init__(
        self,
        base_url: str,
        page_size: int = 50,
        timeout: int = 120,
        max_retries: int = 3,
        retry_backoff: float = 2.0,
        accept: str = "application/fhir+json",
    ) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.page_size = page_size
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.accept = accept
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": accept,
                "User-Agent": "fhir-lakehouse/1.0",
            }
        )

    def lookback_start(self, lookback_days: int) -> str:
        start = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        return start.strftime("%Y-%m-%d")

    def build_search_url(self, resource_path: str, params: dict[str, Any]) -> str:
        query = urlencode(params, doseq=True)
        return f"{urljoin(self.base_url, resource_path)}?{query}"

    def _get(self, url: str) -> dict[str, Any] | str:
        last_exc: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                resp.raise_for_status()
                ctype = (resp.headers.get("Content-Type") or "").lower()
                if "xml" in ctype or "xml" in self.accept:
                    return resp.text
                return resp.json()
            except Exception as exc:  # noqa: BLE001 — retry wrapper
                last_exc = exc
                logger.warning("Attempt %s failed for %s: %s", attempt, url[:120], exc)
                if attempt < self.max_retries:
                    time.sleep(self.retry_backoff * attempt)
        raise RuntimeError(f"Failed GET after {self.max_retries} retries: {url}") from last_exc

    def _next_link(self, bundle: dict[str, Any]) -> str | None:
        for link in bundle.get("link") or []:
            if link.get("relation") == "next":
                return link.get("url")
        return None

    def iter_pages(
        self,
        resource_path: str,
        *,
        since_date: str | None = None,
        extra_params: dict[str, Any] | None = None,
        max_pages: int | None = None,
    ) -> Iterator[tuple[str, dict[str, Any] | str]]:
        """Yield (request_url, bundle_or_xml) following pagination links."""
        params: dict[str, Any] = {"_count": self.page_size}
        if since_date:
            params["_lastUpdated"] = f"ge{since_date}"
        if extra_params:
            params.update(extra_params)

        url: str | None = self.build_search_url(resource_path, params)
        page = 0
        while url:
            page += 1
            if max_pages is not None and page > max_pages:
                break
            logger.info("Fetching page %s: %s", page, url[:160])
            payload = self._get(url)
            yield url, payload
            if isinstance(payload, str):
                # XML mode: stop after first page unless caller handles XML next links
                break
            url = self._next_link(payload)

    def fetch_incremental(
        self,
        resource_name: str,
        resource_path: str,
        lookback_days: int,
        max_pages: int | None = None,
    ) -> FetchResult:
        since = self.lookback_start(lookback_days)
        call_ts = datetime.now(timezone.utc).isoformat()
        params = {"_count": self.page_size, "_lastUpdated": f"ge{since}"}
        result = FetchResult(
            resource_type=resource_name,
            api_call_timestamp=call_ts,
            params=params,
        )

        for url, payload in self.iter_pages(
            resource_path, since_date=since, max_pages=max_pages
        ):
            result.page_count += 1
            result.api_urls.append(url)
            if isinstance(payload, str):
                result.bundles.append({"_xml": payload, "resourceType": "Bundle"})
                continue
            result.bundles.append(payload)
            for entry in payload.get("entry") or []:
                result.entries.append(entry)
            result.entry_count = len(result.entries)

        logger.info(
            "Fetched %s: pages=%s entries=%s since=%s",
            resource_name,
            result.page_count,
            result.entry_count,
            since,
        )
        return result
