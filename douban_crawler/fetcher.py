"""HTTP page fetching."""

from __future__ import annotations

import logging
from typing import Callable, Protocol

import httpx

from douban_crawler.anti_crawl import create_client, generate_bid, random_user_agent, retry_on_failure

logger = logging.getLogger(__name__)


class PageFetcher(Protocol):
    def fetch(self, url: str, referer: str | None = None) -> str | None:
        ...

    def rotate_identity(self) -> None:
        ...

    def close(self) -> None:
        ...


RequestExecutor = Callable[[httpx.Client, str, str | None], httpx.Response | None]


class HttpPageFetcher:
    """HTTP fetcher for direct requests."""

    def __init__(self, request_executor: RequestExecutor | None = None) -> None:
        self._client = create_client()
        self._request_executor = request_executor or self._execute_request

    def fetch(self, url: str, referer: str | None = None) -> str | None:
        response = self._request_executor(self._client, url, referer)
        if response and response.status_code == 200:
            return response.text
        return None

    @retry_on_failure()
    def _do_request(self, client: httpx.Client, url: str, referer: str | None = None) -> httpx.Response:
        logger.debug("HTTP GET %s", url)
        headers = {"Referer": referer} if referer else None
        return client.get(url, headers=headers)

    def _execute_request(
        self,
        client: httpx.Client,
        url: str,
        referer: str | None,
    ) -> httpx.Response | None:
        return self._do_request(client, url, referer=referer)

    def rotate_identity(self) -> None:
        self._client.headers["User-Agent"] = random_user_agent()
        self._client.cookies.set("bid", generate_bid())
        logger.debug("HTTP fetch identity rotated")

    def close(self) -> None:
        self._client.close()


def create_page_fetcher(backend: str = "http") -> PageFetcher:
    if backend != "http":
        raise ValueError(f"Unsupported fetch backend: {backend}")
    return HttpPageFetcher()
