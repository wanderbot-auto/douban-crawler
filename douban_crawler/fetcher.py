"""页面访问层：支持 HTTP 直连与 Chrome DevTools MCP。"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shlex
import threading
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol

import httpx
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from douban_crawler.anti_crawl import create_client, generate_bid, random_user_agent, retry_on_failure
from douban_crawler.config import (
    DOUBAN_MCP_ARGS,
    DOUBAN_MCP_COMMAND,
    DOUBAN_MCP_NAV_TIMEOUT,
    DOUBAN_MCP_READY_TIMEOUT,
    DOUBAN_MCP_STABILIZE_DELAY,
    DOUBAN_MCP_STARTUP_TIMEOUT,
)

logger = logging.getLogger(__name__)

_HTML_SNAPSHOT_SCRIPT = """({
  readyState: document.readyState,
  title: document.title,
  url: location.href,
  html: document.documentElement ? document.documentElement.outerHTML : '',
  htmlLength: document.documentElement ? document.documentElement.outerHTML.length : 0,
  bodyText: document.body ? document.body.innerText.slice(0, 400) : ''
})"""


class PageFetcher(Protocol):
    def fetch(self, url: str, referer: str | None = None) -> str | None:
        ...

    def rotate_identity(self) -> None:
        ...

    def close(self) -> None:
        ...


class HttpPageFetcher:
    """沿用原有 HTTP 抓取逻辑，作为回退方案。"""

    def __init__(self) -> None:
        self._client = create_client()

    def fetch(self, url: str, referer: str | None = None) -> str | None:
        if referer:
            self._client.headers["Referer"] = referer

        response = self._do_request(url)
        if response and response.status_code == 200:
            return response.text
        return None

    @retry_on_failure()
    def _do_request(self, url: str) -> httpx.Response:
        logger.debug("HTTP GET %s", url)
        return self._client.get(url)

    def rotate_identity(self) -> None:
        self._client.headers["User-Agent"] = random_user_agent()
        self._client.cookies.set("bid", generate_bid())
        logger.debug("HTTP 抓取身份已轮换")

    def close(self) -> None:
        self._client.close()


@dataclass(slots=True)
class ChromeMcpSettings:
    command: str = DOUBAN_MCP_COMMAND
    args: tuple[str, ...] = tuple(shlex.split(DOUBAN_MCP_ARGS))
    startup_timeout: float = DOUBAN_MCP_STARTUP_TIMEOUT
    navigate_timeout: float = DOUBAN_MCP_NAV_TIMEOUT
    ready_timeout: float = DOUBAN_MCP_READY_TIMEOUT
    stabilize_delay: float = DOUBAN_MCP_STABILIZE_DELAY


class ChromeMcpPageFetcher:
    """通过 Chrome DevTools MCP 控制 Chrome 获取页面。"""

    def __init__(self, settings: ChromeMcpSettings | None = None) -> None:
        self._settings = settings or ChromeMcpSettings()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._session: ClientSession | None = None
        self._shutdown_event: asyncio.Event | None = None
        self._ready = threading.Event()
        self._startup_error: Exception | None = None
        self._started = False
        self._last_url: str | None = None
        self._referer_warmed: set[str] = set()
        self._errlog = open(os.devnull, "w", encoding="utf-8")

    def fetch(self, url: str, referer: str | None = None) -> str | None:
        self._ensure_started()
        future = asyncio.run_coroutine_threadsafe(
            self._fetch_html(url, referer=referer),
            self._require_loop(),
        )
        timeout = (
            self._settings.navigate_timeout
            + self._settings.ready_timeout
            + self._settings.stabilize_delay
            + 10
        )
        return future.result(timeout=timeout)

    def rotate_identity(self) -> None:
        # Chrome 会话保持同一个真实浏览上下文，不做 UA/bid 轮换。
        logger.debug("MCP Chrome 模式跳过身份轮换")

    def close(self) -> None:
        if not self._started:
            return

        loop = self._loop
        shutdown_event = self._shutdown_event
        if loop and shutdown_event:
            loop.call_soon_threadsafe(shutdown_event.set)

        if self._thread:
            self._thread.join(timeout=10)

        self._session = None
        self._loop = None
        self._shutdown_event = None
        self._errlog.close()
        self._started = False

    def _ensure_started(self) -> None:
        if self._started:
            return

        self._thread = threading.Thread(target=self._run_loop, name="chrome-mcp-fetcher", daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=self._settings.startup_timeout):
            raise RuntimeError("Chrome MCP 启动超时，请检查 Chrome / npx / 网络是否可用")

        if self._startup_error:
            raise RuntimeError(f"Chrome MCP 启动失败: {self._startup_error}") from self._startup_error
        if not self._session:
            raise RuntimeError("Chrome MCP 启动超时，请检查 Chrome / npx / 网络是否可用")
        self._started = True

    def _run_loop(self) -> None:
        try:
            asyncio.run(self._session_main())
        except Exception as exc:  # pragma: no cover - 跨线程兜底
            self._startup_error = exc
            self._ready.set()

    async def _session_main(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._shutdown_event = asyncio.Event()

        params = StdioServerParameters(
            command=self._settings.command,
            args=list(self._settings.args),
            env=None,
        )
        logger.info("启动 Chrome MCP: %s %s", self._settings.command, " ".join(self._settings.args))

        async with stdio_client(params, errlog=self._errlog) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = await session.list_tools()
                tool_names = {tool.name for tool in tools.tools}
                required = {"navigate", "evaluate"}
                missing = required - tool_names
                if missing:
                    raise RuntimeError(f"Chrome MCP 缺少工具: {', '.join(sorted(missing))}")

                self._session = session
                self._ready.set()
                await self._shutdown_event.wait()

    async def _fetch_html(self, url: str, referer: str | None = None) -> str | None:
        if referer and referer != url and referer not in self._referer_warmed and not self._last_url:
            logger.debug("MCP 预热 referer: %s", referer)
            await self._navigate(referer)
            await asyncio.sleep(0.5)
            self._referer_warmed.add(referer)

        await self._navigate(url)
        snapshot = await self._wait_until_ready()
        self._last_url = snapshot.get("url") or url
        return snapshot.get("html") or None

    async def _navigate(self, url: str) -> None:
        result = await self._call_tool(
            "navigate",
            {"url": url},
            timeout=self._settings.navigate_timeout,
        )
        logger.debug("MCP 导航成功: %s | %s", url, result[:120])

    async def _wait_until_ready(self) -> dict[str, object]:
        deadline = asyncio.get_running_loop().time() + self._settings.ready_timeout
        last_length = -1
        stable_hits = 0
        snapshot: dict[str, object] = {}

        while asyncio.get_running_loop().time() < deadline:
            snapshot = await self._evaluate_snapshot()
            ready_state = snapshot.get("readyState")
            html_length = int(snapshot.get("htmlLength") or 0)
            if ready_state == "complete" and html_length > 0:
                stable_hits = stable_hits + 1 if html_length == last_length else 0
                if stable_hits >= 1:
                    await asyncio.sleep(self._settings.stabilize_delay)
                    return snapshot
            last_length = html_length
            await asyncio.sleep(0.5)

        logger.warning("页面等待超时，返回当前快照: %s", snapshot.get("url"))
        return snapshot

    async def _evaluate_snapshot(self) -> dict[str, object]:
        payload = await self._call_tool(
            "evaluate",
            {"script": _HTML_SNAPSHOT_SCRIPT},
            timeout=self._settings.ready_timeout,
        )
        try:
            return json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"MCP evaluate 返回了不可解析的数据: {payload[:200]}") from exc

    async def _call_tool(self, name: str, arguments: dict[str, object], timeout: float) -> str:
        session = self._require_session()
        result = await session.call_tool(
            name,
            arguments,
            read_timeout_seconds=timedelta(seconds=timeout),
        )
        text = _result_to_text(result)
        if result.isError:
            raise RuntimeError(text or f"MCP 工具 {name} 调用失败")
        return text

    def _require_loop(self) -> asyncio.AbstractEventLoop:
        if not self._loop:
            raise RuntimeError("Chrome MCP 事件循环未就绪")
        return self._loop

    def _require_session(self) -> ClientSession:
        if not self._session:
            raise RuntimeError("Chrome MCP 会话未建立")
        return self._session


def _result_to_text(result: object) -> str:
    items = getattr(result, "content", []) or []
    texts = [item.text for item in items if getattr(item, "type", None) == "text"]
    return "\n".join(texts).strip()


def create_page_fetcher(backend: str) -> PageFetcher:
    if backend == "mcp":
        return ChromeMcpPageFetcher()
    if backend == "http":
        return HttpPageFetcher()
    raise ValueError(f"不支持的抓取后端: {backend}")
