"""页面访问层：支持 HTTP 直连与 Chrome DevTools MCP。"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shlex
import threading
from dataclasses import dataclass
from datetime import timedelta
from typing import Callable, Protocol

import httpx
try:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
except ImportError:  # 仅在 mcp 后端运行时才需要
    ClientSession = None
    StdioServerParameters = None
    stdio_client = None

from douban_crawler.anti_crawl import create_client, generate_bid, random_user_agent, retry_on_failure
from douban_crawler.config import (
    DOUBAN_BASE_URL,
    DOUBAN_COOKIE,
    DOUBAN_MCP_ARGS,
    DOUBAN_MCP_COMMAND,
    DOUBAN_MCP_NAV_TIMEOUT,
    DOUBAN_MCP_READY_TIMEOUT,
    DOUBAN_MCP_STABILIZE_DELAY,
    DOUBAN_MCP_STARTUP_TIMEOUT,
    PROXY_MAX_FAILURES,
    PROXY_POOL_CONFIG,
    PROXY_POOL_MODE,
)
from douban_crawler.transport.proxy_pool import MockProxyPool, ProxyEndpoint, build_mock_proxy_pool

logger = logging.getLogger(__name__)

_HTML_SNAPSHOT_SCRIPT = """({
  readyState: document.readyState,
  title: document.title,
  url: location.href,
  html: document.documentElement ? document.documentElement.outerHTML : '',
  htmlLength: document.documentElement ? document.documentElement.outerHTML.length : 0,
  bodyText: document.body ? document.body.innerText.slice(0, 400) : ''
})"""

_EVALUATE_SCRIPT_FUNCTION = """() => ({
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


RequestExecutor = Callable[[httpx.Client, str, str | None, ProxyEndpoint | None], httpx.Response | None]


class RouteFailure(RuntimeError):
    """供测试注入的明确路由失败信号。"""

    def __init__(self, message: str, *, definitive: bool = False) -> None:
        super().__init__(message)
        self.definitive = definitive


class HttpPageFetcher:
    """沿用原有 HTTP 抓取逻辑，作为回退方案。"""

    def __init__(
        self,
        proxy_pool: MockProxyPool | None = None,
        request_executor: RequestExecutor | None = None,
    ) -> None:
        self._client = create_client()
        self._proxy_pool = proxy_pool
        self._request_executor = request_executor or self._execute_request

    def fetch(self, url: str, referer: str | None = None) -> str | None:
        if not self._proxy_pool:
            response = self._request_executor(self._client, url, referer, None)
            if response and response.status_code == 200:
                return response.text
            return None

        attempted: set[str] = set()
        last_reason = "未开始请求"
        while True:
            endpoint = self._proxy_pool.acquire(exclude=attempted)
            if endpoint is None:
                logger.error("mock 代理池没有可用节点，放弃抓取: %s | 最后失败: %s", url, last_reason)
                return None

            attempted.add(endpoint.name)
            logger.debug("使用 mock 代理节点 %s 抓取: %s", endpoint.name, url)
            try:
                response = self._request_executor(self._client, url, referer, endpoint)
            except Exception as exc:
                last_reason = str(exc) or exc.__class__.__name__
                disabled = self._proxy_pool.report_failure(
                    endpoint,
                    last_reason,
                    definitive=_is_definitive_route_error(exc),
                )
                suffix = "，已标记失效" if disabled else ""
                logger.warning("mock 代理节点 %s 请求异常: %s%s", endpoint.name, last_reason, suffix)
                continue

            if response and response.status_code == 200:
                self._proxy_pool.report_success(endpoint)
                return response.text

            if response is not None and not _should_failover_status(response.status_code):
                logger.error("HTTP %s，当前请求不触发 failover", response.status_code)
                return None

            last_reason = f"HTTP {response.status_code}" if response is not None else "空响应"
            disabled = self._proxy_pool.report_failure(
                endpoint,
                last_reason,
                definitive=response is not None and response.status_code == 407,
            )
            suffix = "，已标记失效" if disabled else ""
            logger.warning("mock 代理节点 %s 失败: %s%s", endpoint.name, last_reason, suffix)

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
        endpoint: ProxyEndpoint | None,
    ) -> httpx.Response | None:
        if endpoint:
            logger.debug("mock 代理节点 %s 当前仅用于 failover 测试，不修改真实网络出口", endpoint.name)
        return self._do_request(client, url, referer=referer)

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
        self._tool_mode = "slim"
        self._page_ready = False
        self._douban_cookies_synced = False
        self._douban_cookie_items = _parse_cookie_string(DOUBAN_COOKIE)
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
        if ClientSession is None or StdioServerParameters is None or stdio_client is None:
            raise RuntimeError("mcp 后端需要先安装依赖：pip install -e . 或 pip install mcp")

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
                if {"navigate", "evaluate"} <= tool_names:
                    self._tool_mode = "slim"
                elif {"new_page", "navigate_page", "evaluate_script"} <= tool_names:
                    self._tool_mode = "full"
                else:
                    raise RuntimeError(
                        "Chrome MCP 缺少可用工具组合，需要 slim(navigate/evaluate) "
                        "或完整模式(new_page/navigate_page/evaluate_script)"
                    )

                self._session = session
                self._ready.set()
                await self._shutdown_event.wait()

    async def _fetch_html(self, url: str, referer: str | None = None) -> str | None:
        await self._sync_douban_cookies_if_needed(url)

        if referer and referer != url and referer not in self._referer_warmed and not self._last_url:
            logger.debug("MCP 预热 referer: %s", referer)
            await self._navigate(referer)
            await asyncio.sleep(0.5)
            self._referer_warmed.add(referer)

        await self._navigate(url)
        snapshot = await self._wait_until_ready()
        self._last_url = snapshot.get("url") or url
        return snapshot.get("html") or None

    async def _sync_douban_cookies_if_needed(self, url: str) -> None:
        if self._douban_cookies_synced or not self._douban_cookie_items or "douban.com" not in url:
            return

        logger.info("向 MCP 浏览器同步 %s 个 Douban Cookie（仅浏览器可写项）", len(self._douban_cookie_items))
        await self._navigate(DOUBAN_BASE_URL)
        await self._set_browser_cookies(self._douban_cookie_items)
        await asyncio.sleep(0.5)
        self._douban_cookies_synced = True

    async def _set_browser_cookies(self, cookies: dict[str, str]) -> None:
        if self._tool_mode == "slim":
            await self._call_tool(
                "evaluate",
                {"script": _build_cookie_injection_script(cookies)},
                timeout=self._settings.ready_timeout,
            )
            return

        payload = await self._call_tool(
            "evaluate_script",
            {"function": _build_set_cookies_function(cookies)},
            timeout=self._settings.ready_timeout,
        )
        logger.debug("MCP Cookie 同步结果: %s", payload[:120])

    async def _navigate(self, url: str) -> None:
        if self._tool_mode == "slim":
            result = await self._call_tool(
                "navigate",
                {"url": url},
                timeout=self._settings.navigate_timeout,
            )
        elif not self._page_ready:
            result = await self._call_tool(
                "new_page",
                {"url": url, "timeout": int(self._settings.navigate_timeout * 1000)},
                timeout=self._settings.navigate_timeout,
            )
            self._page_ready = True
        else:
            result = await self._call_tool(
                "navigate_page",
                {"type": "url", "url": url, "timeout": int(self._settings.navigate_timeout * 1000)},
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
        if self._tool_mode == "slim":
            payload = await self._call_tool(
                "evaluate",
                {"script": _HTML_SNAPSHOT_SCRIPT},
                timeout=self._settings.ready_timeout,
            )
        else:
            payload = await self._call_tool(
                "evaluate_script",
                {"function": _EVALUATE_SCRIPT_FUNCTION},
                timeout=self._settings.ready_timeout,
            )
        try:
            return json.loads(_extract_json_payload(payload))
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


def _extract_json_payload(text: str) -> str:
    fenced = re.search(r"```json\s*(\{.*\})\s*```", text, flags=re.DOTALL)
    if fenced:
        return fenced.group(1)

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start : end + 1]
    return text


def _parse_cookie_string(raw: str) -> dict[str, str]:
    cookies: dict[str, str] = {}
    for item in raw.split(";"):
        item = item.strip()
        if not item or "=" not in item:
            continue
        key, value = item.split("=", 1)
        cookies[key.strip()] = value.strip()
    return cookies


def _build_cookie_injection_script(cookies: dict[str, str]) -> str:
    payload = json.dumps(cookies, ensure_ascii=False)
    return f"""(() => {{
  const cookies = {payload};
  const applied = [];
  for (const [key, value] of Object.entries(cookies)) {{
    document.cookie = `${{key}}=${{value}}; path=/; domain=.douban.com; SameSite=Lax`;
    applied.push(key);
  }}
  return JSON.stringify({{ applied, cookie: document.cookie }});
}})()"""


def _build_set_cookies_function(cookies: dict[str, str]) -> str:
    payload = json.dumps(cookies, ensure_ascii=False)
    return f"""() => {{
  const cookies = {payload};
  const applied = [];
  for (const [key, value] of Object.entries(cookies)) {{
    document.cookie = `${{key}}=${{value}}; path=/; domain=.douban.com; SameSite=Lax`;
    applied.push(key);
  }}
  return {{ applied, cookie: document.cookie }};
}}"""


def create_page_fetcher(backend: str) -> PageFetcher:
    if backend == "mcp":
        return ChromeMcpPageFetcher()
    if backend == "http":
        return HttpPageFetcher(proxy_pool=_load_mock_proxy_pool())
    raise ValueError(f"不支持的抓取后端: {backend}")


def _should_failover_status(status_code: int) -> bool:
    return status_code in {407, 502, 503, 504}


def _is_definitive_route_error(exc: Exception) -> bool:
    if isinstance(exc, RouteFailure):
        return exc.definitive
    return isinstance(exc, (httpx.ProxyError, httpx.ConnectError, httpx.ConnectTimeout))


def _load_mock_proxy_pool() -> MockProxyPool | None:
    if PROXY_POOL_MODE != "mock":
        return None
    try:
        pool = build_mock_proxy_pool(PROXY_POOL_CONFIG, max_failures=PROXY_MAX_FAILURES)
    except ValueError as exc:
        logger.error("mock 代理池配置无效: %s", exc)
        return None
    if pool:
        logger.info("已启用 mock 代理池，共 %s 个节点", len(pool.snapshot()))
    else:
        logger.warning("DOUBAN_PROXY_POOL_MODE=mock 但未提供可用节点，已忽略")
    return pool
