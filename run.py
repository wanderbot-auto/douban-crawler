#!/usr/bin/env python3
"""
豆瓣小组爬虫 — 零依赖独立运行脚本

仅使用 Python 标准库，无需安装任何第三方包，默认通过 Chrome DevTools MCP 控制本机 Chrome 访问页面：
    python3 run.py                          # 默认爬取小组 638298，前 3 页列表
    python3 run.py --pages 10               # 爬取前 10 页
    python3 run.py --pages 5 --details      # 前 5 页 + 帖子详情与评论
    python3 run.py --pages 0                # 全部页（0=不限）
    python3 run.py --group 12345            # 指定小组
    python3 run.py --backend http           # 临时切回 HTTP 直连
    python3 run.py --stats                  # 查看数据库统计
    python3 run.py --export csv             # 导出 CSV
    python3 run.py --export json -o out.json

环境变量（可选）：
    DOUBAN_FETCH_BACKEND  页面访问后端（默认 mcp）
    DOUBAN_MCP_COMMAND    MCP 启动命令（默认 npx）
    DOUBAN_MCP_ARGS       MCP 参数（默认 chrome-devtools-mcp）
    DOUBAN_COOKIE         浏览器登录后的 Cookie（仅 http 后端使用）
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import gzip
import html
import html.parser
import http.cookiejar
import json
import logging
import os
import queue
import random
import re
import shlex
import sqlite3
import ssl
import string
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

try:
    import anyio
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
except ImportError:  # 仅在 mcp 后端运行时才需要
    anyio = None
    ClientSession = None
    StdioServerParameters = None
    stdio_client = None

# ═══════════════════════════════════════════════════════════════════════════
#  全局配置
# ═══════════════════════════════════════════════════════════════════════════

DOUBAN_BASE = "https://www.douban.com"
DEFAULT_GROUP = "638298"
DEFAULT_TAB = "36280"
TOPICS_PER_PAGE = 25

REQUEST_TIMEOUT = 30
MAX_RETRIES = 5
RETRY_BASE_DELAY = 3

MIN_INTERVAL = float(os.environ.get("DOUBAN_MIN_REQUEST_INTERVAL", "3.0"))
MAX_INTERVAL = float(os.environ.get("DOUBAN_MAX_REQUEST_INTERVAL", "8.0"))
LONG_PAUSE_EVERY = int(os.environ.get("DOUBAN_LONG_PAUSE_EVERY", "15"))
LONG_PAUSE_MIN = float(os.environ.get("DOUBAN_LONG_PAUSE_MIN", "15.0"))
LONG_PAUSE_MAX = float(os.environ.get("DOUBAN_LONG_PAUSE_MAX", "30.0"))

def _env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _build_default_mcp_args() -> str:
    browser_url = os.environ.get("DOUBAN_MCP_BROWSER_URL", "").strip()
    auto_connect = _env_flag("DOUBAN_MCP_AUTO_CONNECT", False)
    channel = os.environ.get("DOUBAN_MCP_CHANNEL", "").strip()

    parts = ["-y", "chrome-devtools-mcp@latest"]
    if browser_url:
        parts.append(f"--browserUrl={browser_url}")
    elif auto_connect:
        parts.append("--autoConnect")
        if channel:
            parts.append(f"--channel={channel}")
    else:
        parts.extend(["--slim", "--headless", "--isolated", "--viewport", "1440x1800"])
    return " ".join(parts)


FETCH_BACKEND = os.environ.get("DOUBAN_FETCH_BACKEND", "mcp").strip().lower() or "mcp"
DOUBAN_COOKIE_RAW = os.environ.get("DOUBAN_COOKIE", "")
DOUBAN_COOKIE_ENV = DOUBAN_COOKIE_RAW or (
    'bid=qExCrbdUTM8; ll="108288"; _pk_id.100001.8cb4=3ea132d009284f2a.1775486888.;'
    " __utmc=30149280;"
    " __utmz=30149280.1775486888.1.1.utmcsr=rebang.today|utmccn=(referral)|utmcmd=referral|utmcct=/;"
    " __utma=30149280.826503781.1775486888.1775486888.1775574239.2;"
    " _pk_ref.100001.8cb4=%5B%22%22%2C%22%22%2C1775659208%2C%22https%3A%2F%2Frebang.today%2F%22%5D;"
    " _pk_ses.100001.8cb4=1; ap_v=0,6.0"
)
DOUBAN_MCP_COMMAND = os.environ.get("DOUBAN_MCP_COMMAND", "npx")
DOUBAN_MCP_ARGS = os.environ.get(
    "DOUBAN_MCP_ARGS",
    _build_default_mcp_args(),
)
DOUBAN_MCP_STARTUP_TIMEOUT = float(os.environ.get("DOUBAN_MCP_STARTUP_TIMEOUT", "45"))
DOUBAN_MCP_NAV_TIMEOUT = float(os.environ.get("DOUBAN_MCP_NAV_TIMEOUT", "45"))
DOUBAN_MCP_READY_TIMEOUT = float(os.environ.get("DOUBAN_MCP_READY_TIMEOUT", "20"))
DOUBAN_MCP_STABILIZE_DELAY = float(os.environ.get("DOUBAN_MCP_STABILIZE_DELAY", "1.0"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "douban_group.db"

logger = logging.getLogger("douban_crawler")

# ═══════════════════════════════════════════════════════════════════════════
#  数据模型
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class Topic:
    topic_id: str
    title: str
    url: str
    author_name: str
    author_url: str = ""
    reply_count: int = 0
    last_reply_time: str = ""
    group_id: str = ""
    tab_id: str = ""
    created_at: str = ""


@dataclass
class TopicDetail:
    topic_id: str
    title: str = ""
    content: str = ""
    content_html: str = ""
    author_name: str = ""
    author_url: str = ""
    created_time: str = ""
    like_count: int = 0
    collect_count: int = 0
    reshare_count: int = 0
    images: list[str] = field(default_factory=list)
    fetched_at: str = ""


@dataclass
class Comment:
    comment_id: str
    topic_id: str
    author_name: str = ""
    author_url: str = ""
    content: str = ""
    created_time: str = ""
    vote_count: int = 0
    reply_to: str = ""
    fetched_at: str = ""


# ═══════════════════════════════════════════════════════════════════════════
#  反爬策略
# ═══════════════════════════════════════════════════════════════════════════

_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
]


def _random_ua() -> str:
    return random.choice(_USER_AGENTS)


def _generate_bid() -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=11))


def _build_cookie_str() -> str:
    """构建请求 Cookie 头"""
    cookies = {"bid": _generate_bid()}
    if DOUBAN_COOKIE_ENV:
        for item in DOUBAN_COOKIE_ENV.split(";"):
            item = item.strip()
            if "=" in item:
                k, v = item.split("=", 1)
                cookies[k.strip()] = v.strip()
    return "; ".join(f"{k}={v}" for k, v in cookies.items())


class RateLimiter:
    def __init__(self) -> None:
        self._count = 0
        self._last = 0.0

    def wait(self) -> None:
        self._count += 1
        if self._count % LONG_PAUSE_EVERY == 0:
            pause = random.uniform(LONG_PAUSE_MIN, LONG_PAUSE_MAX)
            logger.info(f"长暂停 {pause:.1f}s（已完成 {self._count} 个请求）")
            time.sleep(pause)
            self._last = time.monotonic()
            return
        now = time.monotonic()
        interval = random.uniform(MIN_INTERVAL, MAX_INTERVAL)
        elapsed = now - self._last
        if elapsed < interval:
            time.sleep(interval - elapsed)
        self._last = time.monotonic()

    @property
    def count(self) -> int:
        return self._count


# ═══════════════════════════════════════════════════════════════════════════
#  页面访问层（HTTP / Chrome DevTools MCP）
# ═══════════════════════════════════════════════════════════════════════════

_MCP_SNAPSHOT_SCRIPT = """({
  readyState: document.readyState,
  title: document.title,
  url: location.href,
  html: document.documentElement ? document.documentElement.outerHTML : "",
  htmlLength: document.documentElement ? document.documentElement.outerHTML.length : 0,
  bodyText: document.body ? document.body.innerText.slice(0, 400) : ""
})"""

_MCP_EVALUATE_FUNCTION = """() => ({
  readyState: document.readyState,
  title: document.title,
  url: location.href,
  html: document.documentElement ? document.documentElement.outerHTML : "",
  htmlLength: document.documentElement ? document.documentElement.outerHTML.length : 0,
  bodyText: document.body ? document.body.innerText.slice(0, 400) : ""
})"""


class HttpClient:
    """基于 urllib 的 HTTP 客户端，内置反爬 headers"""

    def __init__(self) -> None:
        self._ua = _random_ua()
        self._cookie = _build_cookie_str()
        self._ssl_ctx = ssl.create_default_context()
        self._ssl_ctx.check_hostname = False
        self._ssl_ctx.verify_mode = ssl.CERT_NONE

    def get(self, url: str, referer: str | None = None) -> tuple[int, str]:
        """发起 GET 请求，返回 (status_code, body_text)"""
        headers = {
            "User-Agent": self._ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cookie": self._cookie,
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }
        if referer:
            headers["Referer"] = referer

        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            resp = urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=self._ssl_ctx)
            data = resp.read()
            if resp.headers.get("Content-Encoding") == "gzip":
                data = gzip.decompress(data)
            charset = resp.headers.get_content_charset() or "utf-8"
            return resp.status, data.decode(charset, errors="replace")
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                pass
            return e.code, body

    def rotate(self) -> None:
        self._ua = _random_ua()
        self._cookie = _build_cookie_str()
        logger.debug("HTTP 模式已轮换 UA + Cookie")

    def close(self) -> None:
        return None


def fetch_with_retry(
    client: HttpClient,
    url: str,
    referer: str | None = None,
    max_retries: int = MAX_RETRIES,
) -> str | None:
    """带指数退避重试的页面获取，成功返回 HTML 文本，失败返回 None"""
    retryable = {403, 429, 500, 502, 503, 504}
    for attempt in range(1, max_retries + 1):
        try:
            status, body = client.get(url, referer=referer)
            if status == 200:
                return body
            if status in retryable:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 2)
                logger.warning(f"HTTP {status}，第 {attempt}/{max_retries} 次重试，等待 {delay:.1f}s")
                time.sleep(delay)
                continue
            logger.error(f"HTTP {status}，不可重试")
            return None
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            delay = RETRY_BASE_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 2)
            logger.warning(f"请求异常 {type(exc).__name__}，第 {attempt}/{max_retries} 次重试，等待 {delay:.1f}s")
            time.sleep(delay)
    logger.error(f"达到最大重试次数 {max_retries}，放弃: {url}")
    return None


class HttpPageFetcher:
    def __init__(self) -> None:
        self._client = HttpClient()

    def fetch(self, url: str, referer: str | None = None) -> str | None:
        return fetch_with_retry(self._client, url, referer=referer)

    def rotate(self) -> None:
        self._client.rotate()

    def close(self) -> None:
        self._client.close()


class ChromeMcpPageFetcher:
    def __init__(self, mcp_args: str | None = None) -> None:
        self._command = DOUBAN_MCP_COMMAND
        self._args = shlex.split(mcp_args or DOUBAN_MCP_ARGS)
        self._started = False
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._session: ClientSession | None = None
        self._shutdown_event: asyncio.Event | None = None
        self._ready = threading.Event()
        self._startup_error: Exception | None = None
        self._last_url = ""
        self._warmed_referers: set[str] = set()
        self._tool_mode = "slim"
        self._page_ready = False
        self._douban_cookies_synced = False
        self._douban_cookie_items = _parse_cookie_str(DOUBAN_COOKIE_RAW)

    def fetch(self, url: str, referer: str | None = None) -> str | None:
        self._ensure_started()
        self._sync_douban_cookies_if_needed(url)
        if referer and referer != url and referer not in self._warmed_referers and not self._last_url:
            logger.debug(f"MCP 预热 referer: {referer}")
            self._navigate(referer)
            time.sleep(0.5)
            self._warmed_referers.add(referer)

        self._navigate(url)
        snapshot = self._wait_until_ready()
        self._last_url = str(snapshot.get("url") or url)
        return str(snapshot.get("html") or "")

    def rotate(self) -> None:
        logger.debug("MCP Chrome 模式跳过 UA/Cookie 轮换")

    def _sync_douban_cookies_if_needed(self, url: str) -> None:
        if self._douban_cookies_synced or not self._douban_cookie_items or "douban.com" not in url:
            return

        logger.info(f"向 MCP 浏览器同步 {len(self._douban_cookie_items)} 个 Douban Cookie（仅浏览器可写项）")
        self._navigate(DOUBAN_BASE)
        self._set_browser_cookies(self._douban_cookie_items)
        time.sleep(0.5)
        self._douban_cookies_synced = True

    def _set_browser_cookies(self, cookies: dict[str, str]) -> None:
        if self._tool_mode == "slim":
            self._call_tool(
                "evaluate",
                {"script": _build_cookie_injection_script(cookies)},
                timeout=DOUBAN_MCP_READY_TIMEOUT,
            )
            return

        self._call_tool(
            "evaluate_script",
            {"function": _build_set_cookies_function(cookies)},
            timeout=DOUBAN_MCP_READY_TIMEOUT,
        )

    def close(self) -> None:
        if not self._started:
            return
        if self._loop and self._shutdown_event:
            self._loop.call_soon_threadsafe(self._shutdown_event.set)
        if self._thread:
            self._thread.join(timeout=10)
        self._session = None
        self._loop = None
        self._shutdown_event = None
        self._started = False

    def _ensure_started(self) -> None:
        if self._started:
            return
        if anyio is None or ClientSession is None or StdioServerParameters is None or stdio_client is None:
            raise RuntimeError(
                "mcp 后端需要先安装依赖：pip install -e . 或 pip install mcp anyio"
            )
        self._thread = threading.Thread(target=self._run_loop, name="runpy-chrome-mcp", daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=DOUBAN_MCP_STARTUP_TIMEOUT):
            raise RuntimeError("Chrome MCP 启动超时，请检查 Chrome / npx / 网络是否可用")
        if self._startup_error:
            raise RuntimeError(f"Chrome MCP 启动失败: {self._startup_error}") from self._startup_error
        if not self._session:
            raise RuntimeError("Chrome MCP 会话未建立")
        self._started = True

    def _run_loop(self) -> None:
        try:
            asyncio.run(self._session_main())
        except Exception as exc:
            self._startup_error = exc
            self._ready.set()

    async def _session_main(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._shutdown_event = asyncio.Event()
        params = StdioServerParameters(
            command=self._command,
            args=self._args,
            env=None,
        )
        logger.info("启动 Chrome MCP: %s %s", self._command, " ".join(self._args))
        async with stdio_client(params) as (read, write):
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
                        "MCP 缺少可用工具组合，需要 slim(navigate/evaluate) "
                        "或完整模式(new_page/navigate_page/evaluate_script)"
                    )
                self._session = session
                self._ready.set()
                await self._shutdown_event.wait()

    def _navigate(self, url: str) -> None:
        if self._tool_mode == "slim":
            text = self._call_tool("navigate", {"url": url}, timeout=DOUBAN_MCP_NAV_TIMEOUT)
        elif not self._page_ready:
            text = self._call_tool(
                "new_page",
                {"url": url, "timeout": int(DOUBAN_MCP_NAV_TIMEOUT * 1000)},
                timeout=DOUBAN_MCP_NAV_TIMEOUT,
            )
            self._page_ready = True
        else:
            text = self._call_tool(
                "navigate_page",
                {"type": "url", "url": url, "timeout": int(DOUBAN_MCP_NAV_TIMEOUT * 1000)},
                timeout=DOUBAN_MCP_NAV_TIMEOUT,
            )
        logger.debug(f"MCP 导航成功: {url} | {text[:120]}")

    def _wait_until_ready(self) -> dict[str, Any]:
        deadline = time.time() + DOUBAN_MCP_READY_TIMEOUT
        last_length = -1
        stable_hits = 0
        snapshot: dict[str, Any] = {}

        while time.time() < deadline:
            snapshot = self._snapshot()
            ready_state = snapshot.get("readyState")
            html_length = int(snapshot.get("htmlLength") or 0)
            if ready_state == "complete" and html_length > 0:
                stable_hits = stable_hits + 1 if html_length == last_length else 0
                if stable_hits >= 1:
                    time.sleep(DOUBAN_MCP_STABILIZE_DELAY)
                    return snapshot
            last_length = html_length
            time.sleep(0.5)

        logger.warning(f"MCP 页面等待超时，返回当前快照: {snapshot.get('url')}")
        return snapshot

    def _snapshot(self) -> dict[str, Any]:
        if self._tool_mode == "slim":
            payload = self._call_tool(
                "evaluate",
                {"script": _MCP_SNAPSHOT_SCRIPT},
                timeout=DOUBAN_MCP_READY_TIMEOUT,
            )
        else:
            payload = self._call_tool(
                "evaluate_script",
                {"function": _MCP_EVALUATE_FUNCTION},
                timeout=DOUBAN_MCP_READY_TIMEOUT,
            )
        try:
            return json.loads(_extract_json_payload(payload))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"MCP evaluate 返回不可解析数据: {payload[:200]}") from exc

    def _call_tool(self, name: str, arguments: dict[str, Any], timeout: float) -> str:
        session = self._require_session()
        future = asyncio.run_coroutine_threadsafe(
            self._call_tool_async(session, name, arguments, timeout),
            self._require_loop(),
        )
        return future.result(timeout=timeout + 10)

    async def _call_tool_async(
        self,
        session: ClientSession,
        name: str,
        arguments: dict[str, Any],
        timeout: float,
    ) -> str:
        result = await session.call_tool(
            name,
            arguments,
            read_timeout_seconds=timedelta(seconds=timeout),
        )
        texts = [item.text for item in result.content if getattr(item, "type", None) == "text"]
        text = "\n".join(texts).strip()
        if result.isError:
            raise RuntimeError(text or f"MCP 工具 {name} 调用失败")
        return text

    def _require_session(self) -> ClientSession:
        if not self._session:
            raise RuntimeError("Chrome MCP 会话未建立")
        return self._session

    def _require_loop(self) -> asyncio.AbstractEventLoop:
        if not self._loop:
            raise RuntimeError("Chrome MCP 事件循环未就绪")
        return self._loop


def _extract_json_payload(text: str) -> str:
    fenced = re.search(r"```json\s*(\{.*\})\s*```", text, flags=re.DOTALL)
    if fenced:
        return fenced.group(1)

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start : end + 1]
    return text


def _parse_cookie_str(raw: str) -> dict[str, str]:
    cookies: dict[str, str] = {}
    for item in raw.split(";"):
        item = item.strip()
        if not item or "=" not in item:
            continue
        k, v = item.split("=", 1)
        cookies[k.strip()] = v.strip()
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


def create_page_fetcher(
    backend: str,
    mcp_args: str | None = None,
) -> HttpPageFetcher | ChromeMcpPageFetcher:
    if backend == "mcp":
        return ChromeMcpPageFetcher(mcp_args=mcp_args)
    if backend == "http":
        return HttpPageFetcher()
    raise ValueError(f"不支持的页面访问后端: {backend}")


# ═══════════════════════════════════════════════════════════════════════════
#  HTML 解析（纯 stdlib，基于正则 + html.parser）
# ═══════════════════════════════════════════════════════════════════════════


def _unescape(text: str) -> str:
    return html.unescape(text).strip()


def _extract_number(text: str) -> int:
    m = re.search(r"(\d+)", text)
    return int(m.group(1)) if m else 0


def _extract_topic_id(url: str) -> str:
    m = re.search(r"/topic/(\d+)", url)
    return m.group(1) if m else ""


def _full_url(href: str) -> str:
    if href.startswith("http"):
        return href
    return urllib.parse.urljoin(DOUBAN_BASE, href)


def _strip_tags(html_str: str) -> str:
    """移除 HTML 标签，保留纯文本"""
    text = re.sub(r"<br\s*/?>", "\n", html_str, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    return _unescape(text).strip()


def _check_blocked(body: str) -> None:
    if "检测到有异常请求" in body or "异常请求" in body:
        logger.error("被豆瓣反爬机制拦截！建议：增大请求间隔 / 更换 IP / 设置 DOUBAN_COOKIE")
    elif re.search(r"<title>[^<]*(登录|登录跳转)[^<]*</title>", body) and "豆瓣" in body:
        logger.warning("当前命中登录跳转页；优先连接已登录的 Chrome，必要时再补 DOUBAN_COOKIE")


# ---------------------------------------------------------------------------
#  列表页解析
# ---------------------------------------------------------------------------

# 匹配讨论列表表格中每一行（<tr> 块）
_TR_PATTERN = re.compile(
    r'<tr\s[^>]*class=""[^>]*>(.*?)</tr>', re.S
)
# 匹配标题列：<td class="title"> <a href="URL" title="TITLE" ...>TEXT</a>
_TITLE_PATTERN = re.compile(
    r'<td\s+class="title">\s*<a\s+href="([^"]*)"[^>]*title="([^"]*)"', re.S
)
# 匹配作者列
_AUTHOR_PATTERN = re.compile(
    r'<td\s+nowrap="nowrap">\s*<a\s+href="([^"]*)"[^>]*>([^<]*)</a>', re.S
)
# 回复数列
_REPLY_PATTERN = re.compile(r'<td\s+class="r-count">\s*(\d*)\s*</td>', re.S)
# 时间列
_TIME_PATTERN = re.compile(r'<td\s+class="time">\s*([^<]*?)\s*</td>', re.S)


def _dump_debug_html(body: str, label: str) -> None:
    """将解析失败的 HTML 保存到 data/ 下，便于诊断"""
    try:
        debug_dir = DATA_DIR / "debug"
        debug_dir.mkdir(exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = debug_dir / f"{label}_{ts}.html"
        path.write_text(body[:50000], encoding="utf-8")  # 最多保留 50KB
        logger.debug(f"已保存调试 HTML → {path}")
    except Exception:
        pass


def parse_topic_list(body: str, group_id: str, tab_id: str = "") -> list[Topic]:
    """从讨论列表页 HTML 解析帖子列表。返回空列表时调用方应区分：是真的无帖子还是解析失败。"""
    topics: list[Topic] = []
    now_str = datetime.now().isoformat()

    # 找到 <table class="olt"> ... </table> 区域
    table_m = re.search(r'<table\s+class="olt">(.*?)</table>', body, re.S)
    if not table_m:
        _check_blocked(body)
        _dump_debug_html(body, "no_table")
        logger.warning("未找到讨论列表表格 (.olt)，可能被反爬拦截")
        return topics

    table_html = table_m.group(1)

    # 逐行解析 <tr>
    for tr_m in re.finditer(r"<tr\s[^>]*>(.*?)</tr>", table_html, re.S):
        row = tr_m.group(1)
        if "<th" in row:
            continue

        title_m = _TITLE_PATTERN.search(row)
        if not title_m:
            continue

        url = _full_url(title_m.group(1))
        title = _unescape(title_m.group(2))
        topic_id = _extract_topic_id(url)
        if not topic_id:
            continue

        author_name, author_url = "", ""
        author_m = _AUTHOR_PATTERN.search(row)
        if author_m:
            author_url = author_m.group(1)
            author_name = _unescape(author_m.group(2))

        reply_count = 0
        reply_m = _REPLY_PATTERN.search(row)
        if reply_m and reply_m.group(1):
            reply_count = int(reply_m.group(1))

        last_reply_time = ""
        time_m = _TIME_PATTERN.search(row)
        if time_m:
            last_reply_time = time_m.group(1).strip()

        topics.append(Topic(
            topic_id=topic_id, title=title, url=url,
            author_name=author_name, author_url=author_url,
            reply_count=reply_count, last_reply_time=last_reply_time,
            group_id=group_id, tab_id=tab_id, created_at=now_str,
        ))

    logger.info(f"从列表页解析到 {len(topics)} 个帖子")
    return topics


def parse_total_pages(body: str) -> int:
    """从列表页解析总页数"""
    m = re.search(r'data-total-page="(\d+)"', body)
    if m:
        return int(m.group(1))
    pages = [int(x) for x in re.findall(r'<a[^>]*>\s*(\d+)\s*</a>', body) if x.isdigit()]
    return max(pages) if pages else 1


# ---------------------------------------------------------------------------
#  详情页解析
# ---------------------------------------------------------------------------

def parse_topic_detail(body: str, topic_id: str) -> TopicDetail | None:
    """从帖子详情页 HTML 解析内容"""
    _check_blocked(body)
    now_str = datetime.now().isoformat()
    detail = TopicDetail(topic_id=topic_id, fetched_at=now_str)

    # 标题
    title_m = re.search(r"<h1[^>]*>(.*?)</h1>", body, re.S)
    if title_m:
        detail.title = _strip_tags(title_m.group(1))

    # 正文区域
    content_m = re.search(
        r'<div\s+class="topic-richtext"[^>]*>(.*?)</div>',
        body, re.S,
    ) or re.search(
        r'<div\s+class="topic-content"[^>]*>(.*?)</div>',
        body, re.S,
    )
    if content_m:
        detail.content_html = content_m.group(1)
        detail.content = _strip_tags(detail.content_html)
        # 提取图片链接（仅保存链接）
        detail.images = re.findall(r'<img[^>]+src="([^"]+)"', detail.content_html)

    # 作者
    from_m = re.search(
        r'class="topic-doc".*?class="from".*?<a\s+href="([^"]*)"[^>]*>([^<]*)</a>',
        body, re.S,
    )
    if from_m:
        detail.author_url = from_m.group(1)
        detail.author_name = _unescape(from_m.group(2))

    # 发布时间
    time_m = re.search(r'class="color-green">\s*([^<]+?)\s*<', body)
    if time_m:
        detail.created_time = time_m.group(1).strip()

    return detail


# ---------------------------------------------------------------------------
#  评论解析
# ---------------------------------------------------------------------------

_COMMENT_ITEM = re.compile(
    r'<li\s[^>]*?data-cid="(\d+)"[^>]*>(.*?)</li>',
    re.S,
)


def parse_comments(body: str, topic_id: str) -> list[Comment]:
    """从帖子页面解析评论"""
    comments: list[Comment] = []
    now_str = datetime.now().isoformat()

    for m in _COMMENT_ITEM.finditer(body):
        cid = m.group(1)
        block = m.group(2)

        comment = Comment(comment_id=cid, topic_id=topic_id, fetched_at=now_str)

        # 作者
        author_m = re.search(r'<a\s+href="([^"]*)"[^>]*class="[^"]*"[^>]*>([^<]+)</a>', block)
        if not author_m:
            author_m = re.search(r'<a\s+href="(https://www\.douban\.com/people/[^"]*)"[^>]*>([^<]+)</a>', block)
        if author_m:
            comment.author_url = author_m.group(1)
            comment.author_name = _unescape(author_m.group(2))

        # 时间
        time_m = re.search(r'class="pubtime"[^>]*>([^<]*)<', block)
        if time_m:
            comment.created_time = time_m.group(1).strip()

        # 内容
        p_m = re.search(r'<p\s+class="reply-content"[^>]*>(.*?)</p>', block, re.S)
        if not p_m:
            p_m = re.search(r'<p[^>]*>(.*?)</p>', block, re.S)
        if p_m:
            comment.content = _strip_tags(p_m.group(1))

        # 赞数
        vote_m = re.search(r'class="vote-count"[^>]*>(\d+)<', block)
        if vote_m:
            comment.vote_count = int(vote_m.group(1))

        if comment.content:
            comments.append(comment)

    logger.debug(f"帖子 {topic_id} 解析到 {len(comments)} 条评论")
    return comments


def has_next_comment_page(body: str) -> str | None:
    """返回下一页评论的 URL，无则返回 None"""
    m = re.search(r'class="next">\s*<a\s+href="([^"]*)"', body)
    if m:
        return _unescape(m.group(1))
    return None


# ═══════════════════════════════════════════════════════════════════════════
#  SQLite 存储
# ═══════════════════════════════════════════════════════════════════════════

_SCHEMA = """
CREATE TABLE IF NOT EXISTS topics (
    topic_id TEXT PRIMARY KEY, title TEXT NOT NULL, url TEXT NOT NULL,
    author_name TEXT, author_url TEXT, reply_count INTEGER DEFAULT 0,
    last_reply_time TEXT, group_id TEXT, tab_id TEXT DEFAULT '',
    created_at TEXT, updated_at TEXT
);
CREATE TABLE IF NOT EXISTS topic_details (
    topic_id TEXT PRIMARY KEY, title TEXT, content TEXT, content_html TEXT,
    author_name TEXT, author_url TEXT, created_time TEXT,
    like_count INTEGER DEFAULT 0, collect_count INTEGER DEFAULT 0,
    reshare_count INTEGER DEFAULT 0, images TEXT, fetched_at TEXT
);
CREATE TABLE IF NOT EXISTS comments (
    comment_id TEXT, topic_id TEXT, author_name TEXT, author_url TEXT,
    content TEXT, created_time TEXT, vote_count INTEGER DEFAULT 0,
    reply_to TEXT, fetched_at TEXT, PRIMARY KEY (comment_id, topic_id)
);
CREATE INDEX IF NOT EXISTS idx_topics_group ON topics(group_id);
CREATE INDEX IF NOT EXISTS idx_comments_topic ON comments(topic_id);
"""


class Storage:
    def __init__(self, db_path: str | Path | None = None) -> None:
        self.db_path = str(db_path or DB_PATH)
        with self._conn() as c:
            c.executescript(_SCHEMA)
            self._migrate(c)

    @staticmethod
    def _migrate(c: sqlite3.Connection) -> None:
        """兼容旧数据库：补齐新增的列"""
        cols = {row[1] for row in c.execute("PRAGMA table_info(topics)")}
        if "tab_id" not in cols:
            c.execute("ALTER TABLE topics ADD COLUMN tab_id TEXT DEFAULT ''")
        if "updated_at" not in cols:
            c.execute("ALTER TABLE topics ADD COLUMN updated_at TEXT")

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def save_topics(self, topics: list[Topic]) -> tuple[int, int]:
        """保存帖子，自动去重：新增插入，数据变化则更新。返回 (new, updated) 数量。"""
        if not topics:
            return 0, 0
        new_count = 0
        updated_count = 0
        now_str = datetime.now().isoformat()
        with self._conn() as c:
            for t in topics:
                row = c.execute("SELECT title, reply_count, last_reply_time FROM topics WHERE topic_id=?",
                                (t.topic_id,)).fetchone()
                if row is None:
                    c.execute(
                        "INSERT INTO topics VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (t.topic_id, t.title, t.url, t.author_name, t.author_url,
                         t.reply_count, t.last_reply_time, t.group_id, t.tab_id,
                         t.created_at, t.created_at),
                    )
                    new_count += 1
                else:
                    old_title, old_reply, old_time = row
                    if (old_title != t.title or old_reply != t.reply_count
                            or old_time != t.last_reply_time):
                        c.execute(
                            "UPDATE topics SET title=?, reply_count=?, last_reply_time=?, updated_at=? WHERE topic_id=?",
                            (t.title, t.reply_count, t.last_reply_time, now_str, t.topic_id),
                        )
                        updated_count += 1
        if new_count or updated_count:
            logger.info(f"帖子入库: 新增 {new_count}, 更新 {updated_count}, 跳过 {len(topics) - new_count - updated_count}")
        return new_count, updated_count

    def save_detail(self, d: TopicDetail) -> bool:
        """保存详情，数据不同则更新，返回是否有写入。"""
        imgs_json = json.dumps(d.images, ensure_ascii=False)
        with self._conn() as c:
            row = c.execute("SELECT content, like_count, collect_count FROM topic_details WHERE topic_id=?",
                            (d.topic_id,)).fetchone()
            if row is None:
                c.execute(
                    "INSERT INTO topic_details VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (d.topic_id, d.title, d.content, d.content_html, d.author_name,
                     d.author_url, d.created_time, d.like_count, d.collect_count,
                     d.reshare_count, imgs_json, d.fetched_at),
                )
                return True
            old_content, old_like, old_collect = row
            if old_content != d.content or old_like != d.like_count or old_collect != d.collect_count:
                c.execute(
                    """UPDATE topic_details SET title=?, content=?, content_html=?,
                       like_count=?, collect_count=?, reshare_count=?, images=?, fetched_at=?
                       WHERE topic_id=?""",
                    (d.title, d.content, d.content_html, d.like_count, d.collect_count,
                     d.reshare_count, imgs_json, d.fetched_at, d.topic_id),
                )
                return True
        return False

    def save_comments(self, comments: list[Comment]) -> tuple[int, int]:
        """保存评论，去重并更新变化的数据。返回 (new, updated)。"""
        if not comments:
            return 0, 0
        new_count = 0
        updated_count = 0
        with self._conn() as c:
            for cm in comments:
                row = c.execute("SELECT content, vote_count FROM comments WHERE comment_id=? AND topic_id=?",
                                (cm.comment_id, cm.topic_id)).fetchone()
                if row is None:
                    c.execute(
                        "INSERT INTO comments VALUES (?,?,?,?,?,?,?,?,?)",
                        (cm.comment_id, cm.topic_id, cm.author_name, cm.author_url,
                         cm.content, cm.created_time, cm.vote_count, cm.reply_to, cm.fetched_at),
                    )
                    new_count += 1
                else:
                    old_content, old_vote = row
                    if old_content != cm.content or old_vote != cm.vote_count:
                        c.execute(
                            "UPDATE comments SET content=?, vote_count=?, fetched_at=? WHERE comment_id=? AND topic_id=?",
                            (cm.content, cm.vote_count, cm.fetched_at, cm.comment_id, cm.topic_id),
                        )
                        updated_count += 1
        return new_count, updated_count

    def get_topic_ids(self, group_id: str) -> set[str]:
        with self._conn() as c:
            return {r[0] for r in c.execute("SELECT topic_id FROM topics WHERE group_id=?", (group_id,))}

    def get_fetched_detail_ids(self) -> set[str]:
        with self._conn() as c:
            return {r[0] for r in c.execute("SELECT topic_id FROM topic_details")}

    def count(self, table: str, group_id: str | None = None) -> int:
        with self._conn() as c:
            if group_id and table == "topics":
                return c.execute(f"SELECT COUNT(*) FROM {table} WHERE group_id=?", (group_id,)).fetchone()[0]
            return c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# ═══════════════════════════════════════════════════════════════════════════
#  爬虫核心
# ═══════════════════════════════════════════════════════════════════════════


class DoubanGroupCrawler:
    def __init__(
        self, group_id: str = DEFAULT_GROUP, tab_id: str = "",
        storage: Storage | None = None,
        max_pages: int = 0, skip_pages: int = 0,
        fetch_details: bool = False, fetch_comments: bool = False,
        fetch_backend: str = FETCH_BACKEND,
        mcp_args: str | None = None,
    ) -> None:
        self.group_id = group_id
        self.tab_id = tab_id
        self.storage = storage or Storage()
        self.max_pages = max_pages
        self.skip_pages = skip_pages
        self.fetch_details = fetch_details
        self.fetch_comments = fetch_comments
        self.fetch_backend = fetch_backend
        self.mcp_args = mcp_args
        self._fetcher = create_page_fetcher(fetch_backend, mcp_args=mcp_args)
        self._limiter = RateLimiter()
        self.stats: dict[str, int] = {
            "pages_fetched": 0, "pages_skipped": 0, "topics_found": 0, "topics_new": 0,
            "topics_updated": 0, "details_fetched": 0,
            "comments_fetched": 0, "comments_updated": 0, "errors": 0,
        }

    # -- 公开接口 --

    def run(self) -> dict[str, int]:
        tab_info = f" tab={self.tab_id}" if self.tab_id else ""
        logger.info(f"开始爬取小组 {self.group_id}{tab_info}")
        logger.info(f"页面访问后端: {self.fetch_backend}")
        logger.info(f"页数配置: max_pages={self.max_pages or '全部'}, skip_pages={self.skip_pages}")
        try:
            topics = self._crawl_list()
            if self.fetch_details and topics:
                self._crawl_details(topics)
        except KeyboardInterrupt:
            logger.warning("用户中断")
        except Exception as exc:
            logger.error(f"爬取异常: {exc}", exc_info=True)
            self.stats["errors"] += 1
        finally:
            self._fetcher.close()
        return self.stats

    # -- 列表爬取 --

    def _crawl_list(self) -> list[Topic]:
        all_topics: list[Topic] = []
        existing = self.storage.get_topic_ids(self.group_id)
        logger.info(f"数据库已有 {len(existing)} 条帖子记录")

        first_html = self._fetch_list_page(0)
        if not first_html:
            logger.error("无法获取第一页")
            return all_topics

        total = parse_total_pages(first_html)
        start_page = self.skip_pages + 1
        if start_page > total:
            self.stats["pages_skipped"] = total
            logger.warning(f"跳过页数 {self.skip_pages} 已覆盖全部 {total} 页，本次无需爬取列表")
            return all_topics

        end_page = total
        if self.max_pages > 0:
            end_page = min(total, self.skip_pages + self.max_pages)

        self.stats["pages_skipped"] = start_page - 1
        logger.info(f"共 {total} 页，跳过 {self.stats['pages_skipped']} 页，实际爬取第 {start_page} 到第 {end_page} 页")

        if start_page == 1:
            topics = parse_topic_list(first_html, self.group_id, self.tab_id)
        else:
            start = (start_page - 1) * TOPICS_PER_PAGE
            logger.info(f"爬取第 {start_page}/{total} 页 (start={start})")
            topics = self._fetch_and_parse_list(start, start_page)

        if not topics:
            logger.warning(f"第 {start_page} 页未解析到帖子")
            return all_topics

        all_topics.extend(self._save(topics, existing))
        self.stats["pages_fetched"] += 1

        consecutive_empty = 0
        max_empty_retries = 3  # 连续空页重试次数

        for page in range(start_page + 1, end_page + 1):
            start = (page - 1) * TOPICS_PER_PAGE
            logger.info(f"爬取第 {page}/{total} 页 (start={start})")

            topics = self._fetch_and_parse_list(start, page)

            if not topics:
                consecutive_empty += 1
                if consecutive_empty >= max_empty_retries:
                    logger.warning(f"连续 {consecutive_empty} 页为空，停止翻页")
                    break
                # 不立即放弃，继续下一页
                continue

            consecutive_empty = 0  # 成功拿到数据，重置计数
            all_topics.extend(self._save(topics, existing))
            self.stats["pages_fetched"] += 1

        logger.info(f"列表完成: 发现 {self.stats['topics_found']}，新增 {self.stats['topics_new']}")
        return all_topics

    def _save(self, topics: list[Topic], existing: set[str]) -> list[Topic]:
        self.stats["topics_found"] += len(topics)
        new_count, updated_count = self.storage.save_topics(topics)
        self.stats["topics_new"] += new_count
        self.stats["topics_updated"] += updated_count
        new = [t for t in topics if t.topic_id not in existing]
        existing.update(t.topic_id for t in topics)
        return new

    def _fetch_and_parse_list(self, start: int, page: int) -> list[Topic]:
        """获取并解析列表页，解析失败时自动重试（含加长等待）"""
        page_retry = 2  # 单页最多额外重试次数
        for attempt in range(1 + page_retry):
            page_html = self._fetch_list_page(start)
            if not page_html:
                self.stats["errors"] += 1
                if attempt < page_retry:
                    wait = random.uniform(10, 20)
                    logger.info(f"第 {page} 页获取失败，等待 {wait:.0f}s 后重试 ({attempt+1}/{page_retry})")
                    time.sleep(wait)
                    self._fetcher.rotate()
                continue

            topics = parse_topic_list(page_html, self.group_id, self.tab_id)
            if topics:
                return topics

            # 解析到空列表 — 可能是反爬，重试
            if attempt < page_retry:
                wait = random.uniform(15, 30)
                logger.warning(f"第 {page} 页解析为空，等待 {wait:.0f}s 后重试 ({attempt+1}/{page_retry})")
                time.sleep(wait)
                self._fetcher.rotate()
            else:
                logger.warning(f"第 {page} 页重试 {page_retry} 次后仍为空，跳过")
                self.stats["errors"] += 1

        return []

    # -- 详情爬取 --

    def _crawl_details(self, topics: list[Topic]) -> None:
        done = self.storage.get_fetched_detail_ids()
        pending = [t for t in topics if t.topic_id not in done]
        logger.info(f"待爬详情: {len(pending)} 个（跳过 {len(topics) - len(pending)} 个）")

        for i, t in enumerate(pending, 1):
            logger.info(f"[{i}/{len(pending)}] {t.title[:40]}")
            page_html = self._fetch_page(t.url)
            if not page_html:
                self.stats["errors"] += 1
                continue

            detail = parse_topic_detail(page_html, t.topic_id)
            if detail:
                self.storage.save_detail(detail)
                self.stats["details_fetched"] += 1

            if self.fetch_comments:
                comments = parse_comments(page_html, t.topic_id)
                # 评论翻页
                next_url = has_next_comment_page(page_html)
                while next_url:
                    next_url = _full_url(next_url) if not next_url.startswith("http") else next_url
                    c_html = self._fetch_page(next_url)
                    if not c_html:
                        break
                    page_comments = parse_comments(c_html, t.topic_id)
                    if not page_comments:
                        break
                    comments.extend(page_comments)
                    next_url = has_next_comment_page(c_html)

                if comments:
                    c_new, c_upd = self.storage.save_comments(comments)
                    self.stats["comments_fetched"] += c_new
                    self.stats["comments_updated"] += c_upd

    # -- HTTP --

    def _fetch_list_page(self, start: int) -> str | None:
        if self.tab_id:
            url = f"{DOUBAN_BASE}/group/{self.group_id}/discussion?start={start}&type=new&tab={self.tab_id}"
        else:
            url = f"{DOUBAN_BASE}/group/{self.group_id}/discussion?start={start}&type=new"
        return self._fetch_page(url, referer=f"{DOUBAN_BASE}/group/{self.group_id}/")

    def _fetch_page(self, url: str, referer: str | None = None) -> str | None:
        self._limiter.wait()
        if self._limiter.count % 10 == 0:
            self._fetcher.rotate()
        return self._fetcher.fetch(url, referer=referer)


# ═══════════════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════════════


def print_stats(group_id: str) -> None:
    s = Storage()
    print(f"\n{'=' * 42}")
    print(f"  数据库统计 — 小组 {group_id}")
    print(f"{'=' * 42}")
    print(f"  帖子(列表):  {s.count('topics', group_id)}")
    print(f"  帖子(详情):  {s.count('topic_details')}")
    print(f"  评论:        {s.count('comments')}")
    print(f"  数据库路径:  {DB_PATH}")
    print(f"{'=' * 42}\n")


def do_export(group_id: str, fmt: str, output: str | None) -> None:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("""
        SELECT t.topic_id, t.title, t.url, t.author_name, t.reply_count,
               t.last_reply_time, d.content, d.created_time,
               d.like_count, d.collect_count, d.images
        FROM topics t LEFT JOIN topic_details d ON t.topic_id = d.topic_id
        WHERE t.group_id = ? ORDER BY t.reply_count DESC
    """, (group_id,)).fetchall()]
    conn.close()
    if not rows:
        print("没有数据可导出")
        return
    out = output or str(DATA_DIR / f"export_{group_id}.{fmt}")
    if fmt == "csv":
        with open(out, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows(rows)
    else:
        with open(out, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"已导出 {len(rows)} 条 → {out}")


def build_mcp_args_override(
    browser_url: str | None,
    auto_connect: bool,
    channel: str,
) -> str | None:
    """根据 CLI 参数组装官方 chrome-devtools-mcp 启动参数。"""
    browser_url = (browser_url or "").strip()
    channel = channel.strip()

    if browser_url and auto_connect:
        raise ValueError("--mcp-browser-url 与 --mcp-auto-connect 不能同时使用")

    if not browser_url and not auto_connect and not channel:
        return None

    parts = ["-y", "chrome-devtools-mcp@latest"]
    if browser_url:
        parts.append(f"--browserUrl={browser_url}")
    else:
        parts.append("--autoConnect")
        if channel:
            parts.append(f"--channel={channel}")
    return " ".join(parts)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="豆瓣小组爬虫（零依赖，纯 Python 标准库）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
示例:
  python3 run.py                            通过 Chrome MCP 爬取默认小组默认 tab 前 3 页
  python3 run.py -p 10                      爬取前 10 页
  python3 run.py --skip-pages 100 -p 20     跳过前 100 页，再爬 20 页
  python3 run.py -t 36280 -p 5 --details    指定 tab + 详情 + 评论
  python3 run.py -t "" -p 5                 不指定 tab（爬 /discussion）
  python3 run.py -p 5 --details --no-comments  详情但不爬评论
  python3 run.py --mcp-auto-connect         连接现有 Chrome（需已开启 remote debugging）
  python3 run.py --mcp-browser-url http://127.0.0.1:9222
                                            连接现有可调试浏览器
  python3 run.py --backend http             切回 HTTP 直连
  python3 run.py -p 0                       全部页
  python3 run.py -g 12345 -t 99999          指定小组和 tab
  python3 run.py --stats                    数据库统计
  python3 run.py --export csv               导出 CSV
  python3 run.py --export json -o out.json  导出 JSON

环境变量:
  DOUBAN_FETCH_BACKEND  页面访问后端（默认 mcp）
  DOUBAN_MCP_COMMAND    MCP 启动命令（默认 npx）
  DOUBAN_MCP_ARGS       MCP 参数（默认 chrome-devtools-mcp）
  DOUBAN_MCP_BROWSER_URL  连接现有浏览器的 remote debugging 地址
  DOUBAN_MCP_AUTO_CONNECT  自动连接现有 Chrome（1/true）
  DOUBAN_MCP_CHANNEL       autoConnect 时使用的 channel
  DOUBAN_COOKIE         浏览器 Cookie（仅 http 后端使用）
""",
    )
    ap.add_argument("-g", "--group", default=DEFAULT_GROUP, help=f"小组 ID（默认 {DEFAULT_GROUP}）")
    ap.add_argument("-t", "--tab", default=DEFAULT_TAB, help=f"Tab ID（默认 {DEFAULT_TAB}，留空则爬 /discussion）")
    ap.add_argument("-p", "--pages", type=int, default=3, help="最大页数，0=全部（默认 3）")
    ap.add_argument("--skip-pages", type=int, default=0, help="跳过前 N 页后再开始爬取（默认 0）")
    ap.add_argument("--backend", choices=["mcp", "http"], default=FETCH_BACKEND, help=f"页面访问后端（默认 {FETCH_BACKEND}）")
    ap.add_argument("--mcp-browser-url", help="连接现有浏览器，例如 http://127.0.0.1:9222")
    ap.add_argument("--mcp-auto-connect", action="store_true", help="自动连接已开启 remote debugging 的现有 Chrome")
    ap.add_argument("--mcp-channel", choices=["stable", "beta", "canary", "dev"], default="", help="配合 --mcp-auto-connect 使用")
    ap.add_argument("--details", action="store_true", help="爬取帖子详情与评论")
    ap.add_argument("--no-comments", action="store_true", help="不爬评论（需配合 --details）")
    ap.add_argument("--stats", action="store_true", help="查看数据库统计")
    ap.add_argument("--export", choices=["csv", "json"], help="导出数据")
    ap.add_argument("-o", "--output", help="导出文件路径")
    ap.add_argument("--debug", action="store_true", help="调试日志")
    args = ap.parse_args()
    if args.skip_pages < 0:
        ap.error("--skip-pages 不能小于 0")

    logging.basicConfig(
        level=logging.DEBUG if args.debug else getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.stats:
        print_stats(args.group)
        return
    if args.export:
        do_export(args.group, args.export, args.output)
        return

    tab_id = args.tab.strip()
    wants_details = args.details
    wants_comments = wants_details and not args.no_comments
    try:
        mcp_args_override = build_mcp_args_override(
            browser_url=args.mcp_browser_url,
            auto_connect=args.mcp_auto_connect,
            channel=args.mcp_channel,
        )
    except ValueError as exc:
        ap.error(str(exc))

    target_url = f"https://www.douban.com/group/{args.group}/"
    if tab_id:
        target_url += f"?tab={tab_id}"

    print(f"{'=' * 48}")
    print(f"  豆瓣小组爬虫 (零依赖)")
    print(f"{'=' * 48}")
    print(f"  目标: {target_url}")
    print(f"  后端: {args.backend}")
    if args.backend == "mcp":
        if args.mcp_browser_url:
            print(f"  MCP连接: 现有浏览器 {args.mcp_browser_url}")
        elif args.mcp_auto_connect:
            channel = args.mcp_channel or "stable"
            print(f"  MCP连接: autoConnect ({channel})")
        else:
            print(f"  MCP连接: 启动独立 Chrome")
    print(f"  页数: {'不限' if args.pages == 0 else args.pages}")
    print(f"  跳过页数: {args.skip_pages}")
    print(f"  详情: {'是' if wants_details else '否'}")
    print(f"  评论: {'是' if wants_comments else '否'}")
    print(f"  图片: 仅保存链接")
    print(f"  去重: 自动（数据变化时更新）")
    print(f"{'=' * 48}\n")

    crawler = DoubanGroupCrawler(
        group_id=args.group,
        tab_id=tab_id,
        max_pages=args.pages,
        skip_pages=args.skip_pages,
        fetch_details=wants_details,
        fetch_comments=wants_comments,
        fetch_backend=args.backend,
        mcp_args=mcp_args_override,
    )
    stats = crawler.run()

    print(f"\n{'=' * 42}")
    print(f"  爬取完成")
    print(f"{'=' * 42}")
    for k, label in [
        ("pages_fetched", "页面"), ("pages_skipped", "跳过页面"),
        ("topics_found", "发现帖子"),
        ("topics_new", "新增帖子"), ("topics_updated", "更新帖子"),
        ("details_fetched", "详情"), ("comments_fetched", "新增评论"),
        ("comments_updated", "更新评论"), ("errors", "错误"),
    ]:
        print(f"  {label:　<6}  {stats[k]}")
    print(f"  数据库    {DB_PATH}")
    print(f"{'=' * 42}")


if __name__ == "__main__":
    main()
