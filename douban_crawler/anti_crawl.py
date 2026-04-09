"""反爬策略模块：UA 轮换、Cookie 管理、请求间隔控制、重试机制"""

import random
import string
import time
import logging
from functools import wraps
from typing import Any, Callable

import httpx
try:
    from fake_useragent import UserAgent
except ImportError:
    UserAgent = None

from douban_crawler.config import (
    DOUBAN_COOKIE,
    MIN_REQUEST_INTERVAL,
    MAX_REQUEST_INTERVAL,
    LONG_PAUSE_EVERY,
    LONG_PAUSE_MIN,
    LONG_PAUSE_MAX,
    MAX_RETRIES,
    RETRY_BASE_DELAY,
    REQUEST_TIMEOUT,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# User-Agent 轮换
# ---------------------------------------------------------------------------

_ua = None


def _get_ua():
    global _ua
    if _ua is None and UserAgent is not None:
        try:
            _ua = UserAgent(browsers=["chrome", "edge"], os=["macos", "windows"])
        except Exception:
            _ua = None
    return _ua


def random_user_agent() -> str:
    """返回一个随机的桌面浏览器 User-Agent"""
    ua = _get_ua()
    if ua:
        try:
            return ua.random
        except Exception:
            pass
    # fallback 硬编码列表
    agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    ]
    return random.choice(agents)


# ---------------------------------------------------------------------------
# Cookie 管理
# ---------------------------------------------------------------------------


def generate_bid() -> str:
    """生成一个随机的豆瓣 bid cookie 值（11 位字母数字）"""
    chars = string.ascii_letters + string.digits
    return "".join(random.choices(chars, k=11))


def build_cookies() -> dict[str, str]:
    """构建请求所需的 cookie 字典

    优先使用用户提供的 DOUBAN_COOKIE，否则自动生成最小必要 cookie。
    """
    cookies: dict[str, str] = {"bid": generate_bid()}

    if DOUBAN_COOKIE:
        # 解析用户提供的 cookie 字符串，如 "bid=xxx; dbcl2=yyy"
        for item in DOUBAN_COOKIE.split(";"):
            item = item.strip()
            if "=" in item:
                k, v = item.split("=", 1)
                cookies[k.strip()] = v.strip()

    return cookies


# ---------------------------------------------------------------------------
# 请求间隔控制
# ---------------------------------------------------------------------------


class RateLimiter:
    """控制请求频率，模拟人类浏览行为"""

    def __init__(self) -> None:
        self._request_count = 0
        self._last_request_time = 0.0

    def wait(self) -> None:
        """在发起请求前调用，自动等待适当时间"""
        self._request_count += 1

        # 长暂停：每隔 N 个请求休息一次
        if self._request_count % LONG_PAUSE_EVERY == 0:
            pause = random.uniform(LONG_PAUSE_MIN, LONG_PAUSE_MAX)
            logger.info(f"长暂停 {pause:.1f}s（已完成 {self._request_count} 个请求）")
            time.sleep(pause)
            self._last_request_time = time.monotonic()
            return

        # 常规间隔
        now = time.monotonic()
        elapsed = now - self._last_request_time
        interval = random.uniform(MIN_REQUEST_INTERVAL, MAX_REQUEST_INTERVAL)
        if elapsed < interval:
            sleep_time = interval - elapsed
            logger.debug(f"等待 {sleep_time:.1f}s")
            time.sleep(sleep_time)

        self._last_request_time = time.monotonic()

    @property
    def request_count(self) -> int:
        return self._request_count


# ---------------------------------------------------------------------------
# 重试装饰器
# ---------------------------------------------------------------------------


def retry_on_failure(
    max_retries: int = MAX_RETRIES,
    base_delay: float = RETRY_BASE_DELAY,
    retryable_status: tuple[int, ...] = (403, 429, 500, 502, 503, 504),
) -> Callable:
    """带指数退避的重试装饰器，适用于返回 httpx.Response 的函数"""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> httpx.Response | None:
            last_exc: Exception | None = None
            for attempt in range(1, max_retries + 1):
                try:
                    response = func(*args, **kwargs)
                    if response.status_code == 200:
                        return response
                    if response.status_code in retryable_status:
                        delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 2)
                        logger.warning(
                            f"HTTP {response.status_code}，第 {attempt}/{max_retries} 次重试，"
                            f"等待 {delay:.1f}s"
                        )
                        time.sleep(delay)
                        continue
                    # 不可重试的状态码直接返回
                    logger.error(f"HTTP {response.status_code}，不可重试")
                    return response
                except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as exc:
                    last_exc = exc
                    delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 2)
                    logger.warning(
                        f"请求异常 {exc.__class__.__name__}，第 {attempt}/{max_retries} 次重试，"
                        f"等待 {delay:.1f}s"
                    )
                    time.sleep(delay)

            logger.error(f"达到最大重试次数 {max_retries}，放弃请求")
            if last_exc:
                raise last_exc
            return None

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# HTTP 客户端工厂
# ---------------------------------------------------------------------------


def create_client() -> httpx.Client:
    """创建配置好反爬策略的 httpx 客户端"""
    ua = random_user_agent()
    cookies = build_cookies()

    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }

    client = httpx.Client(
        headers=headers,
        cookies=cookies,
        timeout=REQUEST_TIMEOUT,
        follow_redirects=True,
        http2=False,
    )
    logger.debug(f"创建 HTTP 客户端 | UA: {ua[:60]}... | bid: {cookies.get('bid', 'N/A')}")
    return client
