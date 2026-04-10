"""用于测试 failover 逻辑的 mock 代理池。"""

from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass, field
from typing import Iterable, Mapping


@dataclass
class ProxyEndpoint:
    """单个 mock 代理节点。"""

    name: str
    proxy_url: str = ""
    metadata: dict[str, str] = field(default_factory=dict)
    consecutive_failures: int = 0
    total_failures: int = 0
    total_successes: int = 0
    last_error: str = ""
    disabled: bool = False


@dataclass
class ProxyFailureEvent:
    """一次失败上报记录，便于测试断言。"""

    proxy_name: str
    reason: str
    definitive: bool
    disabled: bool
    timestamp: float


class MockProxyPool:
    """简单的轮询 mock 代理池。

    这个实现不负责建立真实代理连接，只负责节点选择、失败计数和失效切换。
    """

    def __init__(self, endpoints: Iterable[ProxyEndpoint], max_failures: int = 2) -> None:
        self._endpoints = list(endpoints)
        self._cursor = 0
        self.max_failures = max(1, max_failures)
        self.acquire_history: list[str] = []
        self.failure_history: list[ProxyFailureEvent] = []

    def acquire(self, exclude: set[str] | None = None) -> ProxyEndpoint | None:
        if not self._endpoints:
            return None

        excluded = exclude or set()
        total = len(self._endpoints)
        for offset in range(total):
            idx = (self._cursor + offset) % total
            endpoint = self._endpoints[idx]
            if endpoint.disabled or endpoint.name in excluded:
                continue
            self._cursor = (idx + 1) % total
            self.acquire_history.append(endpoint.name)
            return endpoint
        return None

    def report_success(self, endpoint: ProxyEndpoint) -> None:
        endpoint.consecutive_failures = 0
        endpoint.last_error = ""
        endpoint.total_successes += 1

    def report_failure(self, endpoint: ProxyEndpoint, reason: str, *, definitive: bool = False) -> bool:
        endpoint.consecutive_failures += 1
        endpoint.total_failures += 1
        endpoint.last_error = reason
        if definitive or endpoint.consecutive_failures >= self.max_failures:
            endpoint.disabled = True

        self.failure_history.append(
            ProxyFailureEvent(
                proxy_name=endpoint.name,
                reason=reason,
                definitive=definitive,
                disabled=endpoint.disabled,
                timestamp=time.time(),
            )
        )
        return endpoint.disabled

    def available_count(self, exclude: set[str] | None = None) -> int:
        excluded = exclude or set()
        return sum(
            1 for endpoint in self._endpoints if not endpoint.disabled and endpoint.name not in excluded
        )

    def snapshot(self) -> list[dict[str, object]]:
        return [
            {
                "name": endpoint.name,
                "proxy_url": endpoint.proxy_url,
                "disabled": endpoint.disabled,
                "consecutive_failures": endpoint.consecutive_failures,
                "total_failures": endpoint.total_failures,
                "total_successes": endpoint.total_successes,
                "last_error": endpoint.last_error,
                "metadata": dict(endpoint.metadata),
            }
            for endpoint in self._endpoints
        ]


def build_mock_proxy_pool(
    raw: str | Iterable[str | Mapping[str, object]] | None,
    *,
    max_failures: int = 2,
) -> MockProxyPool | None:
    endpoints = _parse_endpoints(raw)
    if not endpoints:
        return None
    return MockProxyPool(endpoints, max_failures=max_failures)


def _parse_endpoints(raw: str | Iterable[str | Mapping[str, object]] | None) -> list[ProxyEndpoint]:
    if raw is None:
        return []

    items: list[str | Mapping[str, object]]
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        if text.startswith("["):
            loaded = json.loads(text)
            if not isinstance(loaded, list):
                raise ValueError("mock 代理池配置必须是 JSON 数组")
            items = loaded
        else:
            items = [part.strip() for part in text.replace(",", "\n").splitlines() if part.strip()]
    else:
        items = list(raw)

    endpoints: list[ProxyEndpoint] = []
    for index, item in enumerate(items, start=1):
        if isinstance(item, str):
            endpoints.append(_build_endpoint_from_string(item, index))
            continue
        if not isinstance(item, Mapping):
            raise ValueError(f"第 {index} 个 mock 代理节点配置无效")

        vmess_uri = item.get("vmess") or item.get("uri")
        if vmess_uri:
            endpoint = _build_endpoint_from_string(str(vmess_uri), index)
            explicit_name = item.get("name") or item.get("id")
            if explicit_name:
                endpoint.name = str(explicit_name)
            endpoint.proxy_url = str(item.get("proxy_url") or item.get("proxy") or "")
            endpoint.metadata.update(
                {
                    str(key): str(value)
                    for key, value in item.items()
                    if key not in {"name", "id", "proxy_url", "proxy", "vmess", "uri"}
                }
            )
            endpoints.append(endpoint)
            continue

        name = str(item.get("name") or item.get("id") or f"proxy-{index}")
        proxy_url = str(item.get("proxy_url") or item.get("proxy") or "")
        metadata = {
            str(key): str(value)
            for key, value in item.items()
            if key not in {"name", "id", "proxy_url", "proxy", "vmess", "uri"}
        }
        endpoints.append(ProxyEndpoint(name=name, proxy_url=proxy_url, metadata=metadata))

    return endpoints


def _build_endpoint_from_string(value: str, index: int) -> ProxyEndpoint:
    text = value.strip()
    if text.startswith("vmess://"):
        return _parse_vmess_endpoint(text, index)
    return ProxyEndpoint(name=text)


def _parse_vmess_endpoint(uri: str, index: int) -> ProxyEndpoint:
    encoded = uri[len("vmess://") :].strip()
    if not encoded:
        raise ValueError(f"第 {index} 个 vmess 节点缺少 payload")

    payload = _decode_vmess_payload(encoded, index)
    name = (
        str(payload.get("ps") or "").strip()
        or _build_vmess_fallback_name(payload, index)
    )
    metadata = {
        "mock_source": "vmess",
        "vmess_version": str(payload.get("v") or ""),
        "server_host": str(payload.get("add") or ""),
        "server_port": str(payload.get("port") or ""),
        "uuid": str(payload.get("id") or ""),
        "alter_id": str(payload.get("aid") or ""),
        "network": str(payload.get("net") or ""),
        "host_header": str(payload.get("host") or ""),
        "path": str(payload.get("path") or ""),
        "tls": str(payload.get("tls") or ""),
        "type": str(payload.get("type") or ""),
        "connectable": "false",
        "raw_uri": uri,
    }
    return ProxyEndpoint(name=name, proxy_url="", metadata=metadata)


def _decode_vmess_payload(encoded: str, index: int) -> dict[str, object]:
    padded = encoded + "=" * (-len(encoded) % 4)
    try:
        decoded = base64.b64decode(padded).decode("utf-8")
    except Exception as exc:
        raise ValueError(f"第 {index} 个 vmess 节点解码失败: {exc}") from exc

    try:
        payload = json.loads(decoded)
    except json.JSONDecodeError as exc:
        raise ValueError(f"第 {index} 个 vmess 节点 JSON 非法: {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"第 {index} 个 vmess 节点 payload 必须是 JSON 对象")
    return payload


def _build_vmess_fallback_name(payload: Mapping[str, object], index: int) -> str:
    host = str(payload.get("add") or "").strip()
    port = str(payload.get("port") or "").strip()
    if host and port:
        return f"vmess-{index}:{host}:{port}"
    if host:
        return f"vmess-{index}:{host}"
    return f"vmess-{index}"
