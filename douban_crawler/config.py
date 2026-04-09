"""全局配置"""

from __future__ import annotations

import os
from pathlib import Path

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent

# 数据存储目录
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

# 数据库路径
DB_PATH = DATA_DIR / "douban_group.db"

# 豆瓣基础 URL
DOUBAN_BASE_URL = "https://www.douban.com"

# 默认小组 ID
DEFAULT_GROUP_ID = "638298"

# 每页帖子数（豆瓣固定值）
TOPICS_PER_PAGE = 25

# 请求配置
REQUEST_TIMEOUT = 30  # 秒
MAX_RETRIES = 5
RETRY_BASE_DELAY = 3  # 重试基础延迟（秒），指数退避

# 反爬配置
MIN_REQUEST_INTERVAL = 3.0   # 最小请求间隔（秒）
MAX_REQUEST_INTERVAL = 8.0   # 最大请求间隔（秒）
LONG_PAUSE_EVERY = 15        # 每隔 N 个请求执行一次长暂停
LONG_PAUSE_MIN = 15.0        # 长暂停最小秒数
LONG_PAUSE_MAX = 30.0        # 长暂停最大秒数

# 页面访问后端：mcp / http
FETCH_BACKEND = os.environ.get("DOUBAN_FETCH_BACKEND", "mcp").strip().lower() or "mcp"

# Cookie 配置 —— 仅 http 后端使用
DOUBAN_COOKIE = os.environ.get("DOUBAN_COOKIE", "")

# Chrome DevTools MCP 配置
DOUBAN_MCP_COMMAND = os.environ.get("DOUBAN_MCP_COMMAND", "npx")
DOUBAN_MCP_ARGS = os.environ.get(
    "DOUBAN_MCP_ARGS",
    "-y chrome-devtools-mcp@latest --slim --headless --isolated --viewport 1440x1800",
)
DOUBAN_MCP_STARTUP_TIMEOUT = float(os.environ.get("DOUBAN_MCP_STARTUP_TIMEOUT", "45"))
DOUBAN_MCP_NAV_TIMEOUT = float(os.environ.get("DOUBAN_MCP_NAV_TIMEOUT", "45"))
DOUBAN_MCP_READY_TIMEOUT = float(os.environ.get("DOUBAN_MCP_READY_TIMEOUT", "20"))
DOUBAN_MCP_STABILIZE_DELAY = float(os.environ.get("DOUBAN_MCP_STABILIZE_DELAY", "1.0"))

# 日志配置
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
