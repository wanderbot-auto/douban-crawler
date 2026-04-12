"""Global configuration."""

from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

DB_PATH = DATA_DIR / "douban_group.db"

DOUBAN_BASE_URL = "https://www.douban.com"
DEFAULT_GROUP_ID = "638298"
TOPICS_PER_PAGE = 25

REQUEST_TIMEOUT = 30
MAX_RETRIES = 5
RETRY_BASE_DELAY = 3

MIN_REQUEST_INTERVAL = float(os.environ.get("DOUBAN_MIN_REQUEST_INTERVAL", "3.0"))
MAX_REQUEST_INTERVAL = float(os.environ.get("DOUBAN_MAX_REQUEST_INTERVAL", "8.0"))
LONG_PAUSE_EVERY = int(os.environ.get("DOUBAN_LONG_PAUSE_EVERY", "15"))
LONG_PAUSE_MIN = float(os.environ.get("DOUBAN_LONG_PAUSE_MIN", "15.0"))
LONG_PAUSE_MAX = float(os.environ.get("DOUBAN_LONG_PAUSE_MAX", "30.0"))

FETCH_BACKEND = "http"
DOUBAN_COOKIE = os.environ.get("DOUBAN_COOKIE", "")

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
