# 豆瓣小组帖子爬取工具

聚焦于豆瓣小组帖子的爬取与数据存储。默认通过 Chrome DevTools MCP 驱动本机 Chrome 访问页面，尽量降低直接 HTTP 请求被拦截的概率。

## 快速开始

### 1. 安装依赖

```bash
pip install -e .
```

### 2. 确认本机有 Chrome 和 Node.js

程序默认通过 `npx chrome-devtools-mcp@latest` 启动 Chrome MCP，因此需要：

- 已安装 Google Chrome
- 已安装 Node.js / `npx`

### 3. 开始爬取

```bash
douban-crawler crawl --pages 3
```

## 功能

- **讨论列表爬取** — 自动分页遍历小组讨论帖
- **帖子详情爬取** — 获取帖子正文、互动数据
- **评论爬取** — 支持评论分页，完整抓取所有评论
- **增量爬取** — 自动跳过已爬取的帖子
- **数据导出** — 支持 CSV / JSON 格式导出
- **双后端访问** — 默认 `mcp`，必要时可切回 `http`

## 页面访问方式

### 默认方式：MCP 控制 Chrome

默认后端是 `mcp`，会启动官方 `chrome-devtools-mcp`，再通过 Chrome 打开豆瓣页面并提取 HTML：

```bash
douban-crawler crawl --pages 3 --backend mcp
```

这个方式相比直接 `httpx`/`urllib` 更接近真实浏览器访问链路，适合当前更容易触发拦截的页面。

### 回退方式：HTTP 直连

如需排查问题，也可以临时切回旧模式：

```bash
douban-crawler crawl --pages 3 --backend http
```

## 常用命令

```bash
# 默认小组，抓前 3 页
douban-crawler crawl --pages 3

# 只抓列表，不抓详情
douban-crawler crawl --pages 5 --no-details

# 抓详情但不抓评论
douban-crawler crawl --pages 5 --no-comments

# 指定小组
douban-crawler crawl --group 12345 --pages 3

# 开启调试日志
douban-crawler --debug crawl --pages 3

# 查看统计
douban-crawler stats

# 导出 CSV
douban-crawler export --format csv
```

## MCP 配置

默认会使用下面这组参数启动 Chrome MCP：

```bash
export DOUBAN_MCP_COMMAND="npx"
export DOUBAN_MCP_ARGS="-y chrome-devtools-mcp@latest --slim --headless --isolated --viewport 1440x1800"
```

也可以按需覆盖：

```bash
# 连接已开启 remote debugging 的 Chrome
export DOUBAN_MCP_ARGS="-y chrome-devtools-mcp@latest --slim --browserUrl http://127.0.0.1:9222"

# 或者切成有界面的浏览器
export DOUBAN_MCP_ARGS="-y chrome-devtools-mcp@latest --slim --viewport 1440x1800"
```

支持的环境变量：

- `DOUBAN_FETCH_BACKEND` — `mcp` / `http`，默认 `mcp`
- `DOUBAN_MCP_COMMAND` — MCP 启动命令，默认 `npx`
- `DOUBAN_MCP_ARGS` — Chrome MCP 参数
- `DOUBAN_MCP_STARTUP_TIMEOUT` — MCP 启动超时，默认 `45`
- `DOUBAN_MCP_NAV_TIMEOUT` — 页面导航超时，默认 `45`
- `DOUBAN_MCP_READY_TIMEOUT` — 页面就绪等待超时，默认 `20`
- `DOUBAN_MCP_STABILIZE_DELAY` — DOM 稳定额外等待秒数，默认 `1.0`
- `DOUBAN_COOKIE` — 仅 `http` 后端使用的豆瓣 Cookie
- `LOG_LEVEL` — 日志级别，默认 `INFO`

## 数据存储

数据存储在 `data/douban_group.db`（SQLite），包含三张表：

- `topics` — 帖子列表信息
- `topic_details` — 帖子详情
- `comments` — 评论数据

## 项目结构

```text
douban-group-crawler/
├── README.md
├── requirements.txt
├── pyproject.toml
├── data/
│   └── douban_group.db
└── douban_crawler/
    ├── anti_crawl.py
    ├── cli.py
    ├── config.py
    ├── crawler.py
    ├── fetcher.py
    ├── models.py
    ├── parser.py
    └── storage.py
```
