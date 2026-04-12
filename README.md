# Douban Group Crawler

豆瓣小组帖子抓取与存储工具。

当前版本已移除所有 MCP / Chrome DevTools 相关逻辑，只保留 HTTP 直连请求模式。

## 安装

```bash
pip install -e .
```

## 用法

```bash
# 抓取默认小组
douban-crawler crawl --pages 3

# 跳过前 100 页，再抓后 20 页
douban-crawler crawl --skip-pages 100 --pages 20

# 指定小组
douban-crawler crawl --group 12345 --pages 3

# 仅抓列表，不抓详情
douban-crawler crawl --pages 5 --no-details

# 查看统计
douban-crawler stats

# 导出 CSV
douban-crawler export --format csv
```

`run.py` 也只保留 HTTP 直连模式，不再支持任何 MCP 或浏览器连接参数。

## 环境变量

- `DOUBAN_COOKIE` - 豆瓣 Cookie，HTTP 直连时直接使用
- `DOUBAN_MIN_REQUEST_INTERVAL` / `DOUBAN_MAX_REQUEST_INTERVAL` - 常规请求间隔秒数
- `DOUBAN_LONG_PAUSE_EVERY` / `DOUBAN_LONG_PAUSE_MIN` / `DOUBAN_LONG_PAUSE_MAX` - 长暂停策略
- `LOG_LEVEL` - 日志级别，默认 `INFO`

## 数据存储

数据存储在 `data/douban_group.db`。

主要表：

- `topics` - 帖子列表信息
- `topic_details` - 帖子详情

## 项目结构

```text
douban-group-crawler/
├── README.md
├── requirements.txt
├── pyproject.toml
├── run.py
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
