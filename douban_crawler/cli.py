"""命令行入口"""

import logging
import sys

import click
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from douban_crawler.config import DEFAULT_GROUP_ID, FETCH_BACKEND, LOG_LEVEL
from douban_crawler.crawler import DoubanGroupCrawler
from douban_crawler.storage import Storage

console = Console()


def setup_logging(level: str = LOG_LEVEL) -> None:
    """配置 Rich 日志"""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True, show_path=False)],
    )


@click.group()
@click.option("--debug", is_flag=True, help="开启调试日志")
def main(debug: bool) -> None:
    """豆瓣小组帖子爬取工具"""
    setup_logging("DEBUG" if debug else LOG_LEVEL)


@main.command()
@click.option("--group", "-g", default=DEFAULT_GROUP_ID, help="小组 ID")
@click.option("--pages", "-p", default=0, type=int, help="最大爬取页数（0=全部）")
@click.option("--no-details", is_flag=True, help="不爬取帖子详情")
@click.option("--no-comments", is_flag=True, help="不爬取评论")
@click.option("--backend", type=click.Choice(["mcp", "http"]), default=FETCH_BACKEND, show_default=True, help="页面访问后端")
def crawl(group: str, pages: int, no_details: bool, no_comments: bool, backend: str) -> None:
    """爬取小组讨论帖子"""
    comments_enabled = (not no_details) and (not no_comments)

    console.print(f"[bold green]🕷 开始爬取豆瓣小组[/bold green] [cyan]{group}[/cyan]")
    console.print(f"  目标: https://www.douban.com/group/{group}/")
    console.print(f"  页数限制: {'不限' if pages == 0 else pages}")
    console.print(f"  爬取详情: {'否' if no_details else '是'}")
    console.print(f"  爬取评论: {'是' if comments_enabled else '否'}")
    console.print(f"  访问后端: {backend}")
    console.print()

    crawler = DoubanGroupCrawler(
        group_id=group,
        max_pages=pages,
        fetch_details=not no_details,
        fetch_comments=comments_enabled,
        fetch_backend=backend,
    )

    stats = crawler.run()

    # 打印结果摘要
    console.print()
    table = Table(title="爬取结果", show_header=True, header_style="bold magenta")
    table.add_column("指标", style="cyan")
    table.add_column("数值", justify="right", style="green")
    table.add_row("爬取页数", str(stats["pages_fetched"]))
    table.add_row("发现帖子", str(stats["topics_found"]))
    table.add_row("新增帖子", str(stats["topics_new"]))
    table.add_row("详情爬取", str(stats["details_fetched"]))
    table.add_row("评论爬取", str(stats["comments_fetched"]))
    table.add_row("错误次数", str(stats["errors"]))
    console.print(table)


@main.command()
@click.option("--group", "-g", default=DEFAULT_GROUP_ID, help="小组 ID")
def stats(group: str) -> None:
    """查看数据库统计信息"""
    storage = Storage()

    table = Table(title=f"数据库统计 (小组 {group})", show_header=True, header_style="bold magenta")
    table.add_column("数据类型", style="cyan")
    table.add_column("数量", justify="right", style="green")
    table.add_row("帖子(列表)", str(storage.get_topic_count(group)))
    table.add_row("帖子(详情)", str(storage.get_detail_count()))
    table.add_row("评论", str(storage.get_comment_count()))
    console.print(table)


@main.command()
@click.option("--group", "-g", default=DEFAULT_GROUP_ID, help="小组 ID")
@click.option("--format", "-f", "fmt", type=click.Choice(["csv", "json"]), default="csv", help="导出格式")
@click.option("--output", "-o", default=None, help="输出文件路径")
def export(group: str, fmt: str, output: str) -> None:
    """导出数据"""
    import csv
    import json
    import sqlite3
    from douban_crawler.config import DB_PATH

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # 导出帖子和详情的联合数据
    cursor = conn.execute("""
        SELECT t.topic_id, t.title, t.url, t.author_name, t.reply_count,
               t.last_reply_time, d.content, d.created_time,
               d.like_count, d.collect_count
        FROM topics t
        LEFT JOIN topic_details d ON t.topic_id = d.topic_id
        WHERE t.group_id = ?
        ORDER BY t.reply_count DESC
    """, (group,))

    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()

    if not rows:
        console.print("[yellow]没有数据可导出[/yellow]")
        return

    output_path = output or f"data/export_{group}.{fmt}"

    if fmt == "csv":
        with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    elif fmt == "json":
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)

    console.print(f"[green]已导出 {len(rows)} 条数据到 {output_path}[/green]")


if __name__ == "__main__":
    main()
