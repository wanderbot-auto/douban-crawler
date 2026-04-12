"""Command line interface."""

from __future__ import annotations

import logging

import click
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from douban_crawler.config import DEFAULT_GROUP_ID, LOG_LEVEL
from douban_crawler.crawler import DoubanGroupCrawler
from douban_crawler.storage import Storage

console = Console()


def setup_logging(level: str = LOG_LEVEL) -> None:
    """Configure Rich logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True, show_path=False)],
    )


@click.group()
@click.option("--debug", is_flag=True, help="??????")
def main(debug: bool) -> None:
    """?????????"""
    setup_logging("DEBUG" if debug else LOG_LEVEL)


@main.command()
@click.option("--group", "-g", default=DEFAULT_GROUP_ID, help="?? ID")
@click.option("--pages", "-p", default=0, type=int, help="???????0=??")
@click.option("--skip-pages", default=0, type=click.IntRange(min=0), help="??? N ????")
@click.option(
    "--service",
    type=click.Choice(["list", "detail"], case_sensitive=False),
    default="list",
    show_default=True,
    help="list ??????detail ?? topics ?????",
)
def crawl(group: str, pages: int, skip_pages: int, service: str) -> None:
    """?????????"""
    service = service.lower()

    console.print(f"[bold green]????[/bold green] [cyan]{group}[/cyan]")
    console.print(f"  ??: https://www.douban.com/group/{group}/")
    console.print(f"  ??: {service}")
    console.print(f"  ??: {'??' if pages == 0 else pages}")
    console.print(f"  ??: {skip_pages}")
    console.print("  ??: http")
    if service == "detail" and (pages > 0 or skip_pages > 0):
        console.print("[yellow]detail ????? --pages ? --skip-pages???? topics ???????[/yellow]")
    console.print()

    crawler = DoubanGroupCrawler(
        group_id=group,
        max_pages=pages,
        skip_pages=skip_pages,
        service=service,
    )
    stats = crawler.run()

    console.print()
    table = Table(title="????", show_header=True, header_style="bold magenta")
    table.add_column("??", style="cyan")
    table.add_column("??", justify="right", style="green")
    table.add_row("????", str(stats["pages_fetched"]))
    table.add_row("????", str(stats["pages_skipped"]))
    table.add_row("????", str(stats["topics_found"]))
    table.add_row("????", str(stats["topics_new"]))
    table.add_row("????", str(stats["details_skipped"]))
    table.add_row("????", str(stats["details_fetched"]))
    table.add_row("????", str(stats["errors"]))
    console.print(table)


@main.command()
@click.option("--group", "-g", default=DEFAULT_GROUP_ID, help="?? ID")
def stats(group: str) -> None:
    """??????????"""
    storage = Storage()

    table = Table(title=f"?????: {group}", show_header=True, header_style="bold magenta")
    table.add_column("???", style="cyan")
    table.add_column("??", justify="right", style="green")
    table.add_row("??(??)", str(storage.get_topic_count(group)))
    table.add_row("??(??)", str(storage.get_detail_count(group)))
    table.add_row("????", str(storage.get_pending_detail_count(group)))
    console.print(table)


@main.command()
@click.option("--group", "-g", default=DEFAULT_GROUP_ID, help="?? ID")
@click.option("--format", "-f", "fmt", type=click.Choice(["csv", "json"]), default="csv", help="????")
@click.option("--output", "-o", default=None, help="??????")
def export(group: str, fmt: str, output: str | None) -> None:
    """???????"""
    import csv
    import json
    import sqlite3

    from douban_crawler.config import DB_PATH

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(
        """
        SELECT t.topic_id, t.title, t.url, t.author_name, t.reply_count,
               t.last_reply_time, d.content, d.created_time,
               d.like_count, d.collect_count
        FROM topics t
        LEFT JOIN topic_details d ON t.topic_id = d.topic_id
        WHERE t.group_id = ?
        ORDER BY t.reply_count DESC
        """,
        (group,),
    )
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()

    if not rows:
        console.print("[yellow]????????[/yellow]")
        return

    output_path = output or f"data/export_{group}.{fmt}"
    if fmt == "csv":
        with open(output_path, "w", newline="", encoding="utf-8-sig") as file:
            writer = csv.DictWriter(file, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    else:
        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(rows, file, ensure_ascii=False, indent=2)

    console.print(f"[green]??? {len(rows)} ???? {output_path}[/green]")


if __name__ == "__main__":
    main()
