"""HTML 解析模块：从豆瓣页面提取结构化数据"""

import re
import logging
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from douban_crawler.config import DOUBAN_BASE_URL
from douban_crawler.models import Topic, TopicDetail

logger = logging.getLogger(__name__)


class ListPageUnavailableError(RuntimeError):
    """讨论列表首页不可用，无法继续分页。"""


def parse_topic_list(html: str, group_id: str) -> list[Topic]:
    """解析小组讨论列表页，返回帖子列表

    豆瓣小组讨论列表页结构：
    <table class="olt">
      <tr class=""> (每行一个帖子)
        <td class="title"> <a href="...">标题</a> </td>
        <td class=""> <a href="...">作者</a> </td>
        <td class="r-count">回复数</td>
        <td class="time">最后回复时间</td>
      </tr>
    </table>
    """
    soup = BeautifulSoup(html, "lxml")
    topics: list[Topic] = []
    now_str = datetime.now().isoformat()

    table = soup.find("table", class_="olt")
    if not table:
        logger.warning("未找到讨论列表表格 (.olt)，可能页面结构变化或被反爬拦截")
        # 尝试检测是否被拦截
        _check_blocked(soup)
        return topics

    rows = table.find_all("tr")
    for row in rows:
        if not isinstance(row, Tag):
            continue
        # 跳过表头
        th = row.find("th")
        if th:
            continue

        try:
            topic = _parse_topic_row(row, group_id, now_str)
            if topic:
                topics.append(topic)
        except Exception as exc:
            logger.debug(f"解析行失败: {exc}")
            continue

    logger.info(f"从列表页解析到 {len(topics)} 个帖子")
    return topics


def _parse_topic_row(row: Tag, group_id: str, now_str: str) -> Topic | None:
    """解析单行帖子信息"""
    cells = row.find_all("td")
    if len(cells) < 4:
        return None

    # 标题和链接
    title_cell = cells[0]
    title_link = title_cell.find("a")
    if not title_link:
        return None

    title = title_link.get("title", "") or title_link.get_text(strip=True)
    url = title_link.get("href", "")
    if url and not url.startswith("http"):
        url = urljoin(DOUBAN_BASE_URL, url)

    # 从 URL 提取 topic_id
    topic_id = _extract_topic_id(url)
    if not topic_id:
        return None

    # 作者
    author_cell = cells[1]
    author_link = author_cell.find("a")
    author_name = author_link.get_text(strip=True) if author_link else ""
    author_url = author_link.get("href", "") if author_link else ""

    # 回复数
    reply_text = cells[2].get_text(strip=True)
    reply_count = int(reply_text) if reply_text.isdigit() else 0

    # 最后回复时间
    last_reply_time = cells[3].get_text(strip=True)

    return Topic(
        topic_id=topic_id,
        title=title,
        url=url,
        author_name=author_name,
        author_url=author_url,
        reply_count=reply_count,
        last_reply_time=last_reply_time,
        group_id=group_id,
        created_at=now_str,
    )


def parse_topic_detail(html: str, topic_id: str) -> TopicDetail | None:
    """解析帖子详情页

    豆瓣帖子详情结构：
    - 标题: h1 或 #content h1
    - 正文: .topic-richtext 或 .topic-content
    - 作者: .topic-doc .from a
    - 发布时间: .topic-doc .color-green
    - 图片: .topic-richtext img
    """
    soup = BeautifulSoup(html, "lxml")
    now_str = datetime.now().isoformat()

    _check_blocked(soup)

    detail = TopicDetail(topic_id=topic_id, fetched_at=now_str)

    # 标题
    title_el = soup.select_one("#content h1") or soup.find("h1")
    if title_el:
        detail.title = title_el.get_text(strip=True)

    # 正文
    content_el = soup.select_one(".topic-richtext") or soup.select_one(".topic-content")
    if content_el:
        detail.content_html = str(content_el)
        detail.content = content_el.get_text("\n", strip=True)
        # 提取图片
        detail.images = [
            img.get("src", "") for img in content_el.find_all("img") if img.get("src")
        ]

    # 作者信息
    from_el = soup.select_one(".topic-doc .from a")
    if from_el:
        detail.author_name = from_el.get_text(strip=True)
        detail.author_url = from_el.get("href", "")

    # 发布时间
    time_el = soup.select_one(".topic-doc .color-green") or soup.select_one(
        ".topic-doc .create-time"
    )
    if time_el:
        detail.created_time = time_el.get_text(strip=True)

    # 互动数据（赞/收藏/转发）
    for btn in soup.select(".topic-opt .sns-bar span, .topic-opt button"):
        text = btn.get_text(strip=True)
        num = _extract_number(text)
        if "赞" in text or "like" in text.lower():
            detail.like_count = num
        elif "收藏" in text or "collect" in text.lower():
            detail.collect_count = num
        elif "转发" in text or "reshare" in text.lower():
            detail.reshare_count = num

    return detail


def parse_total_pages(html: str) -> int:
    """从讨论列表页解析总页数

    分页结构: <div class="paginator"> ... <span class="thispage" data-total-page="N"> ... </div>
    """
    soup = BeautifulSoup(html, "lxml")
    issue = _detect_list_page_issue(soup)
    if issue:
        raise ListPageUnavailableError(issue)

    # 方式1: data-total-page 属性
    this_page = soup.select_one(".paginator .thispage")
    if this_page and this_page.get("data-total-page"):
        try:
            return int(this_page["data-total-page"])
        except (ValueError, TypeError):
            pass

    # 方式2: 找最后一个页码链接
    paginator = soup.select_one(".paginator")
    if paginator:
        links = paginator.find_all("a")
        max_page = 1
        for link in links:
            text = link.get_text(strip=True)
            if text.isdigit():
                max_page = max(max_page, int(text))
        if max_page > 1:
            return max_page

    return 1


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------


def _extract_topic_id(url: str) -> str:
    """从帖子 URL 中提取 topic_id"""
    match = re.search(r"/topic/(\d+)", url)
    return match.group(1) if match else ""


def _extract_number(text: str) -> int:
    """从文本中提取第一个数字"""
    match = re.search(r"(\d+)", text)
    return int(match.group(1)) if match else 0


def _check_blocked(soup: BeautifulSoup) -> None:
    """检测是否被反爬拦截"""
    title = soup.title.get_text(strip=True) if soup.title else ""

    if "检测到有异常请求" in title or "异常请求" in str(soup):
        logger.error("🚫 被豆瓣反爬机制拦截！建议：增大请求间隔 / 更换 IP / 设置登录 Cookie")
    elif "登录" in title and "豆瓣" in title:
        logger.warning("⚠️ 当前命中登录跳转页；优先连接已登录的 Chrome，必要时再补 DOUBAN_COOKIE")


def _detect_list_page_issue(soup: BeautifulSoup) -> str | None:
    title = soup.title.get_text(strip=True) if soup.title else ""
    page_text = str(soup)

    if "检测到有异常请求" in title or "异常请求" in page_text:
        return "当前请求命中豆瓣异常请求/反爬页面，无法判断总页数"
    if "登录" in title and "豆瓣" in title:
        return "当前命中豆瓣登录跳转页，无法判断总页数；请优先连接已登录的 Chrome，或补充有效 DOUBAN_COOKIE"
    if not soup.find("table", class_="olt"):
        return "首页未找到讨论列表表格 (.olt)，无法判断总页数；可能被反爬拦截或页面结构已变化"
    return None
