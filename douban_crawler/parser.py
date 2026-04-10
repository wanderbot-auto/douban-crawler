"""HTML 解析模块：从豆瓣页面提取结构化数据"""

import re
import logging
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from douban_crawler.config import DOUBAN_BASE_URL
from douban_crawler.models import Topic, TopicDetail, Comment

logger = logging.getLogger(__name__)


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


def parse_comments(html: str, topic_id: str) -> list[Comment]:
    """解析帖子页面中的评论

    评论结构:
    <ul id="comments">
      <li class="comment-item" data-cid="...">
        <div class="reply-doc">
          <div class="bg-img-green"> <a href="...">用户名</a> </div>
          <span class="pubtime">时间</span>
          <p>评论内容</p>
        </div>
      </li>
    </ul>
    """
    soup = BeautifulSoup(html, "lxml")
    comments: list[Comment] = []
    now_str = datetime.now().isoformat()

    comment_items = soup.select("#comments .comment-item")
    for item in comment_items:
        try:
            comment = _parse_comment_item(item, topic_id, now_str)
            if comment:
                comments.append(comment)
        except Exception as exc:
            logger.debug(f"解析评论失败: {exc}")
            continue

    logger.debug(f"从帖子 {topic_id} 解析到 {len(comments)} 条评论")
    return comments


def _parse_comment_item(item: Tag, topic_id: str, now_str: str) -> Comment | None:
    """解析单条评论"""
    comment_id = item.get("data-cid", "") or item.get("id", "")
    if not comment_id:
        return None

    comment = Comment(
        comment_id=str(comment_id),
        topic_id=topic_id,
        fetched_at=now_str,
    )

    # 作者
    author_link = item.select_one(".reply-doc a") or item.select_one("a")
    if author_link:
        comment.author_name = author_link.get_text(strip=True)
        comment.author_url = author_link.get("href", "")

    # 评论时间
    time_el = item.select_one(".pubtime") or item.select_one(".reply-doc span")
    if time_el:
        comment.created_time = time_el.get_text(strip=True)

    # 评论内容
    content_el = item.select_one(".reply-doc p") or item.select_one("p")
    if content_el:
        comment.content = content_el.get_text(strip=True)

    # 赞数
    vote_el = item.select_one(".comment-vote span") or item.select_one(".vote-count")
    if vote_el:
        comment.vote_count = _extract_number(vote_el.get_text(strip=True))

    # 回复引用
    reply_quote = item.select_one(".reply-quote")
    if reply_quote:
        ref_link = reply_quote.find("a")
        if ref_link:
            comment.reply_to = ref_link.get("data-cid", "")

    return comment


def parse_total_pages(html: str) -> int:
    """从讨论列表页解析总页数

    分页结构: <div class="paginator"> ... <span class="thispage" data-total-page="N"> ... </div>
    """
    soup = BeautifulSoup(html, "lxml")

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


def has_next_comment_page(html: str) -> str | None:
    """检查是否有下一页评论，返回下一页 URL 或 None

    评论分页: <div id="comments" ...> ... <span class="next"> <a href="?start=100">后页></a> </span>
    """
    soup = BeautifulSoup(html, "lxml")
    next_link = soup.select_one("#comments .paginator .next a") or soup.select_one(
        ".paginator .next a"
    )
    if next_link and next_link.get("href"):
        return next_link["href"]
    return None


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
