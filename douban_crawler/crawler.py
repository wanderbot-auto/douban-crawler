"""核心爬虫模块：负责页面抓取、分页遍历、详情爬取"""

from __future__ import annotations

import logging
from typing import Callable
from urllib.parse import urljoin

from douban_crawler.anti_crawl import RateLimiter
from douban_crawler.config import DOUBAN_BASE_URL, DEFAULT_GROUP_ID, FETCH_BACKEND, TOPICS_PER_PAGE
from douban_crawler.fetcher import PageFetcher, create_page_fetcher
from douban_crawler.models import Comment, Topic
from douban_crawler.parser import (
    has_next_comment_page,
    ListPageUnavailableError,
    parse_comments,
    parse_topic_detail,
    parse_topic_list,
    parse_total_pages,
)
from douban_crawler.storage import Storage

logger = logging.getLogger(__name__)


class DoubanGroupCrawler:
    """豆瓣小组爬虫

    功能：
    1. 爬取小组讨论列表（分页遍历）
    2. 爬取帖子详情（正文 + 评论）
    3. 增量爬取（跳过已爬取的帖子）
    """

    def __init__(
        self,
        group_id: str = DEFAULT_GROUP_ID,
        storage: Storage | None = None,
        max_pages: int = 0,
        skip_pages: int = 0,
        fetch_details: bool = True,
        fetch_comments: bool = True,
        fetch_backend: str = FETCH_BACKEND,
        fetcher_factory: Callable[[str], PageFetcher] | None = None,
    ) -> None:
        self.group_id = group_id
        self.storage = storage or Storage()
        self.max_pages = max_pages  # 0 = 全部
        self.skip_pages = skip_pages
        self.fetch_details = fetch_details
        self.fetch_comments = fetch_comments
        self.fetch_backend = fetch_backend
        self._fetcher_factory = fetcher_factory or create_page_fetcher

        self._fetcher: PageFetcher | None = None
        self._rate_limiter = RateLimiter()

        self.stats = {
            "pages_fetched": 0,
            "pages_skipped": 0,
            "topics_found": 0,
            "topics_new": 0,
            "details_fetched": 0,
            "comments_fetched": 0,
            "errors": 0,
        }

    def run(self) -> dict:
        """执行爬取任务，返回统计信息"""
        logger.info("开始爬取小组 %s 的讨论帖子", self.group_id)
        logger.info(
            "配置: backend=%s, max_pages=%s, skip_pages=%s, fetch_details=%s, fetch_comments=%s",
            self.fetch_backend,
            self.max_pages or "全部",
            self.skip_pages,
            self.fetch_details,
            self.fetch_comments,
        )

        try:
            self._fetcher = self._fetcher_factory(self.fetch_backend)

            all_topics = self._crawl_topic_list()
            if self.fetch_details and all_topics:
                self._crawl_topic_details(all_topics)

        except KeyboardInterrupt:
            logger.warning("用户中断爬取")
        except Exception as exc:
            logger.error("爬取异常: %s", exc, exc_info=True)
            self.stats["errors"] += 1
        finally:
            if self._fetcher:
                self._fetcher.close()

        self._log_stats()
        return self.stats

    def _crawl_topic_list(self) -> list[Topic]:
        """遍历讨论列表所有页"""
        all_topics: list[Topic] = []
        existing_ids = self.storage.get_topic_ids(self.group_id)
        logger.info("数据库中已有 %s 个帖子记录", len(existing_ids))

        first_page_html = self._fetch_discussion_page(start=0)
        if not first_page_html:
            logger.error("无法获取第一页，终止爬取")
            return all_topics

        try:
            total_pages = parse_total_pages(first_page_html)
        except ListPageUnavailableError as exc:
            logger.error("第一页不是可用的讨论列表页：%s", exc)
            self.stats["errors"] += 1
            return all_topics

        start_page = self.skip_pages + 1
        if start_page > total_pages:
            self.stats["pages_skipped"] = total_pages
            logger.warning(
                "跳过页数 %s 已覆盖全部 %s 页，本次无需爬取列表页",
                self.skip_pages,
                total_pages,
            )
            return all_topics

        end_page = total_pages
        if self.max_pages > 0:
            end_page = min(total_pages, self.skip_pages + self.max_pages)

        self.stats["pages_skipped"] = start_page - 1
        logger.info(
            "共 %s 页，跳过 %s 页，实际爬取第 %s 到第 %s 页",
            total_pages,
            self.stats["pages_skipped"],
            start_page,
            end_page,
        )

        if start_page == 1:
            current_page_html = first_page_html
        else:
            start = (start_page - 1) * TOPICS_PER_PAGE
            logger.info("正在爬取第 %s/%s 页 (start=%s)", start_page, total_pages, start)
            current_page_html = self._fetch_discussion_page(start=start)
            if not current_page_html:
                logger.error("无法获取起始页 %s，终止列表爬取", start_page)
                self.stats["errors"] += 1
                return all_topics

        topics = parse_topic_list(current_page_html, self.group_id)
        if not topics:
            logger.warning("第 %s 页未解析到帖子，可能已到末尾", start_page)
            return all_topics

        new_topics = self._save_new_topics(topics, existing_ids)
        all_topics.extend(new_topics)
        self.stats["pages_fetched"] += 1

        for page in range(start_page + 1, end_page + 1):
            start = (page - 1) * TOPICS_PER_PAGE
            logger.info("正在爬取第 %s/%s 页 (start=%s)", page, total_pages, start)

            html = self._fetch_discussion_page(start=start)
            if not html:
                self.stats["errors"] += 1
                continue

            topics = parse_topic_list(html, self.group_id)
            if not topics:
                logger.warning("第 %s 页未解析到帖子，可能已到末尾", page)
                break

            new_topics = self._save_new_topics(topics, existing_ids)
            all_topics.extend(new_topics)
            self.stats["pages_fetched"] += 1

        logger.info(
            "列表爬取完成: 共 %s 个帖子, 新增 %s 个",
            self.stats["topics_found"],
            self.stats["topics_new"],
        )
        return all_topics

    def _save_new_topics(self, topics: list[Topic], existing_ids: set[str]) -> list[Topic]:
        """保存帖子并返回新帖子列表"""
        self.stats["topics_found"] += len(topics)
        self.storage.save_topics(topics)

        new_topics = [t for t in topics if t.topic_id not in existing_ids]
        self.stats["topics_new"] += len(new_topics)
        for topic in topics:
            existing_ids.add(topic.topic_id)
        return new_topics

    def _crawl_topic_details(self, topics: list[Topic]) -> None:
        """爬取帖子详情和评论"""
        fetched_detail_ids = self.storage.get_fetched_detail_ids()
        pending = [t for t in topics if t.topic_id not in fetched_detail_ids]
        logger.info("待爬取详情: %s 个帖子（跳过已爬取 %s 个）", len(pending), len(topics) - len(pending))

        for i, topic in enumerate(pending, 1):
            logger.info("[%s/%s] 爬取详情: %s", i, len(pending), topic.title[:40])

            try:
                html = self._fetch_page(topic.url)
                if not html:
                    self.stats["errors"] += 1
                    continue

                detail = parse_topic_detail(html, topic.topic_id)
                if detail:
                    self.storage.save_topic_detail(detail)
                    self.stats["details_fetched"] += 1

                if self.fetch_comments:
                    comments = parse_comments(html, topic.topic_id)
                    comments.extend(self._fetch_remaining_comments(html, topic))
                    if comments:
                        self.storage.save_comments(comments)
                        self.stats["comments_fetched"] += len(comments)

            except Exception as exc:
                logger.error("爬取帖子 %s 详情失败: %s", topic.topic_id, exc)
                self.stats["errors"] += 1

    def _fetch_remaining_comments(self, first_page_html: str, topic: Topic) -> list[Comment]:
        """爬取后续页的评论"""
        comments: list[Comment] = []
        next_url = has_next_comment_page(first_page_html)

        while next_url:
            if not next_url.startswith("http"):
                next_url = urljoin(topic.url, next_url)

            logger.debug("爬取评论下一页: %s", next_url)
            html = self._fetch_page(next_url)
            if not html:
                break

            page_comments = parse_comments(html, topic.topic_id)
            if not page_comments:
                break

            comments.extend(page_comments)
            next_url = has_next_comment_page(html)

        return comments

    def _fetch_discussion_page(self, start: int = 0) -> str | None:
        """获取小组讨论列表页"""
        url = f"{DOUBAN_BASE_URL}/group/{self.group_id}/discussion"
        if start > 0:
            url += f"?start={start}"
        return self._fetch_page(url, referer=f"{DOUBAN_BASE_URL}/group/{self.group_id}/")

    def _fetch_page(self, url: str, referer: str | None = None) -> str | None:
        """通用页面获取，集成限速与抓取后端。"""
        self._rate_limiter.wait()
        fetcher = self._require_fetcher()
        if self._rate_limiter.request_count % 10 == 0:
            fetcher.rotate_identity()
        return fetcher.fetch(url, referer=referer)

    def _require_fetcher(self) -> PageFetcher:
        if not self._fetcher:
            raise RuntimeError("页面抓取器尚未初始化")
        return self._fetcher

    def _log_stats(self) -> None:
        """打印爬取统计"""
        logger.info("=" * 50)
        logger.info("爬取统计:")
        logger.info("  页面爬取: %s 页", self.stats["pages_fetched"])
        logger.info("  页面跳过: %s 页", self.stats["pages_skipped"])
        logger.info("  帖子发现: %s 个", self.stats["topics_found"])
        logger.info("  新增帖子: %s 个", self.stats["topics_new"])
        logger.info("  详情爬取: %s 个", self.stats["details_fetched"])
        logger.info("  评论爬取: %s 条", self.stats["comments_fetched"])
        logger.info("  错误次数: %s 次", self.stats["errors"])
        logger.info("=" * 50)
