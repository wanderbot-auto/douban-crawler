"""Crawler orchestration."""

from __future__ import annotations

import logging
from typing import Callable, Literal

from douban_crawler.anti_crawl import RateLimiter
from douban_crawler.config import DOUBAN_BASE_URL, DEFAULT_GROUP_ID, FETCH_BACKEND, TOPICS_PER_PAGE
from douban_crawler.fetcher import PageFetcher, create_page_fetcher
from douban_crawler.models import Topic
from douban_crawler.parser import (
    ListPageUnavailableError,
    parse_topic_detail,
    parse_topic_list,
    parse_total_pages,
)
from douban_crawler.storage import Storage

logger = logging.getLogger(__name__)

ServiceName = Literal["list", "detail"]


class DoubanGroupCrawler:
    """Douban group crawler."""

    def __init__(
        self,
        group_id: str = DEFAULT_GROUP_ID,
        storage: Storage | None = None,
        max_pages: int = 0,
        skip_pages: int = 0,
        service: ServiceName = "list",
        fetch_backend: str = FETCH_BACKEND,
        fetcher_factory: Callable[[str], PageFetcher] | None = None,
    ) -> None:
        self.group_id = group_id
        self.storage = storage or Storage()
        self.max_pages = max_pages
        self.skip_pages = skip_pages
        self.service = service
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
            "details_skipped": 0,
            "errors": 0,
        }

    def run(self) -> dict:
        logger.info("?????? %s", self.group_id)
        logger.info(
            "??: service=%s, backend=%s, max_pages=%s, skip_pages=%s",
            self.service,
            self.fetch_backend,
            self.max_pages or "??",
            self.skip_pages,
        )

        try:
            self._fetcher = self._fetcher_factory(self.fetch_backend)
            if self.service == "list":
                self._crawl_topic_list()
            else:
                self._crawl_topic_details()
        except KeyboardInterrupt:
            logger.warning("??????")
        except Exception as exc:
            logger.error("????: %s", exc, exc_info=True)
            self.stats["errors"] += 1
        finally:
            if self._fetcher:
                self._fetcher.close()

        self._log_stats()
        return self.stats

    def _crawl_topic_list(self) -> None:
        existing_ids = self.storage.get_topic_ids(self.group_id)
        logger.info("???????? %s ???", len(existing_ids))

        first_page_html = self._fetch_discussion_page(start=0)
        if not first_page_html:
            logger.error("??????????")
            return

        try:
            total_pages = parse_total_pages(first_page_html)
        except ListPageUnavailableError as exc:
            logger.error("??????????????: %s", exc)
            self.stats["errors"] += 1
            return

        start_page = self.skip_pages + 1
        if start_page > total_pages:
            self.stats["pages_skipped"] = total_pages
            logger.warning("???? %s ????? %s????????", self.skip_pages, total_pages)
            return

        end_page = total_pages
        if self.max_pages > 0:
            end_page = min(total_pages, self.skip_pages + self.max_pages)

        self.stats["pages_skipped"] = start_page - 1
        logger.info(
            "? %s ???? %s ?????? %s ? %s ?",
            total_pages,
            self.stats["pages_skipped"],
            start_page,
            end_page,
        )

        if start_page == 1:
            current_page_html = first_page_html
        else:
            start = (start_page - 1) * TOPICS_PER_PAGE
            logger.info("??? %s/%s ? (start=%s)", start_page, total_pages, start)
            current_page_html = self._fetch_discussion_page(start=start)
            if not current_page_html:
                logger.error("? %s ?????", start_page)
                self.stats["errors"] += 1
                return

        topics = parse_topic_list(current_page_html, self.group_id)
        if not topics:
            logger.warning("? %s ?????????", start_page)
            return

        self._save_new_topics(topics, existing_ids)
        self.stats["pages_fetched"] += 1

        for page in range(start_page + 1, end_page + 1):
            start = (page - 1) * TOPICS_PER_PAGE
            logger.info("??? %s/%s ? (start=%s)", page, total_pages, start)

            html = self._fetch_discussion_page(start=start)
            if not html:
                self.stats["errors"] += 1
                continue

            topics = parse_topic_list(html, self.group_id)
            if not topics:
                logger.warning("? %s ?????????", page)
                break

            self._save_new_topics(topics, existing_ids)
            self.stats["pages_fetched"] += 1

        logger.info("??????: ?? %s ???? %s ?", self.stats["topics_found"], self.stats["topics_new"])

    def _save_new_topics(self, topics: list[Topic], existing_ids: set[str]) -> None:
        self.stats["topics_found"] += len(topics)
        self.storage.save_topics(topics)

        new_topics = [topic for topic in topics if topic.topic_id not in existing_ids]
        self.stats["topics_new"] += len(new_topics)
        for topic in topics:
            existing_ids.add(topic.topic_id)

    def _crawl_topic_details(self) -> None:
        pending = self.storage.get_pending_detail_topics(self.group_id)
        self.stats["details_skipped"] = self.storage.get_detail_count(self.group_id)

        if not pending:
            logger.info("topics ????????????")
            return

        logger.info("???? %s ??????? %s ?", len(pending), self.stats["details_skipped"])

        for index, topic in enumerate(pending, 1):
            logger.info("[%s/%s] ????: %s", index, len(pending), topic.title[:40])

            try:
                html = self._fetch_page(topic.url, referer=f"{DOUBAN_BASE_URL}/group/{self.group_id}/")
                if not html:
                    self.stats["errors"] += 1
                    continue

                detail = parse_topic_detail(html, topic.topic_id)
                if detail:
                    if not detail.content_html:
                        logger.warning(
                            "帖子 %s content_html 为空，跳过落库（页面解析失败或被反爬拦截）",
                            topic.topic_id,
                        )
                        self.stats["errors"] += 1
                        continue
                    self.storage.save_topic_detail(detail)
                    self.stats["details_fetched"] += 1
            except Exception as exc:
                logger.error("???? %s ????: %s", topic.topic_id, exc)
                self.stats["errors"] += 1

    def _fetch_discussion_page(self, start: int = 0) -> str | None:
        url = f"{DOUBAN_BASE_URL}/group/{self.group_id}/discussion"
        if start > 0:
            url += f"?start={start}"
        return self._fetch_page(url, referer=f"{DOUBAN_BASE_URL}/group/{self.group_id}/")

    def _fetch_page(self, url: str, referer: str | None = None) -> str | None:
        self._rate_limiter.wait()
        fetcher = self._require_fetcher()
        if self._rate_limiter.request_count % 10 == 0:
            fetcher.rotate_identity()
        return fetcher.fetch(url, referer=referer)

    def _require_fetcher(self) -> PageFetcher:
        if not self._fetcher:
            raise RuntimeError("fetcher ?????")
        return self._fetcher

    def _log_stats(self) -> None:
        logger.info("=" * 50)
        logger.info("??:")
        logger.info("  ????: %s ?", self.stats["pages_fetched"])
        logger.info("  ????: %s ?", self.stats["pages_skipped"])
        logger.info("  ????: %s ?", self.stats["topics_found"])
        logger.info("  ????: %s ?", self.stats["topics_new"])
        logger.info("  ????: %s ?", self.stats["details_skipped"])
        logger.info("  ????: %s ?", self.stats["details_fetched"])
        logger.info("  ????: %s ?", self.stats["errors"])
        logger.info("=" * 50)
