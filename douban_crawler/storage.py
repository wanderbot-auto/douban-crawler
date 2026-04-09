"""SQLite 数据存储模块"""

import sqlite3
import logging
from contextlib import contextmanager
from pathlib import Path

from douban_crawler.config import DB_PATH
from douban_crawler.models import Topic, TopicDetail, Comment

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 数据库初始化
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS topics (
    topic_id    TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    url         TEXT NOT NULL,
    author_name TEXT,
    author_url  TEXT,
    reply_count INTEGER DEFAULT 0,
    last_reply_time TEXT,
    group_id    TEXT,
    created_at  TEXT
);

CREATE TABLE IF NOT EXISTS topic_details (
    topic_id      TEXT PRIMARY KEY,
    title         TEXT,
    content       TEXT,
    content_html  TEXT,
    author_name   TEXT,
    author_url    TEXT,
    created_time  TEXT,
    like_count    INTEGER DEFAULT 0,
    collect_count INTEGER DEFAULT 0,
    reshare_count INTEGER DEFAULT 0,
    images        TEXT,   -- JSON array
    fetched_at    TEXT,
    FOREIGN KEY (topic_id) REFERENCES topics(topic_id)
);

CREATE TABLE IF NOT EXISTS comments (
    comment_id  TEXT,
    topic_id    TEXT,
    author_name TEXT,
    author_url  TEXT,
    content     TEXT,
    created_time TEXT,
    vote_count  INTEGER DEFAULT 0,
    reply_to    TEXT,
    fetched_at  TEXT,
    PRIMARY KEY (comment_id, topic_id),
    FOREIGN KEY (topic_id) REFERENCES topics(topic_id)
);

CREATE INDEX IF NOT EXISTS idx_topics_group ON topics(group_id);
CREATE INDEX IF NOT EXISTS idx_comments_topic ON comments(topic_id);
"""


class Storage:
    """SQLite 存储管理"""

    def __init__(self, db_path: Path | str | None = None) -> None:
        self.db_path = str(db_path or DB_PATH)
        self._init_db()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(_SCHEMA)
        logger.info(f"数据库已就绪: {self.db_path}")

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------------

    def save_topics(self, topics: list[Topic]) -> int:
        """批量保存帖子列表信息，返回新增数量"""
        if not topics:
            return 0
        sql = """
            INSERT OR REPLACE INTO topics
            (topic_id, title, url, author_name, author_url, reply_count, last_reply_time, group_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        rows = [
            (t.topic_id, t.title, t.url, t.author_name, t.author_url,
             t.reply_count, t.last_reply_time, t.group_id, t.created_at)
            for t in topics
        ]
        with self._connect() as conn:
            conn.executemany(sql, rows)
        logger.info(f"保存 {len(rows)} 个帖子到数据库")
        return len(rows)

    def save_topic_detail(self, detail: TopicDetail) -> None:
        """保存帖子详情"""
        import json
        sql = """
            INSERT OR REPLACE INTO topic_details
            (topic_id, title, content, content_html, author_name, author_url,
             created_time, like_count, collect_count, reshare_count, images, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        with self._connect() as conn:
            conn.execute(sql, (
                detail.topic_id, detail.title, detail.content, detail.content_html,
                detail.author_name, detail.author_url, detail.created_time,
                detail.like_count, detail.collect_count, detail.reshare_count,
                json.dumps(detail.images, ensure_ascii=False), detail.fetched_at,
            ))

    def save_comments(self, comments: list[Comment]) -> int:
        """批量保存评论，返回保存数量"""
        if not comments:
            return 0
        sql = """
            INSERT OR REPLACE INTO comments
            (comment_id, topic_id, author_name, author_url, content, created_time,
             vote_count, reply_to, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        rows = [
            (c.comment_id, c.topic_id, c.author_name, c.author_url, c.content,
             c.created_time, c.vote_count, c.reply_to, c.fetched_at)
            for c in comments
        ]
        with self._connect() as conn:
            conn.executemany(sql, rows)
        return len(rows)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def get_topic_ids(self, group_id: str | None = None) -> set[str]:
        """获取已有帖子 ID 集合"""
        with self._connect() as conn:
            if group_id:
                cursor = conn.execute(
                    "SELECT topic_id FROM topics WHERE group_id = ?", (group_id,)
                )
            else:
                cursor = conn.execute("SELECT topic_id FROM topics")
            return {row[0] for row in cursor.fetchall()}

    def get_fetched_detail_ids(self) -> set[str]:
        """获取已爬取详情的帖子 ID 集合"""
        with self._connect() as conn:
            cursor = conn.execute("SELECT topic_id FROM topic_details")
            return {row[0] for row in cursor.fetchall()}

    def get_topic_count(self, group_id: str | None = None) -> int:
        with self._connect() as conn:
            if group_id:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM topics WHERE group_id = ?", (group_id,)
                )
            else:
                cursor = conn.execute("SELECT COUNT(*) FROM topics")
            return cursor.fetchone()[0]

    def get_detail_count(self) -> int:
        with self._connect() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM topic_details")
            return cursor.fetchone()[0]

    def get_comment_count(self, topic_id: str | None = None) -> int:
        with self._connect() as conn:
            if topic_id:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM comments WHERE topic_id = ?", (topic_id,)
                )
            else:
                cursor = conn.execute("SELECT COUNT(*) FROM comments")
            return cursor.fetchone()[0]
