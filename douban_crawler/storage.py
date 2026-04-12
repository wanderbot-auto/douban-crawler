"""SQLite storage layer."""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from douban_crawler.config import DB_PATH
from douban_crawler.models import Topic, TopicDetail

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS topics (
    topic_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    author_name TEXT,
    author_url TEXT,
    reply_count INTEGER DEFAULT 0,
    last_reply_time TEXT,
    group_id TEXT,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS topic_details (
    topic_id TEXT PRIMARY KEY,
    title TEXT,
    content TEXT,
    content_html TEXT,
    author_name TEXT,
    author_url TEXT,
    created_time TEXT,
    like_count INTEGER DEFAULT 0,
    collect_count INTEGER DEFAULT 0,
    reshare_count INTEGER DEFAULT 0,
    images TEXT,
    fetched_at TEXT,
    FOREIGN KEY (topic_id) REFERENCES topics(topic_id)
);

CREATE INDEX IF NOT EXISTS idx_topics_group ON topics(group_id);
"""


class Storage:
    """SQLite-backed persistence."""

    def __init__(self, db_path: Path | str | None = None) -> None:
        self.db_path = str(db_path or DB_PATH)
        self._init_db()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(_SCHEMA)
            self._migrate(conn)
        logger.info("???????: %s", self.db_path)

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        conn.execute("DROP TABLE IF EXISTS comments")
        conn.execute("DROP TABLE IF EXISTS discussion_page_snapshots")
        conn.execute("DROP INDEX IF EXISTS idx_comments_topic")

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

    def save_topics(self, topics: list[Topic]) -> int:
        if not topics:
            return 0

        sql = """
            INSERT OR REPLACE INTO topics
            (topic_id, title, url, author_name, author_url, reply_count, last_reply_time, group_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        rows = [
            (
                topic.topic_id,
                topic.title,
                topic.url,
                topic.author_name,
                topic.author_url,
                topic.reply_count,
                topic.last_reply_time,
                topic.group_id,
                topic.created_at,
            )
            for topic in topics
        ]
        with self._connect() as conn:
            conn.executemany(sql, rows)
        logger.info("??? %s ???????", len(rows))
        return len(rows)

    def save_topic_detail(self, detail: TopicDetail) -> None:
        sql = """
            INSERT OR REPLACE INTO topic_details
            (topic_id, title, content, content_html, author_name, author_url,
             created_time, like_count, collect_count, reshare_count, images, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        with self._connect() as conn:
            conn.execute(
                sql,
                (
                    detail.topic_id,
                    detail.title,
                    detail.content,
                    detail.content_html,
                    detail.author_name,
                    detail.author_url,
                    detail.created_time,
                    detail.like_count,
                    detail.collect_count,
                    detail.reshare_count,
                    json.dumps(detail.images, ensure_ascii=False),
                    detail.fetched_at,
                ),
            )

    def get_topic_ids(self, group_id: str | None = None) -> set[str]:
        with self._connect() as conn:
            if group_id:
                cursor = conn.execute("SELECT topic_id FROM topics WHERE group_id = ?", (group_id,))
            else:
                cursor = conn.execute("SELECT topic_id FROM topics")
            return {row[0] for row in cursor.fetchall()}

    def get_pending_detail_topics(self, group_id: str, limit: int | None = None) -> list[Topic]:
        sql = """
            SELECT t.topic_id, t.title, t.url, t.author_name, t.author_url,
                   t.reply_count, t.last_reply_time, t.group_id, t.created_at
            FROM topics t
            LEFT JOIN topic_details d ON d.topic_id = t.topic_id
            WHERE t.group_id = ?
              AND d.topic_id IS NULL
              AND t.url != ''
            ORDER BY COALESCE(t.created_at, ''), t.topic_id
        """
        params: list[object] = [group_id]
        if limit and limit > 0:
            sql += " LIMIT ?"
            params.append(limit)

        with self._connect() as conn:
            cursor = conn.execute(sql, tuple(params))
            return [
                Topic(
                    topic_id=row[0],
                    title=row[1],
                    url=row[2],
                    author_name=row[3] or "",
                    author_url=row[4] or "",
                    reply_count=row[5] or 0,
                    last_reply_time=row[6] or "",
                    group_id=row[7] or "",
                    created_at=row[8] or "",
                )
                for row in cursor.fetchall()
            ]

    def get_pending_detail_count(self, group_id: str) -> int:
        sql = """
            SELECT COUNT(*)
            FROM topics t
            LEFT JOIN topic_details d ON d.topic_id = t.topic_id
            WHERE t.group_id = ?
              AND d.topic_id IS NULL
              AND t.url != ''
        """
        with self._connect() as conn:
            cursor = conn.execute(sql, (group_id,))
            return cursor.fetchone()[0]

    def get_fetched_detail_ids(self, group_id: str | None = None) -> set[str]:
        with self._connect() as conn:
            if group_id:
                cursor = conn.execute(
                    """
                    SELECT d.topic_id
                    FROM topic_details d
                    INNER JOIN topics t ON t.topic_id = d.topic_id
                    WHERE t.group_id = ?
                    """,
                    (group_id,),
                )
            else:
                cursor = conn.execute("SELECT topic_id FROM topic_details")
            return {row[0] for row in cursor.fetchall()}

    def get_topic_count(self, group_id: str | None = None) -> int:
        with self._connect() as conn:
            if group_id:
                cursor = conn.execute("SELECT COUNT(*) FROM topics WHERE group_id = ?", (group_id,))
            else:
                cursor = conn.execute("SELECT COUNT(*) FROM topics")
            return cursor.fetchone()[0]

    def get_detail_count(self, group_id: str | None = None) -> int:
        with self._connect() as conn:
            if group_id:
                cursor = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM topic_details d
                    INNER JOIN topics t ON t.topic_id = d.topic_id
                    WHERE t.group_id = ?
                    """,
                    (group_id,),
                )
            else:
                cursor = conn.execute("SELECT COUNT(*) FROM topic_details")
            return cursor.fetchone()[0]
