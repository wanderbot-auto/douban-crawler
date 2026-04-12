"""??????"""

from dataclasses import dataclass, field


@dataclass
class Topic:
    """?????????????"""

    topic_id: str
    title: str
    url: str
    author_name: str
    author_url: str = ""
    reply_count: int = 0
    last_reply_time: str = ""
    group_id: str = ""
    created_at: str = ""


@dataclass
class TopicDetail:
    """????"""

    topic_id: str
    title: str = ""
    content: str = ""
    content_html: str = ""
    author_name: str = ""
    author_url: str = ""
    created_time: str = ""
    like_count: int = 0
    collect_count: int = 0
    reshare_count: int = 0
    images: list[str] = field(default_factory=list)
    fetched_at: str = ""
