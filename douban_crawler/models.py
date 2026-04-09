"""数据模型定义"""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Topic:
    """小组讨论帖子（列表页信息）"""

    topic_id: str              # 帖子 ID
    title: str                 # 标题
    url: str                   # 帖子链接
    author_name: str           # 作者昵称
    author_url: str = ""       # 作者主页链接
    reply_count: int = 0       # 回复数
    last_reply_time: str = ""  # 最后回复时间
    group_id: str = ""         # 所属小组 ID
    created_at: str = ""       # 爬取时间


@dataclass
class TopicDetail:
    """帖子详情"""

    topic_id: str
    title: str = ""
    content: str = ""          # 帖子正文（纯文本）
    content_html: str = ""     # 帖子正文（HTML）
    author_name: str = ""
    author_url: str = ""
    created_time: str = ""     # 帖子发布时间
    like_count: int = 0        # 点赞数
    collect_count: int = 0     # 收藏数
    reshare_count: int = 0     # 转发数
    images: list[str] = field(default_factory=list)  # 帖子中的图片链接
    fetched_at: str = ""       # 爬取时间


@dataclass
class Comment:
    """帖子评论"""

    comment_id: str
    topic_id: str              # 所属帖子 ID
    author_name: str = ""
    author_url: str = ""
    content: str = ""          # 评论内容
    created_time: str = ""     # 评论时间
    vote_count: int = 0        # 赞数
    reply_to: str = ""         # 回复的评论 ID（如有）
    fetched_at: str = ""
