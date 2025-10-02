from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class InstagramComment:
    id: str
    username: str
    text: str
    like_count: int = 0
    created_at: Optional[datetime] = None


@dataclass
class InstagramPost:
    shortcode: str
    caption: Optional[str]
    username: str
    full_name: Optional[str]
    like_count: Optional[int]
    comment_count: Optional[int]
    view_count: Optional[int]
    taken_at: Optional[datetime]
    video_duration: Optional[float]
    video_url: Optional[str]
    thumbnail_url: Optional[str]
    hashtags: List[str] = field(default_factory=list)
    mentions: List[str] = field(default_factory=list)


@dataclass
class ScrapedMedia:
    post: InstagramPost
    comments: List[InstagramComment]
    video_path: Optional[str]
