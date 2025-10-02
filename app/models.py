from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import AnyHttpUrl, BaseModel, Field, HttpUrl


class InstagramScrapeRequest(BaseModel):
    url: HttpUrl = Field(..., description="Public Instagram post or reel URL")


class InstagramDownloadRequest(BaseModel):
    url: HttpUrl = Field(..., description="Public Instagram post or reel URL")


class Comment(BaseModel):
    id: str
    username: str
    text: str
    like_count: int = 0
    created_at: Optional[datetime] = None


class VideoMetadata(BaseModel):
    shortcode: str
    caption: Optional[str] = None
    username: str
    full_name: Optional[str] = None
    like_count: Optional[int] = None
    comment_count: Optional[int] = None
    view_count: Optional[int] = None
    taken_at: Optional[datetime] = None
    video_duration: Optional[float] = None
    video_url: Optional[HttpUrl] = None
    thumbnail_url: Optional[HttpUrl] = None
    hashtags: List[str] = Field(default_factory=list)
    mentions: List[str] = Field(default_factory=list)


class ScrapeResponse(BaseModel):
    metadata: VideoMetadata
    comments: List[Comment] = Field(default_factory=list)
    video_path: Optional[str] = Field(
        default=None,
        description="Local filesystem path of the downloaded video",
    )


class DriveDownloadRequest(BaseModel):
    url: AnyHttpUrl = Field(..., description="Google Drive sharing link or file URL")
    filename: Optional[str] = Field(
        default=None,
        description="Optional file name to use when saving to disk",
    )


class DriveFileMetadata(BaseModel):
    file_id: str
    file_name: str
    mime_type: Optional[str] = None
    size_bytes: Optional[int] = None
    local_path: str


class DriveDownloadResponse(BaseModel):
    file: DriveFileMetadata


class VideoToAudioResponse(BaseModel):
    audio_path: str
    format: str
    size_bytes: int


class TranscriptionSegment(BaseModel):
    id: Optional[int]
    start: Optional[float]
    end: Optional[float]
    text: str


class TranscriptionResponse(BaseModel):
    text: str
    language: Optional[str]
    segments: List[TranscriptionSegment] = Field(default_factory=list)
