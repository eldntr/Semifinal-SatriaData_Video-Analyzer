
from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import AnyHttpUrl, BaseModel, Field, HttpUrl


class InstagramScrapeRequest(BaseModel):
    url: HttpUrl = Field(..., description="Public Instagram post or reel URL")


class InstagramDownloadRequest(BaseModel):
    url: HttpUrl = Field(..., description="Public Instagram post or reel URL")


class UserProfile(BaseModel):
    username: str
    full_name: Optional[str] = None
    biography: Optional[str] = None
    posts: Optional[int] = None
    followers: Optional[int] = None
    following: Optional[int] = None
    profile_pic_url: Optional[AnyHttpUrl] = None


class Comment(BaseModel):
    id: str
    username: str
    text: str
    like_count: int = 0
    created_at: Optional[datetime] = None
    profile: Optional[UserProfile] = None


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
    audio_title: Optional[str] = None
    audio_artist: Optional[str] = None
    audio_id: Optional[str] = None
    audio_url: Optional[AnyHttpUrl] = None
    hashtags: List[str] = Field(default_factory=list)
    mentions: List[str] = Field(default_factory=list)
    owner_profile: Optional[UserProfile] = None


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


class ChapterSegment(BaseModel):
    id: Optional[int] = None
    start: float
    end: float
    text: str


class ChapterRequest(BaseModel):
    text: str
    language: Optional[str] = None
    segments: List[ChapterSegment]


class ChapterItem(BaseModel):
    start_time: str
    title: str


class ChapterResponse(BaseModel):
    chapters: List[ChapterItem] = Field(default_factory=list)


class SummaryRequest(BaseModel):
    text: str
    language: Optional[str] = None
    segments: List[ChapterSegment] = Field(default_factory=list)


class SummaryResponse(BaseModel):
    ringkasan: str
    judul: str
    topik: str


class WordCloudRequest(BaseModel):
    text: str
    language: Optional[str] = None
    segments: List[ChapterSegment] = Field(default_factory=list)
    width: Optional[int] = Field(default=800, ge=100)
    height: Optional[int] = Field(default=None, ge=100)
    background_color: Optional[str] = Field(default="white")
    colormap: Optional[str] = Field(default="viridis")
    min_font_size: Optional[int] = Field(default=10, ge=1)


class WordCloudResponse(BaseModel):
    image_path: str


class VideoVisualAnalysisResponse(BaseModel):
    analysis_id: str
    average_brightness: float
    std_dev_brightness: float
    scene_cut_timestamps: List[float] = Field(default_factory=list)
    brightness_plot_path: str
    stats_path: Optional[str] = None


class VideoAudioAnalysisResponse(BaseModel):
    analysis_id: str
    average_pitch_hz: float
    std_dev_pitch_hz: float
    spectrogram_plot_path: str
    stats_path: Optional[str] = None


class VideoFullAnalysisResponse(BaseModel):
    analysis_id: str
    visual: VideoVisualAnalysisResponse
    audio: VideoAudioAnalysisResponse
    stats_path: str
