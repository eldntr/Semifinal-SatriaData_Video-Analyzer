from __future__ import annotations

from pathlib import Path

from app.config import Settings


class MediaStorage:
    def __init__(self, settings: Settings) -> None:
        self._root = settings.media_directory / "instagram"
        self._root.mkdir(parents=True, exist_ok=True)

    def build_video_path(self, shortcode: str) -> Path:
        filename = f"{shortcode}.mp4"
        return self._root / filename

    def build_thumbnail_path(self, shortcode: str) -> Path:
        filename = f"{shortcode}.jpg"
        return self._root / filename
