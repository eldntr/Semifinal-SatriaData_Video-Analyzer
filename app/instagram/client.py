from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Dict, Optional

import yt_dlp

from app.config import Settings
from .exceptions import InstagramRequestError, MediaDownloadError


class InstagramClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def _build_options(
        self,
        *,
        download: bool,
        output_path: Optional[Path] = None,
        get_comments: bool = False,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "retries": self._settings.ytdlp_retries,
            "user_agent": self._settings.user_agent,
            "http_headers": {"User-Agent": self._settings.user_agent},
            "skip_download": not download,
            "format": self._settings.ytdlp_format,
            "outtmpl": str(output_path) if output_path else None,
            "overwrites": True,
            "merge_output_format": "mp4",
            "getcomments": get_comments,
        }
        if not download:
            params.pop("outtmpl", None)
        if self._settings.cookies_path:
            params["cookiefile"] = str(self._settings.cookies_path)
        return params

    async def fetch_media_info(self, url: str) -> Dict[str, Any]:
        return await asyncio.to_thread(self._extract_info, url)

    def _extract_info(self, url: str) -> Dict[str, Any]:
        params = self._build_options(download=False, get_comments=self._settings.include_comments)
        try:
            with yt_dlp.YoutubeDL(params) as ydl:
                info = ydl.extract_info(url, download=False)
        except yt_dlp.utils.DownloadError as exc:
            raise InstagramRequestError(str(exc)) from exc
        return self._unwrap_entry(info)

    async def download_media(self, url: str, destination: Path) -> None:
        await asyncio.to_thread(self._download, url, destination)

    def _download(self, url: str, destination: Path) -> None:
        params = self._build_options(download=True, output_path=destination)
        try:
            with yt_dlp.YoutubeDL(params) as ydl:
                ydl.download([url])
        except yt_dlp.utils.DownloadError as exc:
            raise MediaDownloadError(str(exc)) from exc

    @staticmethod
    def _unwrap_entry(info: Dict[str, Any]) -> Dict[str, Any]:
        if not info:
            raise InstagramRequestError("Empty response from yt-dlp")
        if info.get("_type") == "playlist" and info.get("entries"):
            return InstagramClient._unwrap_entry(info["entries"][0])
        return info
