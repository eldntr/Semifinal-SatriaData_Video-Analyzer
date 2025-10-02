from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import List

from app.config import Settings
from app.instagram.client import InstagramClient
from app.instagram.exceptions import (
    InstagramParsingError,
    InstagramScraperError,
    MediaDownloadError,
)
from app.instagram.parser import parse_info_payload
from app.instagram.storage import MediaStorage
from app.instagram.types import InstagramComment, ScrapedMedia
from app.instagram.url_utils import parse_instagram_url


logger = logging.getLogger(__name__)


class InstagramScraperService:
    def __init__(
        self,
        client: InstagramClient,
        storage: MediaStorage,
        settings: Settings,
    ) -> None:
        self._client = client
        self._storage = storage
        self._settings = settings

    async def scrape(
        self,
        url: str,
        *,
        download_video: bool = True,
    ) -> ScrapedMedia:
        parsed_url = parse_instagram_url(url)
        payload = await self._client.fetch_media_info(parsed_url.canonical_url)

        if self._settings.log_instagram_raw:
            formatted = json.dumps(payload, ensure_ascii=False, indent=2)
            logger.debug("Raw Instagram payload for %s:\n%s", parsed_url.shortcode, formatted)

        post, comments = parse_info_payload(
            payload,
            include_comments=self._settings.include_comments,
            max_comments=self._settings.max_comments,
        )

        if not post.video_url:
            raise InstagramParsingError("The provided URL does not contain a downloadable video")

        video_path: Path | None = None
        if download_video:
            destination = self._storage.build_video_path(post.shortcode)
            try:
                await self._client.download_media(parsed_url.canonical_url, destination)
            except InstagramScraperError as exc:
                raise MediaDownloadError("Failed to download Instagram video") from exc
            video_path = destination

        normalized_comments: List[InstagramComment] = (
            comments if self._settings.include_comments else []
        )
        return ScrapedMedia(
            post=post,
            comments=normalized_comments,
            video_path=str(video_path) if video_path else None,
        )
