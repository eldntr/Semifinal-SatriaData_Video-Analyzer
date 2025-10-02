from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

from app.config import Settings
from app.google_drive.client import GoogleDriveClient
from app.google_drive.exceptions import (
    GoogleDriveDownloadError,
    GoogleDriveError,
    InvalidGoogleDriveUrlError,
)
from app.google_drive.storage import GoogleDriveStorage
from app.google_drive.types import GoogleDriveFile
from app.google_drive.url_utils import ParsedGoogleDriveUrl, parse_google_drive_url


class GoogleDriveDownloaderService:
    def __init__(
        self,
        client: GoogleDriveClient,
        storage: GoogleDriveStorage,
        settings: Settings,
    ) -> None:
        self._client = client
        self._storage = storage
        self._settings = settings

    async def download(self, url: str, *, filename: Optional[str] = None) -> GoogleDriveFile:
        return await asyncio.to_thread(self._download_sync, url, filename)

    def _download_sync(self, url: str, filename: Optional[str]) -> GoogleDriveFile:
        parsed = self._parse_url(url)
        destination_dir = self._storage.root
        try:
            downloaded = self._client.download_file(
                file_id=parsed.file_id,
                destination_dir=destination_dir,
                preferred_name=filename,
            )
        except GoogleDriveError as exc:
            raise GoogleDriveDownloadError(str(exc)) from exc

        managed_path = self._storage.ensure_within_root(downloaded.local_path)
        return GoogleDriveFile(
            file_id=downloaded.file_id,
            file_name=downloaded.file_name,
            mime_type=downloaded.mime_type,
            size_bytes=downloaded.size_bytes,
            local_path=managed_path,
        )

    @staticmethod
    def _parse_url(url: str) -> ParsedGoogleDriveUrl:
        try:
            return parse_google_drive_url(url)
        except InvalidGoogleDriveUrlError:
            raise
        except Exception as exc:
            raise InvalidGoogleDriveUrlError("Failed to parse Google Drive URL") from exc
