from __future__ import annotations

from pathlib import Path
from typing import Optional

import gdown

from app.config import Settings
from .exceptions import GoogleDriveDownloadError
from .types import GoogleDriveFile


class GoogleDriveClient:
    def __init__(self, settings: Settings) -> None:
        self._quiet = True
        self._settings = settings

    def download_file(
        self,
        *,
        file_id: str,
        destination_dir: Path,
        preferred_name: Optional[str] = None,
    ) -> GoogleDriveFile:
        destination_dir.mkdir(parents=True, exist_ok=True)
        output_arg: Optional[str] = None
        if preferred_name:
            output_arg = str(destination_dir / preferred_name)
        else:
            output_arg = str(destination_dir)

        try:
            downloaded = gdown.download(
                id=file_id,
                output=output_arg,
                quiet=self._quiet,
                fuzzy=True,
            )
        except Exception as exc:
            raise GoogleDriveDownloadError(f"Failed to download Google Drive file {file_id}") from exc

        if not downloaded:
            raise GoogleDriveDownloadError(f"gdown returned no output for file {file_id}")

        downloaded_path = Path(downloaded).resolve()
        if not downloaded_path.exists():
            raise GoogleDriveDownloadError(
                f"Downloaded file path '{downloaded_path}' does not exist"
            )

        if downloaded_path.is_dir():
            raise GoogleDriveDownloadError("Expected a file but gdown returned a directory")

        file_name = downloaded_path.name
        size_bytes = downloaded_path.stat().st_size

        return GoogleDriveFile(
            file_id=file_id,
            file_name=file_name,
            mime_type=None,
            size_bytes=size_bytes,
            local_path=downloaded_path,
        )
