from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Dict

from fastapi import UploadFile

from app.config import Settings
from app.media.exceptions import MediaProcessingError
from app.media.storage import ConversionStorage
from app.media.types import ConvertedAudio


class VideoAudioConverterService:
    _CHUNK_SIZE = 1024 * 1024
    _OUTPUT_FORMAT = "mp3"
    _FFMPEG_ARGS: Dict[str, str] = {
        "-acodec": "libmp3lame",
        "-ac": "2",
    }

    def __init__(self, storage: ConversionStorage, settings: Settings) -> None:
        self._storage = storage
        self._settings = settings

    async def convert(self, upload: UploadFile) -> ConvertedAudio:
        original_name = Path(upload.filename or "uploaded")
        suffix = original_name.suffix or ".mp4"
        temp_path = self._storage.build_temp_path(suffix)
        await self._save_upload(upload, temp_path)

        stem = original_name.stem or temp_path.stem
        output_path = self._storage.build_output_path(stem, self._OUTPUT_FORMAT)
        try:
            await self._run_ffmpeg(temp_path, output_path)
        finally:
            self._storage.cleanup(temp_path)

        size_bytes = output_path.stat().st_size if output_path.exists() else 0
        return ConvertedAudio(path=output_path, format=self._OUTPUT_FORMAT, size_bytes=size_bytes)

    async def _save_upload(self, upload: UploadFile, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            with destination.open("wb") as buffer:
                while True:
                    chunk = await upload.read(self._CHUNK_SIZE)
                    if not chunk:
                        break
                    buffer.write(chunk)
        finally:
            await upload.close()

    async def _run_ffmpeg(self, source: Path, target: Path) -> None:
        command = [
            "ffmpeg",
            "-y",
            "-i",
            str(source),
            "-vn",
        ]
        for key, value in self._FFMPEG_ARGS.items():
            command.extend([key, value])
        command.append(str(target))

        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            raise MediaProcessingError(
                f"ffmpeg failed with code {process.returncode}: {stderr.decode().strip()}"
            )
