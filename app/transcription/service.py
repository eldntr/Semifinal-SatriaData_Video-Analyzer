from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import Any, Dict, Optional

import torch
import whisper
from fastapi import UploadFile

from app.config import Settings
from app.transcription.exceptions import (
    TranscriptionModelError,
    TranscriptionProcessingError,
)
from app.transcription.storage import TranscriptionStorage


class WhisperTranscriberService:
    def __init__(self, storage: TranscriptionStorage, settings: Settings) -> None:
        self._storage = storage
        self._settings = settings
        self._model_lock = threading.Lock()
        self._model: Optional[whisper.Whisper] = None

    async def transcribe(
        self,
        file: UploadFile,
        *,
        language: Optional[str] = None,
    ) -> Dict[str, Any]:
        suffix = Path(file.filename or "audio").suffix or ".wav"
        temp_path = self._storage.build_temp_path(suffix)
        await self._save_upload(file, temp_path)

        try:
            return await asyncio.to_thread(self._run_transcription, temp_path, language)
        finally:
            self._storage.cleanup(temp_path)

    def _run_transcription(self, audio_path: Path, language: Optional[str]) -> Dict[str, Any]:
        model = self._load_model()
        device = self._determine_device()
        fp16 = device == "cuda"
        kwargs: Dict[str, Any] = {"fp16": fp16}
        if language:
            kwargs["language"] = language
        elif self._settings.whisper_language:
            kwargs["language"] = self._settings.whisper_language

        try:
            result = model.transcribe(str(audio_path), **kwargs)
        except Exception as exc:
            raise TranscriptionProcessingError(str(exc)) from exc

        segments = []
        for segment in result.get("segments", []):
            text = (segment.get("text") or "").strip()
            segments.append(
                {
                    "id": segment.get("id"),
                    "start": segment.get("start"),
                    "end": segment.get("end"),
                    "text": text,
                }
            )

        return {
            "text": (result.get("text") or "").strip(),
            "segments": segments,
            "language": result.get("language") or kwargs.get("language"),
        }

    def _load_model(self) -> whisper.Whisper:
        with self._model_lock:
            if self._model is None:
                device = self._determine_device()
                try:
                    self._model = whisper.load_model(self._settings.whisper_model, device=device)
                except Exception as exc:
                    raise TranscriptionModelError(
                        f"Failed to load Whisper model '{self._settings.whisper_model}': {exc}"
                    ) from exc
            return self._model

    def _determine_device(self) -> str:
        device = self._settings.whisper_device
        if device == "auto":
            device = "cuda" if torch.cuda.is_available() else "cpu"
        return device

    async def _save_upload(self, upload: UploadFile, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            with destination.open("wb") as buffer:
                while True:
                    chunk = await upload.read(1024 * 1024)
                    if not chunk:
                        break
                    buffer.write(chunk)
        finally:
            await upload.close()
