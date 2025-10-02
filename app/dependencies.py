from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from app.config import Settings, get_settings
from app.google_drive.client import GoogleDriveClient
from app.google_drive.storage import GoogleDriveStorage
from app.media.storage import ConversionStorage
from app.services.google_drive_downloader import GoogleDriveDownloaderService
from app.services.instagram_scraper import InstagramScraperService
from app.services.media_converter import VideoAudioConverterService
from app.instagram.client import InstagramClient
from app.instagram.storage import MediaStorage
from app.transcription.storage import TranscriptionStorage
from app.transcription.service import WhisperTranscriberService


@lru_cache(maxsize=1)
def get_instagram_service() -> InstagramScraperService:
    settings = get_settings()
    client = InstagramClient(settings)
    storage = MediaStorage(settings)
    return InstagramScraperService(client=client, storage=storage, settings=settings)


@lru_cache(maxsize=1)
def get_google_drive_service() -> GoogleDriveDownloaderService:
    settings = get_settings()
    client = GoogleDriveClient(settings)
    root = settings.media_directory / "google-drive"
    storage = GoogleDriveStorage(root)
    return GoogleDriveDownloaderService(client=client, storage=storage, settings=settings)


@lru_cache(maxsize=1)
def get_media_converter_service() -> VideoAudioConverterService:
    settings = get_settings()
    storage = ConversionStorage(settings.media_directory / "conversions")
    return VideoAudioConverterService(storage=storage, settings=settings)


@lru_cache(maxsize=1)
def get_transcription_service() -> WhisperTranscriberService:
    settings = get_settings()
    storage = TranscriptionStorage(settings.media_directory / "transcription")
    return WhisperTranscriberService(storage=storage, settings=settings)


def get_settings_dependency() -> Settings:
    return get_settings()
