
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status

from app.dependencies import (
    get_google_drive_service,
    get_instagram_service,
    get_media_converter_service,
    get_transcription_service,
)
from app.google_drive.exceptions import (
    GoogleDriveDownloadError,
    InvalidGoogleDriveUrlError,
)
from app.instagram.exceptions import (
    InstagramParsingError,
    InstagramRequestError,
    InvalidInstagramUrlError,
    MediaDownloadError,
)
from app.instagram.types import InstagramComment, InstagramPost, InstagramProfile
from app.media.exceptions import (
    MediaConversionError,
    MediaProcessingError,
)
from app.models import (
    Comment,
    UserProfile,
    DriveDownloadRequest,
    DriveDownloadResponse,
    DriveFileMetadata,
    InstagramDownloadRequest,
    InstagramScrapeRequest,
    ScrapeResponse,
    TranscriptionResponse,
    TranscriptionSegment,
    VideoMetadata,
    VideoToAudioResponse,
)
from app.services.google_drive_downloader import GoogleDriveDownloaderService
from app.services.instagram_scraper import InstagramScraperService
from app.services.media_converter import VideoAudioConverterService
from app.transcription.exceptions import (
    TranscriptionError,
    TranscriptionModelError,
    TranscriptionProcessingError,
)
from app.transcription.service import WhisperTranscriberService

router = APIRouter()
instagram_router = APIRouter(prefix="/instagram", tags=["instagram"])
gdrive_router = APIRouter(prefix="/google-drive", tags=["google-drive"])
media_router = APIRouter(prefix="/media", tags=["media"])


def _to_profile(profile: InstagramProfile | None) -> UserProfile | None:
    if profile is None:
        return None
    return UserProfile(
        username=profile.username,
        full_name=profile.full_name,
        biography=profile.biography,
        posts=profile.posts,
        followers=profile.followers,
        following=profile.following,
        profile_pic_url=profile.profile_pic_url,
    )


def _to_comment(model: InstagramComment) -> Comment:
    return Comment(
        id=model.id,
        username=model.username,
        text=model.text,
        like_count=model.like_count,
        created_at=model.created_at,
        profile=_to_profile(model.profile),
    )


def _to_metadata(model: InstagramPost) -> VideoMetadata:
    return VideoMetadata(
        shortcode=model.shortcode,
        caption=model.caption,
        username=model.username,
        full_name=model.full_name,
        like_count=model.like_count,
        comment_count=model.comment_count,
        view_count=model.view_count,
        taken_at=model.taken_at,
        video_duration=model.video_duration,
        video_url=model.video_url,
        thumbnail_url=model.thumbnail_url,
        audio_title=model.audio_title,
        audio_artist=model.audio_artist,
        audio_id=model.audio_id,
        audio_url=model.audio_url,
        hashtags=model.hashtags,
        mentions=model.mentions,
        owner_profile=_to_profile(model.owner_profile),
    )

@instagram_router.post("/scrape", response_model=ScrapeResponse)
async def scrape_instagram_video(
    request: InstagramScrapeRequest,
    service: InstagramScraperService = Depends(get_instagram_service),
) -> ScrapeResponse:
    try:
        result = await service.scrape(
            str(request.url),
            download_video=False,
        )
    except InvalidInstagramUrlError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except InstagramRequestError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    except InstagramParsingError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    except MediaDownloadError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    response = ScrapeResponse(
        metadata=_to_metadata(result.post),
        comments=[_to_comment(comment) for comment in result.comments],
        video_path=result.video_path,
    )
    return response


@instagram_router.post("/download", response_model=ScrapeResponse)
async def download_instagram_video(
    request: InstagramDownloadRequest,
    service: InstagramScraperService = Depends(get_instagram_service),
) -> ScrapeResponse:
    try:
        result = await service.scrape(
            str(request.url),
            download_video=True,
        )
    except InvalidInstagramUrlError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except InstagramRequestError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    except InstagramParsingError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    except MediaDownloadError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    response = ScrapeResponse(
        metadata=_to_metadata(result.post),
        comments=[_to_comment(comment) for comment in result.comments],
        video_path=result.video_path,
    )
    return response


@gdrive_router.post("/download", response_model=DriveDownloadResponse)
async def download_google_drive_file(
    request: DriveDownloadRequest,
    service: GoogleDriveDownloaderService = Depends(get_google_drive_service),
) -> DriveDownloadResponse:
    try:
        result = await service.download(str(request.url), filename=request.filename)
    except InvalidGoogleDriveUrlError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except GoogleDriveDownloadError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    metadata = DriveFileMetadata(
        file_id=result.file_id,
        file_name=result.file_name,
        mime_type=result.mime_type,
        size_bytes=result.size_bytes,
        local_path=str(result.local_path),
    )
    return DriveDownloadResponse(file=metadata)


@media_router.post("/video-to-audio", response_model=VideoToAudioResponse)
async def convert_video_to_audio(
    file: UploadFile = File(...),
    service: VideoAudioConverterService = Depends(get_media_converter_service),
) -> VideoToAudioResponse:
    try:
        converted = await service.convert(file)
    except MediaProcessingError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    except MediaConversionError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    return VideoToAudioResponse(
        audio_path=str(converted.path),
        format=converted.format,
        size_bytes=converted.size_bytes,
    )


@media_router.post("/transcribe", response_model=TranscriptionResponse)
async def transcribe_audio(
    file: UploadFile = File(...),
    language: Optional[str] = Form(None),
    service: WhisperTranscriberService = Depends(get_transcription_service),
) -> TranscriptionResponse:
    try:
        result = await service.transcribe(file, language=language)
    except TranscriptionModelError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    except TranscriptionProcessingError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    except TranscriptionError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    segments = [
        TranscriptionSegment(
            id=item.get("id"),
            start=item.get("start"),
            end=item.get("end"),
            text=item.get("text", ""),
        )
        for item in result.get("segments", [])
    ]
    return TranscriptionResponse(
        text=result.get("text", ""),
        language=result.get("language"),
        segments=segments,
    )


router.include_router(instagram_router)
router.include_router(gdrive_router)
router.include_router(media_router)
