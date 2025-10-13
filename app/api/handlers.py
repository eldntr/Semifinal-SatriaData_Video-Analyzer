
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status

from app.dependencies import (
    get_dataset_visualization_service,
    get_google_drive_service,
    get_instagram_service,
    get_media_converter_service,
    get_transcription_service,
    get_chapter_generation_service,
    get_transcript_summary_service,
    get_wordcloud_generation_service,
    get_video_analysis_service,
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
    ChapterRequest,
    ChapterResponse,
    SummaryRequest,
    SummaryResponse,
    WordCloudRequest,
    WordCloudResponse,
    VideoVisualAnalysisResponse,
    VideoAudioAnalysisResponse,
    VideoFullAnalysisResponse,
    DatasetVisualizationResponse,
    DatasetTableResponse,
)
from app.services.google_drive_downloader import GoogleDriveDownloaderService
from app.services.instagram_scraper import InstagramScraperService
from app.services.media_converter import VideoAudioConverterService
from app.services.chapter_generator import (
    ChapterGenerationError,
    ChapterGenerationService,
)
from app.services.transcript_summary import (
    TranscriptSummaryError,
    TranscriptSummaryService,
)
from app.services.wordcloud_generator import (
    WordCloudGenerationError,
    WordCloudGenerationService,
)
from app.services.video_analysis import VideoAnalysisService
from app.services.dataset_visualization import (
    DatasetVisualizationService,
    DatasetEmptyError,
    DatasetVisualizationError,
    DatasetNotFoundError,
    UnknownVisualizationType,
)
from app.transcription.exceptions import (
    TranscriptionError,
    TranscriptionModelError,
    TranscriptionProcessingError,
)
from app.video_analysis.exceptions import VideoAnalysisError
from app.transcription.service import WhisperTranscriberService

router = APIRouter()
instagram_router = APIRouter(prefix="/instagram", tags=["instagram"])
gdrive_router = APIRouter(prefix="/google-drive", tags=["google-drive"])
media_router = APIRouter(prefix="/media", tags=["media"])
chapters_router = APIRouter(prefix="/chapters", tags=["chapters"])
summary_router = APIRouter(prefix="/summary", tags=["summary"])
wordcloud_router = APIRouter(prefix="/wordcloud", tags=["wordcloud"])
video_analysis_router = APIRouter(prefix="/video-analysis", tags=["video-analysis"])
analytics_router = APIRouter(prefix="/analytics", tags=["analytics"])


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


@chapters_router.post("/generate", response_model=ChapterResponse)
async def generate_chapters(
    request: ChapterRequest,
    service: ChapterGenerationService = Depends(get_chapter_generation_service),
) -> ChapterResponse:
    try:
        items = await service.generate(request)
    except ChapterGenerationError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc
    return ChapterResponse(chapters=items)


@summary_router.post("/generate", response_model=SummaryResponse)
async def summarize_transcript(
    request: SummaryRequest,
    service: TranscriptSummaryService = Depends(get_transcript_summary_service),
) -> SummaryResponse:
    try:
        return await service.summarize(request)
    except TranscriptSummaryError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc


@wordcloud_router.post("/generate", response_model=WordCloudResponse)
async def generate_wordcloud(
    request: WordCloudRequest,
    service: WordCloudGenerationService = Depends(get_wordcloud_generation_service),
) -> WordCloudResponse:
    try:
        return service.generate(request)
    except WordCloudGenerationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@video_analysis_router.post("/visual", response_model=VideoVisualAnalysisResponse)
async def analyze_video_visual(
    file: UploadFile = File(...),
    video_id: Optional[str] = Form(None),
    service: VideoAnalysisService = Depends(get_video_analysis_service),
) -> VideoVisualAnalysisResponse:
    try:
        result = await service.analyze_visual(file, video_id=video_id)
    except VideoAnalysisError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc

    return VideoVisualAnalysisResponse(
        analysis_id=result.analysis_id,
        average_brightness=result.average_brightness,
        std_dev_brightness=result.std_dev_brightness,
        scene_cut_timestamps=result.scene_cut_timestamps,
        brightness_plot_html=result.brightness_plot_html,
        stats_path=str(result.stats_path) if result.stats_path else None,
    )


@video_analysis_router.post("/audio", response_model=VideoAudioAnalysisResponse)
async def analyze_video_audio(
    file: UploadFile = File(...),
    video_id: Optional[str] = Form(None),
    service: VideoAnalysisService = Depends(get_video_analysis_service),
) -> VideoAudioAnalysisResponse:
    try:
        result = await service.analyze_audio(file, video_id=video_id)
    except VideoAnalysisError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc

    return VideoAudioAnalysisResponse(
        analysis_id=result.analysis_id,
        average_pitch_hz=result.average_pitch_hz,
        std_dev_pitch_hz=result.std_dev_pitch_hz,
        spectrogram_plot_html=result.spectrogram_plot_html,
        stats_path=str(result.stats_path) if result.stats_path else None,
    )


@video_analysis_router.post("/full", response_model=VideoFullAnalysisResponse)
async def analyze_video_full(
    file: UploadFile = File(...),
    video_id: Optional[str] = Form(None),
    service: VideoAnalysisService = Depends(get_video_analysis_service),
) -> VideoFullAnalysisResponse:
    try:
        combined = await service.analyze_full(file, video_id=video_id)
    except VideoAnalysisError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc

    visual_response = VideoVisualAnalysisResponse(
        analysis_id=combined.visual.analysis_id,
        average_brightness=combined.visual.average_brightness,
        std_dev_brightness=combined.visual.std_dev_brightness,
        scene_cut_timestamps=combined.visual.scene_cut_timestamps,
        brightness_plot_html=combined.visual.brightness_plot_html,
        stats_path=str(combined.stats_path),
    )
    audio_response = VideoAudioAnalysisResponse(
        analysis_id=combined.audio.analysis_id,
        average_pitch_hz=combined.audio.average_pitch_hz,
        std_dev_pitch_hz=combined.audio.std_dev_pitch_hz,
        spectrogram_plot_html=combined.audio.spectrogram_plot_html,
        stats_path=str(combined.stats_path),
    )
    return VideoFullAnalysisResponse(
        analysis_id=combined.analysis_id,
        visual=visual_response,
        audio=audio_response,
        stats_path=str(combined.stats_path),
    )


@analytics_router.get("/visualizations", response_model=DatasetVisualizationResponse)
async def get_dataset_visualization(
    visualization_type: str = Query(..., alias="type"),
    post_created_from: Optional[datetime] = Query(None),
    post_created_to: Optional[datetime] = Query(None),
    service: DatasetVisualizationService = Depends(get_dataset_visualization_service),
) -> DatasetVisualizationResponse:
    try:
        plots = service.generate_html(
            visualization_type,
            created_from=post_created_from,
            created_to=post_created_to,
        )
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc
    except UnknownVisualizationType as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except DatasetEmptyError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except DatasetVisualizationError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc

    return DatasetVisualizationResponse(plots=plots)


@analytics_router.get("/table", response_model=DatasetTableResponse)
async def get_dataset_table(
    post_created_from: Optional[datetime] = Query(None),
    post_created_to: Optional[datetime] = Query(None),
    service: DatasetVisualizationService = Depends(get_dataset_visualization_service),
) -> DatasetTableResponse:
    try:
        rows = service.generate_table_data(
            created_from=post_created_from,
            created_to=post_created_to,
        )
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc
    except DatasetEmptyError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except DatasetVisualizationError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc

    return DatasetTableResponse(rows=rows)


router.include_router(instagram_router)
router.include_router(gdrive_router)
router.include_router(media_router)
router.include_router(chapters_router)
router.include_router(summary_router)
router.include_router(wordcloud_router)
router.include_router(video_analysis_router)
router.include_router(analytics_router)
