from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


def _as_bool(value: str, *, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _as_int(value: str, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/128.0.0.0 Safari/537.36"
)
DEFAULT_YTDLP_FORMAT = (
    "best[acodec!=none][vcodec!=none]/best"
)


@dataclass(frozen=True)
class Settings:
    instagram_base_url: str = os.getenv("INSTAGRAM_BASE_URL", "https://www.instagram.com")
    request_timeout: float = float(os.getenv("INSTAGRAM_REQUEST_TIMEOUT", "20"))
    media_directory: Path = Path(os.getenv("MEDIA_DIR", "downloads"))
    user_agent: str = os.getenv("INSTAGRAM_USER_AGENT", DEFAULT_USER_AGENT)
    include_comments: bool = _as_bool(os.getenv("INSTAGRAM_INCLUDE_COMMENTS", "true"), default=True)
    max_comments: int = _as_int(os.getenv("INSTAGRAM_MAX_COMMENTS"), default=200)
    cookies_path: Optional[Path] = None
    ytdlp_format: str = os.getenv("INSTAGRAM_YTDLP_FORMAT", DEFAULT_YTDLP_FORMAT)
    ytdlp_retries: int = _as_int(os.getenv("INSTAGRAM_YTDLP_RETRIES"), default=3)
    log_instagram_raw: bool = _as_bool(os.getenv("INSTAGRAM_LOG_RAW", "false"))
    whisper_model: str = os.getenv("WHISPER_MODEL", "large-v2")
    whisper_language: Optional[str] = os.getenv("WHISPER_LANGUAGE")
    whisper_device: str = os.getenv("WHISPER_DEVICE", "auto")
    whisper_compute_type: str = os.getenv("WHISPER_COMPUTE_TYPE", "auto")
    whisper_batch_size: int = _as_int(os.getenv("WHISPER_BATCH_SIZE"), default=8)


_settings: Settings | None = None


def _resolve_cookies_path() -> Optional[Path]:
    candidate = os.getenv("INSTAGRAM_COOKIES_PATH")
    if candidate:
        path = Path(candidate).expanduser().resolve()
        if path.exists():
            return path
        raise FileNotFoundError(f"Instagram cookies file '{path}' does not exist")
    default_path = Path("cookies.txt").resolve()
    if default_path.exists():
        return default_path
    return None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        settings = Settings()
        media_dir = settings.media_directory.expanduser().resolve()
        media_dir.mkdir(parents=True, exist_ok=True)
        object.__setattr__(settings, "media_directory", media_dir)
        object.__setattr__(settings, "cookies_path", _resolve_cookies_path())
        _settings = settings
    return _settings
