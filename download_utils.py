from __future__ import annotations
import os
import re
from pathlib import Path
from typing import Dict, Optional, Tuple
import requests
import yt_dlp
from pytube import YouTube
from app.config import DEFAULT_USER_AGENT, DEFAULT_YTDLP_FORMAT
def get_link_type(url: str) -> str:
    if not isinstance(url, str):
        return "unknown"
    lowered = url.lower()
    if "drive.google.com" in lowered:
        return "gdrive"
    if "youtube.com" in lowered or "youtu.be" in lowered:
        return "youtube"
    if "instagram.com" in lowered or "cdninstagram.com" in lowered:
        return "instagram"
    return "unknown"
_DRIVE_REGEX = re.compile(r"/d/([a-zA-Z0-9_-]+)")
_DRIVE_QUERY_REGEX = re.compile(r"id=([a-zA-Z0-9_-]+)")
def convert_drive_link(url: str) -> str:
    match = _DRIVE_REGEX.search(url)
    if match:
        return f"https://drive.google.com/uc?id={match.group(1)}"
    match_export = _DRIVE_QUERY_REGEX.search(url)
    if match_export:
        return f"https://drive.google.com/uc?id={match_export.group(1)}"
    return url
def download_gdrive_requests(url: str, output_path: Path) -> Tuple[bool, Optional[str]]:
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        session = requests.Session()
        response = session.get(url, stream=True)
        token = None
        for key, value in response.cookies.items():
            if key.startswith("download_warning"):
                token = value
                break
        if token:
            url_with_token = f"{url}&confirm={token}"
            response = session.get(url_with_token, stream=True)
        with output_path.open("wb") as handle:
            for chunk in response.iter_content(8192):
                if chunk:
                    handle.write(chunk)
        return True, None
    except Exception as exc:  # pragma: no cover - simple CLI helper
        return False, str(exc)
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
def _build_instagram_headers(referer: Optional[str]) -> Dict[str, str]:
    headers = {
        "User-Agent": os.getenv("INSTAGRAM_USER_AGENT", DEFAULT_USER_AGENT),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": os.getenv("INSTAGRAM_ACCEPT_LANGUAGE", "en-US,en;q=0.9"),
    }
    headers["Referer"] = referer or os.getenv("INSTAGRAM_BASE_URL", "https://www.instagram.com")
    return headers
def _build_instagram_options(
    output_path: Path,
    cookie_file_path: Optional[Path],
    referer: Optional[str],
) -> Dict[str, object]:
    options: Dict[str, object] = {
        "format": os.getenv("INSTAGRAM_YTDLP_FORMAT", DEFAULT_YTDLP_FORMAT),
        "outtmpl": str(output_path),
        "quiet": True,
        "noplaylist": True,
        "merge_output_format": "mp4",
        "overwrites": True,
        "retries": _env_int("INSTAGRAM_YTDLP_RETRIES", 3),
        "user_agent": os.getenv("INSTAGRAM_USER_AGENT", DEFAULT_USER_AGENT),
        "http_headers": _build_instagram_headers(referer),
    }
    if cookie_file_path:
        options["cookiefile"] = str(cookie_file_path)
    return options
def download_instagram(
    url: str,
    output_path: Path,
    cookie_file_path: Optional[Path],
) -> None:
    if cookie_file_path and not cookie_file_path.exists():
        raise FileNotFoundError(
            f"Instagram cookie file '{cookie_file_path}' tidak ditemukan"
        )
    referer = os.getenv("INSTAGRAM_BASE_URL", "https://www.instagram.com")
    info_opts = _build_instagram_options(output_path, cookie_file_path, referer)
    info_opts["skip_download"] = True
    target_url = url
    try:
        with yt_dlp.YoutubeDL(info_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            target_url = (
                info.get("webpage_url")
                or info.get("original_url")
                or info.get("url")
                or url
            )
            referer = info.get("webpage_url") or referer
    except yt_dlp.utils.DownloadError as exc:
        print(f"⚠️ Gagal mengambil metadata Instagram: {exc}")
        target_url = url
    download_opts = _build_instagram_options(output_path, cookie_file_path, referer)
    download_opts.pop("skip_download", None)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with yt_dlp.YoutubeDL(download_opts) as ydl:
        ydl.download([target_url])
def download_youtube(url: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    stream = YouTube(url).streams.get_highest_resolution()
    stream.download(filename=str(output_path))
def download_video(
    url: str,
    output_path: Path,
    *,
    cookie_file_path: Optional[Path] = None,
) -> None:
    link_type = get_link_type(url)
    if link_type == "gdrive":
        clean_url = convert_drive_link(url)
        success, error_msg = download_gdrive_requests(clean_url, output_path)
        if not success:
            raise RuntimeError(error_msg or "Unknown Google Drive error")
        return
    if link_type == "youtube":
        download_youtube(url, output_path)
        return
    if link_type == "instagram":
        download_instagram(url, output_path, cookie_file_path)
        return
    raise ValueError("Link tidak dikenali atau tidak didukung")