from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import parse_qs, urlparse

from .exceptions import InvalidGoogleDriveUrlError


_FILE_ID_RE = re.compile(r"/d/([a-zA-Z0-9_-]{10,})")
_ALTERNATE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{10,}$")


@dataclass(frozen=True)
class ParsedGoogleDriveUrl:
    file_id: str
    original_url: str


def _from_path(path: str) -> Optional[str]:
    match = _FILE_ID_RE.search(path)
    if match:
        return match.group(1)
    segments = [segment for segment in path.split("/") if segment]
    if segments:
        last_segment = segments[-1]
        if _ALTERNATE_ID_RE.match(last_segment):
            return last_segment
    return None


def _from_query(query: str) -> Optional[str]:
    params = parse_qs(query)
    for key in ("id", "file_id"):
        if key in params and params[key]:
            candidate = params[key][0]
            if _ALTERNATE_ID_RE.match(candidate):
                return candidate
    return None


def parse_google_drive_url(url: str) -> ParsedGoogleDriveUrl:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise InvalidGoogleDriveUrlError("Invalid Google Drive URL")

    file_id = _from_path(parsed.path) or _from_query(parsed.query)
    if not file_id:
        raise InvalidGoogleDriveUrlError("Unable to extract file id from Google Drive URL")

    return ParsedGoogleDriveUrl(file_id=file_id, original_url=url)
