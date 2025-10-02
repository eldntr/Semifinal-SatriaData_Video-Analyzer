from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse

from .exceptions import InvalidInstagramUrlError


_SHORTCODE_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_VALID_PATH_PREFIXES = {"p", "reel", "reels", "tv"}
_CANONICAL_SEGMENT = {"reels": "reel"}


@dataclass(frozen=True)
class ParsedInstagramUrl:
    entity: str
    shortcode: str
    canonical_url: str


def parse_instagram_url(url: str) -> ParsedInstagramUrl:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise InvalidInstagramUrlError("URL must use http or https scheme")

    path_parts = [part for part in parsed.path.split("/") if part]
    if len(path_parts) < 2:
        raise InvalidInstagramUrlError("Unsupported Instagram URL format")

    entity = path_parts[0].lower()
    if entity not in _VALID_PATH_PREFIXES:
        raise InvalidInstagramUrlError("Unsupported Instagram URL format")

    shortcode = path_parts[1]
    if not _SHORTCODE_RE.match(shortcode):
        raise InvalidInstagramUrlError("Invalid Instagram shortcode")

    canonical_entity = _CANONICAL_SEGMENT.get(entity, entity)
    canonical_url = f"https://www.instagram.com/{canonical_entity}/{shortcode}/"
    return ParsedInstagramUrl(entity=canonical_entity, shortcode=shortcode, canonical_url=canonical_url)
