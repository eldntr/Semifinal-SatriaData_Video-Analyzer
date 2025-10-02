from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from .exceptions import InstagramParsingError
from .types import InstagramComment, InstagramPost


_HASHTAG_RE = re.compile(r"#(\w+)")
_MENTION_RE = re.compile(r"@(\w+)")
_NUMBER_RE = re.compile(r"([0-9]+(?:[.,][0-9]+)?)")


def _get_first(iterable: Iterable[Any], default: Any = None) -> Any:
    for item in iterable:
        return item
    return default


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.utcfromtimestamp(value)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        candidate = value.strip()
        if candidate.isdigit() and len(candidate) == 8:
            try:
                return datetime.strptime(candidate, "%Y%m%d")
            except ValueError:
                return None
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            return None
    return None


def _collect_tags(text: Optional[str], pattern: re.Pattern[str]) -> List[str]:
    if not text:
        return []
    return [match.group(1) for match in pattern.finditer(text)]


def _select_format_url(info: Dict[str, Any]) -> Optional[str]:
    if info.get("url"):
        return info["url"]
    formats = info.get("formats") or []
    usable_formats = [
        entry for entry in formats if isinstance(entry.get("height"), (int, float))
    ]
    fmt = _get_first(
        sorted(
            usable_formats,
            key=lambda entry: entry.get("height", 0),
            reverse=True,
        )
    )
    if fmt:
        return fmt.get("url") or fmt.get("manifest_url")
    fallback = _get_first(formats)
    if fallback:
        return fallback.get("url") or fallback.get("manifest_url")
    return info.get("webpage_url")


def _decode_number_string(raw: str) -> Optional[int]:
    cleaned = raw.strip().lower()
    if cleaned in {"", "none", "null", "n/a", "na", "nan"}:
        return None
    multiplier = 1
    suffix_map = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}
    if cleaned.endswith(tuple(suffix_map.keys())):
        suffix = cleaned[-1]
        multiplier = suffix_map[suffix]
        cleaned = cleaned[:-1]
    cleaned = cleaned.replace(",", "")
    match = _NUMBER_RE.search(cleaned)
    if not match:
        return None
    try:
        value = float(match.group(1).replace(",", "."))
    except ValueError:
        return None
    return int(value * multiplier)


def _extract_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        return _decode_number_string(value)
    return None


def _extract_view_count(payload: Dict[str, Any]) -> Optional[int]:
    keys = (
        "view_count",
        "viewCount",
        "play_count",
        "playCount",
        "play_count_total",
        "view_count_pretty",
        "play_count_pretty",
        "interaction_count",
    )
    for key in keys:
        number = _extract_int(payload.get(key))
        if number is not None:
            return number
    statistics = payload.get("statistics") or {}
    if isinstance(statistics, dict):
        for key in (
            "view_count",
            "viewCount",
            "play_count",
            "playCount",
            "interaction_count",
        ):
            number = _extract_int(statistics.get(key))
            if number is not None:
                return number
    return None


def parse_info_payload(
    payload: Dict[str, Any],
    *,
    include_comments: bool,
    max_comments: int,
) -> tuple[InstagramPost, List[InstagramComment]]:
    if "id" not in payload:
        raise InstagramParsingError("Missing media identifier in yt-dlp payload")

    caption = payload.get("description") or payload.get("full_description")
    post = InstagramPost(
        shortcode=str(payload.get("id", "")),
        caption=caption,
        username=payload.get("uploader_id") or payload.get("uploader") or "",
        full_name=payload.get("uploader") or payload.get("creator"),
        like_count=_extract_int(payload.get("like_count")),
        comment_count=_extract_int(payload.get("comment_count")),
        view_count=_extract_view_count(payload),
        taken_at=_parse_timestamp(payload.get("timestamp") or payload.get("upload_date")),
        video_duration=payload.get("duration"),
        video_url=_select_format_url(payload),
        thumbnail_url=payload.get("thumbnail"),
        hashtags=_collect_tags(caption, _HASHTAG_RE),
        mentions=_collect_tags(caption, _MENTION_RE),
    )

    comments: List[InstagramComment] = []
    if include_comments:
        raw_comments = payload.get("comments") or []
        for raw in raw_comments:
            comment = InstagramComment(
                id=str(raw.get("id") or raw.get("comment_id") or ""),
                username=raw.get("author") or raw.get("user") or raw.get("username") or "",
                text=raw.get("text") or raw.get("body") or "",
                like_count=_extract_int(raw.get("like_count") or raw.get("likecount")) or 0,
                created_at=_parse_timestamp(raw.get("timestamp") or raw.get("created_at")),
            )
            comments.append(comment)
            if len(comments) >= max_comments:
                break
    return post, comments
