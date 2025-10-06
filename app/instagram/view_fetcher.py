
from __future__ import annotations

import json
import logging
from copy import copy
from http.cookiejar import MozillaCookieJar
from typing import Any, Optional

from crawlee.http_clients import ImpitHttpClient
from crawlee.sessions import Session
from yt_dlp.extractor.instagram import _id_to_pk

from app.config import Settings
from app.instagram.exceptions import InstagramViewFetchError


logger = logging.getLogger(__name__)


class InstagramCrawleeViewFetcher:
    """Retrieve Instagram media metrics using a crawlee-backed HTTP client."""

    _INFO_URL = "https://i.instagram.com/api/v1/media/{media_pk}/info/"

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._http_client = ImpitHttpClient()
        self._cookie_source = self._load_cookie_jar(settings.cookies_path)
        self._logged_sessionless = False
        if self._cookie_source:
            logger.debug(
                "Loaded %d cookies for Instagram view fetching",
                sum(1 for _ in self._cookie_source),
            )
        else:
            logger.warning(
                "Instagram view fetcher initialized without cookies; metrics may be limited"
            )
            self._logged_sessionless = True

    async def fetch_view_count(self, shortcode: str) -> Optional[int]:
        details = await self.fetch_media_details(shortcode)
        return details.get("view_count")

    async def fetch_metrics(self, shortcode: str) -> tuple[Optional[int], Optional[int]]:
        details = await self.fetch_media_details(shortcode)
        return details.get("view_count"), details.get("comment_count")

    async def fetch_media_details(self, shortcode: str) -> dict[str, Any]:
        payload = await self._fetch_info_payload(shortcode)
        view_count = self._extract_view_count(payload)
        comment_count = self._extract_comment_count(payload)
        caption = self._extract_caption(payload)
        audio = self._extract_audio_info(payload)
        owner = self._extract_owner_info(payload)
        return {
            "view_count": view_count,
            "comment_count": comment_count,
            "caption": caption,
            "audio": audio,
            "owner": owner,
        }

    async def _fetch_info_payload(self, shortcode: str) -> dict[str, Any]:
        if not shortcode:
            raise InstagramViewFetchError("Missing shortcode for media fetch")

        try:
            media_pk = _id_to_pk(shortcode)
        except Exception as exc:  # pragma: no cover - defensive branch
            raise InstagramViewFetchError(
                f"Cannot derive media identifier from shortcode '{shortcode}': {exc}"
            ) from exc

        session = self._build_session()
        url = self._INFO_URL.format(media_pk=media_pk)
        headers = self._build_headers(shortcode)
        logger.debug("Fetching Instagram media info for %s (media_pk=%s)", shortcode, media_pk)

        try:
            response = await self._http_client.send_request(url, headers=headers, session=session)
        except Exception as exc:  # pragma: no cover - network dependent
            logger.warning("Instagram media info request failed for %s: %s", shortcode, exc)
            raise InstagramViewFetchError("Network error while fetching media info") from exc

        status_code = getattr(response, "status_code", None)
        body = await response.read()
        logger.debug(
            "Instagram media info response (shortcode=%s, status=%s, bytes=%s)",
            shortcode,
            status_code,
            len(body),
        )

        if status_code and status_code >= 400:
            logger.warning(
                "Instagram returned HTTP %s while fetching media info for %s",
                status_code,
                shortcode,
            )
            logger.debug("Instagram media info error payload: %s", body[:200])
            raise InstagramViewFetchError("Instagram responded with an error status while fetching media info")

        try:
            return json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            logger.debug("Instagram media info payload decode error: %s", body[:200])
            raise InstagramViewFetchError("Invalid JSON while fetching media info") from exc

    def _build_headers(self, shortcode: str) -> dict[str, str]:
        return {
            "User-Agent": self._settings.user_agent,
            "Accept": "application/json, text/plain, */*",
            "Referer": f"https://www.instagram.com/p/{shortcode}/",
            "Origin": "https://www.instagram.com",
            "X-IG-App-ID": "936619743392459",
            "X-IG-WWW-Claim": "0",
            "X-ASBD-ID": "198387",
            "X-Requested-With": "XMLHttpRequest",
        }

    def _load_cookie_jar(self, path: Optional[Any]) -> Optional[MozillaCookieJar]:
        if not path:
            return None

        jar = MozillaCookieJar(str(path))
        try:
            jar.load(ignore_discard=True, ignore_expires=True)
        except FileNotFoundError:
            logger.warning("Instagram cookies file '%s' not found; proceeding without it", path)
            return None
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning("Failed to load Instagram cookies from '%s': %s", path, exc)
            return None

        return jar

    def _build_session(self) -> Optional[Session]:
        if not self._cookie_source:
            if not self._logged_sessionless:
                logger.warning("Instagram media info requests are unauthenticated")
                self._logged_sessionless = True
            return None

        clone = MozillaCookieJar()
        for cookie in self._cookie_source:
            clone.set_cookie(copy(cookie))
        logger.debug("Using Instagram view session with %d cookies", sum(1 for _ in clone))
        return Session(cookies=clone)

    @staticmethod
    def _extract_view_count(payload: dict[str, Any]) -> Optional[int]:
        items = payload.get("items")
        if not isinstance(items, list) or not items:
            return None

        media = items[0]
        if not isinstance(media, dict):
            return None

        for key in (
            "view_count",
            "video_view_count",
            "play_count",
            "play_count_total",
            "view_count_pretty",
            "play_count_pretty",
        ):
            value = media.get(key)
            if isinstance(value, (int, float)):
                return int(value)
            if isinstance(value, str):
                digits = "".join(ch for ch in value if ch.isdigit())
                if digits:
                    return int(digits)
        return None

    @staticmethod
    def _extract_comment_count(payload: dict[str, Any]) -> Optional[int]:
        items = payload.get("items")
        if isinstance(items, list) and items:
            media = items[0]
            if isinstance(media, dict):
                value = media.get("comment_count") or media.get("commentCount")
                if isinstance(value, (int, float)):
                    return int(value)
                if isinstance(value, str):
                    digits = ''.join(ch for ch in value if ch.isdigit())
                    if digits:
                        return int(digits)
        return None

    @staticmethod
    def _extract_caption(payload: dict[str, Any]) -> Optional[str]:
        items = payload.get("items")
        if isinstance(items, list) and items:
            media = items[0]
            if isinstance(media, dict):
                caption = media.get("caption")
                if isinstance(caption, dict):
                    text_value = caption.get("text")
                    if isinstance(text_value, str):
                        stripped = text_value.strip()
                        return stripped or None
        return None

    @staticmethod
    def _extract_audio_info(payload: dict[str, Any]) -> Optional[dict[str, Optional[str]]]:
        items = payload.get("items")
        if not isinstance(items, list) or not items:
            return None
        media = items[0]
        if not isinstance(media, dict):
            return None

        clips_metadata = media.get("clips_metadata")
        if not isinstance(clips_metadata, dict):
            clips_metadata = {}

        def _normalize_audio_url(value: Any) -> Optional[str]:
            if isinstance(value, str) and value.startswith("http"):
                return value
            return None

        audio_info: Optional[dict[str, Optional[str]]] = None

        music_info = clips_metadata.get("music_info")
        if isinstance(music_info, dict):
            music_asset = music_info.get("music_asset_info")
            if isinstance(music_asset, dict):
                title = music_asset.get("title")
                artist = music_asset.get("display_artist")
                audio_id = music_asset.get("id") or music_asset.get("audio_asset_id")
                audio_url = _normalize_audio_url(music_asset.get("progressive_download_url")) or _normalize_audio_url(music_asset.get("dash_manifest"))
                if any([title, artist, audio_id, audio_url]):
                    audio_info = {
                        "title": title,
                        "artist": artist,
                        "audio_id": str(audio_id) if audio_id is not None else None,
                        "audio_url": audio_url,
                    }

        if audio_info is None:
            original_sound = clips_metadata.get("original_sound_info")
            if isinstance(original_sound, dict):
                title = original_sound.get("original_audio_title")
                artist = original_sound.get("original_audio_artist") or media.get("user", {}).get("username")
                audio_id = original_sound.get("audio_asset_id")
                audio_url = _normalize_audio_url(original_sound.get("progressive_download_url")) or _normalize_audio_url(original_sound.get("dash_manifest"))
                if any([title, artist, audio_id, audio_url]):
                    audio_info = {
                        "title": title,
                        "artist": artist,
                        "audio_id": str(audio_id) if audio_id is not None else None,
                        "audio_url": audio_url,
                    }

        return audio_info

    @staticmethod
    def _extract_owner_info(payload: dict[str, Any]) -> Optional[dict[str, Optional[str]]]:
        items = payload.get("items")
        if not isinstance(items, list) or not items:
            return None
        media = items[0]
        if not isinstance(media, dict):
            return None

        user: Optional[dict[str, Any]] = None
        for key in ("owner", "user"):
            candidate = media.get(key)
            if isinstance(candidate, dict):
                user = candidate
                break
        if not isinstance(user, dict):
            return None

        username = user.get("username")
        full_name = user.get("full_name")
        biography = user.get("biography")
        profile_pic = user.get("profile_pic_url_hd") or user.get("profile_pic_url")

        posts = InstagramCrawleeViewFetcher._safe_int(
            user.get("media_count") or user.get("mediaCount")
        )
        followers = InstagramCrawleeViewFetcher._safe_int(
            user.get("follower_count")
            or user.get("followerCount")
            or (user.get("edge_followed_by") or {}).get("count")
        )
        following = InstagramCrawleeViewFetcher._safe_int(
            user.get("following_count")
            or user.get("followingCount")
            or (user.get("edge_follow") or {}).get("count")
        )

        if not any([username, full_name, biography, posts, followers, following, profile_pic]):
            return None

        return {
            "username": username,
            "full_name": full_name,
            "biography": biography,
            "posts": posts,
            "followers": followers,
            "following": following,
            "profile_pic_url": profile_pic,
        }

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return None
