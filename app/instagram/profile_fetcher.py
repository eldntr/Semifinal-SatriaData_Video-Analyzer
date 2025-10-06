
from __future__ import annotations

import asyncio
import json
import logging
from copy import copy
from http.cookiejar import MozillaCookieJar
from typing import Any, Optional
from urllib.parse import quote

from crawlee.http_clients import ImpitHttpClient
from crawlee.sessions import Session

from app.config import Settings
from app.instagram.exceptions import InstagramProfileFetchError
from app.instagram.types import InstagramProfile


logger = logging.getLogger(__name__)

_MOBILE_USER_AGENT = (
    "Instagram 302.0.0.0.30 Android (29/10; 420dpi; 1080x1920; "
    "OnePlus; ONEPLUS A6013; OnePlus6T; qcom; en_US; 382583461)"
)
_MOBILE_APP_ID = "567067343352427"


class InstagramProfileFetcher:
    """Fetch public Instagram profile data using Crawlee-backed HTTP client."""

    _TOPSEARCH_URL = (
        "https://www.instagram.com/api/v1/web/search/topsearch/"
        "?context=blended&include_reel=true&query={query}"
    )
    _USER_INFO_URL = "https://i.instagram.com/api/v1/users/{user_id}/info/"

    def __init__(self, settings: Settings, *, request_delay: float = 0.25) -> None:
        self._settings = settings
        self._http_client = ImpitHttpClient()
        self._cookie_source = self._load_cookie_jar(settings.cookies_path)
        self._logged_sessionless = False
        self._profile_cache: dict[str, Optional[InstagramProfile]] = {}
        self._id_cache: dict[str, Optional[str]] = {}
        self._request_delay = max(request_delay, 0.0)
        if self._cookie_source:
            logger.debug(
                "Loaded %d cookies for Instagram profile fetching",
                sum(1 for _ in self._cookie_source),
            )
        else:
            logger.warning(
                "Instagram profile fetcher initialized without cookies; profile enrichment may be limited"
            )
            self._logged_sessionless = True

    async def fetch_profile(self, username: str) -> Optional[InstagramProfile]:
        username = (username or "").strip()
        if not username:
            return None

        key = username.lower()
        if key in self._profile_cache:
            return self._profile_cache[key]

        user_id = await self._resolve_user_id(username)
        if not user_id:
            logger.info("Unable to resolve Instagram user id for %s", username)
            self._profile_cache[key] = None
            return None

        payload = await self._fetch_user_payload(user_id)
        profile = self._build_profile(payload, username)
        self._profile_cache[key] = profile

        if self._request_delay:
            await asyncio.sleep(self._request_delay)

        return profile

    async def _resolve_user_id(self, username: str) -> Optional[str]:
        cache_key = username.lower()
        if cache_key in self._id_cache:
            return self._id_cache[cache_key]

        session = self._build_session()
        url = self._TOPSEARCH_URL.format(query=quote(username))
        headers = self._build_topsearch_headers(username)
        logger.debug("Searching Instagram user id for %s", username)

        try:
            response = await self._http_client.send_request(url, headers=headers, session=session)
            body = await response.read()
        except Exception as exc:  # pragma: no cover - network dependent
            logger.warning("Instagram topsearch request failed for %s: %s", username, exc)
            raise InstagramProfileFetchError("Network error while resolving username") from exc

        status_code = getattr(response, "status_code", None)
        if status_code and status_code >= 400:
            logger.info("Instagram returned HTTP %s for username lookup %s", status_code, username)
            self._id_cache[cache_key] = None
            return None

        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            logger.debug("Instagram topsearch payload decode error: %s", body[:200])
            raise InstagramProfileFetchError("Invalid JSON while resolving username") from exc

        user_id = self._extract_user_id(payload, username)
        self._id_cache[cache_key] = user_id
        return user_id

    async def _fetch_user_payload(self, user_id: str) -> dict[str, Any]:
        session = self._build_session()
        url = self._USER_INFO_URL.format(user_id=user_id)
        headers = self._build_user_info_headers()
        logger.debug("Fetching Instagram profile info for user_id=%s", user_id)

        try:
            response = await self._http_client.send_request(url, headers=headers, session=session)
            body = await response.read()
        except Exception as exc:  # pragma: no cover - network dependent
            logger.warning("Instagram user info request failed for %s: %s", user_id, exc)
            raise InstagramProfileFetchError("Network error while fetching profile info") from exc

        status_code = getattr(response, "status_code", None)
        if status_code and status_code >= 400:
            logger.warning("Instagram returned HTTP %s for profile info %s", status_code, user_id)
            return {}

        try:
            return json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            logger.debug("Instagram user info payload decode error: %s", body[:200])
            raise InstagramProfileFetchError("Invalid JSON while fetching profile info") from exc

    def _build_profile(self, payload: dict[str, Any], fallback_username: str) -> Optional[InstagramProfile]:
        user = (payload or {}).get("user") if isinstance(payload, dict) else None
        if not isinstance(user, dict):
            return None

        username = user.get("username") or fallback_username
        profile = InstagramProfile(
            username=username,
            full_name=user.get("full_name") or user.get("name"),
            biography=(user.get("biography") or None),
            posts=self._safe_int(user.get("media_count")),
            followers=self._safe_int(user.get("follower_count")),
            following=self._safe_int(user.get("following_count")),
            profile_pic_url=user.get("profile_pic_url_hd")
            or user.get("hd_profile_pic_url_info", {}).get("url")
            or user.get("profile_pic_url"),
        )
        return profile

    def _extract_user_id(self, payload: dict[str, Any], username: str) -> Optional[str]:
        users = payload.get("users") if isinstance(payload, dict) else None
        if isinstance(users, list):
            for entry in users:
                user = entry.get("user") if isinstance(entry, dict) else None
                if not isinstance(user, dict):
                    continue
                candidate = user.get("username") or ""
                if candidate.lower() == username.lower():
                    identifier = user.get("pk") or user.get("pk_id") or user.get("id")
                    if identifier is not None:
                        return str(identifier)
        return None

    def _build_topsearch_headers(self, username: str) -> dict[str, str]:
        return {
            "User-Agent": self._settings.user_agent,
            "Accept": "application/json, text/plain, */*",
            "Referer": f"https://www.instagram.com/{username}/",
            "X-IG-App-ID": "936619743392459",
            "X-ASBD-ID": "198387",
            "X-Requested-With": "XMLHttpRequest",
        }

    def _build_user_info_headers(self) -> dict[str, str]:
        return {
            "User-Agent": _MOBILE_USER_AGENT,
            "Accept": "application/json",
            "X-IG-App-ID": _MOBILE_APP_ID,
            "Accept-Language": "en-US,en;q=0.9",
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
                logger.warning("Instagram profile requests are unauthenticated")
                self._logged_sessionless = True
            return None

        clone = MozillaCookieJar()
        for cookie in self._cookie_source:
            clone.set_cookie(copy(cookie))
        logger.debug("Using Instagram profile session with %d cookies", sum(1 for _ in clone))
        return Session(cookies=clone)

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return None
