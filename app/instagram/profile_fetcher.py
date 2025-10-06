
from __future__ import annotations

import asyncio
import json
import logging
from copy import copy
from http.cookiejar import MozillaCookieJar
from typing import Any, Optional

from crawlee.http_clients import ImpitHttpClient
from crawlee.sessions import Session

from app.config import Settings
from app.instagram.exceptions import InstagramProfileFetchError
from app.instagram.types import InstagramProfile


logger = logging.getLogger(__name__)


class InstagramProfileFetcher:
    """Fetch public Instagram profile data using a crawlee-backed HTTP client."""

    _PROFILE_URL = "https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"

    def __init__(self, settings: Settings, *, request_delay: float = 0.25) -> None:
        self._settings = settings
        self._http_client = ImpitHttpClient()
        self._cookie_source = self._load_cookie_jar(settings.cookies_path)
        self._logged_sessionless = False
        self._cache: dict[str, Optional[InstagramProfile]] = {}
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
        if key in self._cache:
            return self._cache[key]

        payload = await self._fetch_payload(username)
        profile = self._build_profile(payload, username)
        self._cache[key] = profile

        if self._request_delay:
            await asyncio.sleep(self._request_delay)
        return profile

    async def _fetch_payload(self, username: str) -> dict[str, Any]:
        session = self._build_session()
        url = self._PROFILE_URL.format(username=username)
        headers = self._build_headers(username)
        logger.debug("Fetching Instagram profile for %s", username)

        try:
            response = await self._http_client.send_request(url, headers=headers, session=session)
        except Exception as exc:  # pragma: no cover - network error
            logger.warning("Instagram profile request failed for %s: %s", username, exc)
            raise InstagramProfileFetchError("Network error while fetching profile") from exc

        status_code = getattr(response, "status_code", None)
        body = await response.read()
        logger.debug(
            "Instagram profile response (username=%s, status=%s, bytes=%s)",
            username,
            status_code,
            len(body),
        )

        if status_code and status_code >= 400:
            if status_code == 429:
                logger.warning("Instagram rate limited profile fetch for %s", username)
            elif status_code in {401, 403, 404}:
                logger.info("Instagram profile %s returned status %s", username, status_code)
            else:
                logger.warning("Instagram returned HTTP %s for profile %s", status_code, username)
            return {}

        try:
            return json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            logger.debug("Instagram profile payload decode error: %s", body[:200])
            raise InstagramProfileFetchError("Invalid JSON while fetching profile") from exc

    def _build_profile(self, payload: dict[str, Any], fallback_username: str) -> Optional[InstagramProfile]:
        user = ((payload or {}).get("data") or {}).get("user") if isinstance(payload, dict) else None
        if not isinstance(user, dict):
            return None

        username = user.get("username") or fallback_username
        profile = InstagramProfile(
            username=username,
            full_name=user.get("full_name") or user.get("name"),
            biography=(user.get("biography") or None),
            posts=self._safe_int(((user.get("edge_owner_to_timeline_media") or {}).get("count")) or user.get("media_count")),
            followers=self._safe_int(((user.get("edge_followed_by") or {}).get("count")) or user.get("follower_count")),
            following=self._safe_int(((user.get("edge_follow") or {}).get("count")) or user.get("following_count")),
            profile_pic_url=user.get("profile_pic_url_hd") or user.get("profile_pic_url"),
        )
        return profile

    def _build_headers(self, username: str) -> dict[str, str]:
        return {
            "User-Agent": self._settings.user_agent,
            "Accept": "application/json, text/plain, */*",
            "Referer": f"https://www.instagram.com/{username}/",
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
