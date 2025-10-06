
from __future__ import annotations

import json
import logging
from copy import copy
from http.cookiejar import CookieJar, MozillaCookieJar
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlencode

from crawlee.http_clients import ImpitHttpClient
from crawlee.sessions import Session
from yt_dlp.extractor.instagram import _id_to_pk

from app.config import Settings
from app.instagram.exceptions import InstagramCommentFetchError
from app.instagram.parser import _extract_int, _parse_timestamp
from app.instagram.types import InstagramComment


logger = logging.getLogger(__name__)


class InstagramCrawleeCommentFetcher:
    """Fetch Instagram comments using crawlee-backed HTTP client."""

    _API_BASE_URL = "https://i.instagram.com/api/v1"
    _GRAPHQL_URL = "https://www.instagram.com/graphql/query/"
    _GRAPHQL_QUERY_HASH = "97b41c52301f77ce508f55e66d17620e"

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._http_client = ImpitHttpClient()
        self._cookie_source = self._load_cookie_jar(settings.cookies_path)
        self._logged_sessionless = False
        if self._cookie_source:
            cookie_count = sum(1 for _ in self._cookie_source)
            logger.debug("Loaded %d cookies for Instagram comment fetching", cookie_count)
        else:
            logger.warning("Instagram comment fetcher initialized without cookies; comments may be limited")
            self._logged_sessionless = True

    async def fetch_comments(
        self,
        *,
        shortcode: str,
        limit: int,
        existing_ids: Sequence[str] | None = None,
    ) -> List[InstagramComment]:
        if not shortcode or limit <= 0:
            logger.debug("Skipping comment fetch for shortcode=%s with limit=%s", shortcode or "", limit)
            return []

        existing_count = len(existing_ids or [])
        dedupe: Set[str] = {str(comment_id) for comment_id in (existing_ids or []) if comment_id}
        logger.debug("Preparing to fetch comments for %s (limit=%s, existing=%s)", shortcode, limit, existing_count)
        remaining = limit - len(dedupe)
        if remaining <= 0:
            logger.debug("Provided existing comments already satisfy limit=%s for %s", limit, shortcode)
            return []

        try:
            media_pk = _id_to_pk(shortcode)
        except Exception as exc:  # pragma: no cover - defensive branch
            raise InstagramCommentFetchError(
                f"Cannot derive media identifier from shortcode '{shortcode}': {exc}"
            ) from exc

        try:
            return await self._collect_comments(media_pk=media_pk, shortcode=shortcode, dedupe=dedupe, limit=limit)
        except InstagramCommentFetchError:
            raise
        except Exception as exc:  # pragma: no cover - network-dependent
            raise InstagramCommentFetchError(f"Failed to enrich comments: {exc}") from exc

    async def _collect_comments(
        self,
        *,
        media_pk: int,
        shortcode: str,
        dedupe: Set[str],
        limit: int,
    ) -> List[InstagramComment]:
        comments: List[InstagramComment] = []
        cursor: Optional[str] = None
        cursor_kind: Optional[str] = None
        mode: str = "api_v1"

        while len(dedupe) + len(comments) < limit:
            response_payload = await self._fetch_page(
                media_pk=media_pk,
                shortcode=shortcode,
                cursor=cursor,
                cursor_kind=cursor_kind,
                mode=mode,
                remaining=limit - (len(dedupe) + len(comments)),
            )
            if not response_payload:
                break

            for node in self._extract_comment_nodes(response_payload):
                comment = self._build_comment(node)
                if not comment or not comment.id or comment.id in dedupe:
                    continue
                comments.append(comment)
                dedupe.add(comment.id)
                if len(dedupe) >= limit:
                    break

            mode, cursor, cursor_kind, has_more = self._extract_pagination_state(
                response_payload, current_mode=mode
            )
            if not has_more or not cursor:
                break

        return comments

    async def _fetch_page(
        self,
        *,
        media_pk: int,
        shortcode: str,
        cursor: Optional[str],
        cursor_kind: Optional[str],
        mode: str,
        remaining: int,
    ) -> Dict[str, Any]:
        headers = self._build_headers(shortcode)
        session = self._build_session()
        if mode == "graphql":
            payload = self._build_graphql_request(
                shortcode=shortcode, cursor=cursor if cursor_kind == "after" else cursor, remaining=remaining
            )
            url = f"{self._GRAPHQL_URL}?{urlencode(payload)}"
        else:
            params = self._build_api_params(cursor=cursor, cursor_kind=cursor_kind, remaining=remaining)
            url = f"{self._API_BASE_URL}/media/{media_pk}/comments/?{urlencode(params)}"

        logger.debug("Issuing Instagram comments request (mode=%s, cursor_kind=%s, cursor=%s, remaining=%s)", mode, cursor_kind, cursor, remaining)
        try:
            response = await self._http_client.send_request(url, headers=headers, session=session)
        except Exception as exc:  # pragma: no cover - network dependent
            logger.warning("Instagram comments HTTP request failed (mode=%s, cursor=%s): %s", mode, cursor, exc)
            raise InstagramCommentFetchError("Network error while fetching Instagram comments") from exc

        status_code = getattr(response, "status_code", None)
        raw = await response.read()
        if status_code and status_code >= 400:
            logger.warning("Instagram returned HTTP %s for comments request (mode=%s, cursor=%s)", status_code, mode, cursor)
            logger.debug("Instagram error payload: %s", raw[:200])
            raise InstagramCommentFetchError("Instagram responded with an error status while fetching comments")

        logger.debug("Instagram comments response received (mode=%s, cursor=%s, status=%s, bytes=%s)", mode, cursor, status_code, len(raw))
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            logger.debug("Instagram comment payload decode error: %s", raw[:200])
            raise InstagramCommentFetchError("Invalid JSON while fetching Instagram comments") from exc

    def _build_headers(self, shortcode: str) -> Dict[str, str]:
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

    def _build_api_params(
        self, *, cursor: Optional[str], cursor_kind: Optional[str], remaining: int
    ) -> Dict[str, str]:
        params: Dict[str, str] = {
            "can_support_threading": "true",
            "permalink_enabled": "false",
            "page_size": str(min(max(remaining, 1), 50)),
        }
        if cursor:
            if cursor_kind == "min_id":
                params["min_id"] = cursor
            elif cursor_kind == "cursor":
                params["cursor"] = cursor
            else:
                params["max_id"] = cursor
        return params

    def _build_graphql_request(self, *, shortcode: str, cursor: Optional[str], remaining: int) -> Dict[str, str]:
        first = min(max(remaining, 1), 50)
        variables: Dict[str, Any] = {"shortcode": shortcode, "first": first}
        if cursor:
            variables["after"] = cursor
        return {
            "query_hash": self._GRAPHQL_QUERY_HASH,
            "variables": json.dumps(variables, separators=(",", ":")),
        }

    def _extract_pagination_state(
        self,
        payload: Dict[str, Any],
        *,
        current_mode: str,
    ) -> Tuple[str, Optional[str], Optional[str], bool]:
        container = self._locate_comment_container(payload)
        if container:
            page_info = container.get("page_info")
            if isinstance(page_info, dict):
                next_cursor = page_info.get("end_cursor")
                has_more = bool(page_info.get("has_next_page"))
                logger.debug("GraphQL pagination info: end_cursor=%s, has_next_page=%s", next_cursor, has_more)
                if next_cursor and has_more:
                    return "graphql", str(next_cursor), "after", True

        next_max_id = payload.get("next_max_id")
        next_min_id = payload.get("next_min_id")
        has_more_comments = payload.get("has_more_comments")
        has_more_headload = payload.get("has_more_headload_comments")

        if next_max_id:
            has_more = has_more_comments if has_more_comments is not None else has_more_headload
            logger.debug("API pagination info (max_id): next=%s, has_more=%s", next_max_id, has_more)
            return "api_v1", str(next_max_id), "max_id", bool(has_more if has_more is not None else True)

        if next_min_id:
            has_more = has_more_headload if has_more_headload is not None else has_more_comments
            logger.debug("API pagination info (min_id): next=%s, has_more=%s", next_min_id, has_more)
            return "api_v1", str(next_min_id), "min_id", bool(has_more if has_more is not None else True)

        logger.debug("No pagination cursor returned by Instagram for %s", payload.get("id") or "<unknown>")
        return current_mode, None, None, False

    def _extract_comment_nodes(self, payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        container = self._locate_comment_container(payload)
        if container:
            edges = container.get("edges") or []
            for edge in edges:
                if isinstance(edge, dict):
                    node = edge.get("node") if isinstance(edge.get("node"), dict) else edge
                    if isinstance(node, dict):
                        yield node
            return

        raw_comments = payload.get("comments")
        if isinstance(raw_comments, list):
            for entry in raw_comments:
                if isinstance(entry, dict):
                    node = entry.get("node") if isinstance(entry.get("node"), dict) else entry
                    if isinstance(node, dict):
                        yield node

    def _locate_comment_container(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        direct = payload.get("edge_media_to_parent_comment")
        if isinstance(direct, dict):
            return direct

        data = payload.get("data")
        if isinstance(data, dict):
            media = data.get("shortcode_media")
            if isinstance(media, dict):
                container = media.get("edge_media_to_parent_comment")
                if isinstance(container, dict):
                    return container

        api_v1 = payload.get("xdt_api__v1__media__comments")
        if isinstance(api_v1, dict):
            return api_v1

        return None

    def _build_comment(self, node: Dict[str, Any]) -> Optional[InstagramComment]:
        identifier = node.get("id") or node.get("pk")
        if not identifier:
            return None

        owner = node.get("owner") or node.get("user") or {}
        username = owner.get("username") or ""
        text = node.get("text") or node.get("body") or ""
        like_source = (
            (node.get("edge_liked_by") or {}).get("count")
            or node.get("comment_like_count")
            or node.get("like_count")
        )
        created_source = (
            node.get("created_at")
            or node.get("created_at_utc")
            or node.get("created_at_timestamp")
            or node.get("created_time")
        )

        return InstagramComment(
            id=str(identifier),
            username=username,
            text=text,
            like_count=_extract_int(like_source) or 0,
            created_at=_parse_timestamp(created_source),
        )

    def _load_cookie_jar(self, path: Optional[Path]) -> Optional[CookieJar]:
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
                logger.warning("Instagram comment requests are unauthenticated; additional comments may be unavailable")
                self._logged_sessionless = True
            return None

        cloned = MozillaCookieJar()
        for cookie in self._cookie_source:
            cloned.set_cookie(copy(cookie))
        logger.debug("Using Instagram session with %d cookies", sum(1 for _ in cloned))
        return Session(cookies=cloned)

