from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, List

from app.config import Settings
from app.instagram.client import InstagramClient
from app.instagram.comment_fetcher import InstagramCrawleeCommentFetcher
from app.instagram.exceptions import (
    InstagramCommentFetchError,
    InstagramParsingError,
    InstagramProfileFetchError,
    InstagramScraperError,
    InstagramViewFetchError,
    MediaDownloadError,
)
from app.instagram.profile_fetcher import InstagramProfileFetcher
from app.instagram.view_fetcher import InstagramCrawleeViewFetcher
from app.instagram.parser import parse_info_payload
from app.instagram.storage import MediaStorage
from app.instagram.types import InstagramComment, InstagramProfile, ScrapedMedia
from app.instagram.url_utils import parse_instagram_url


logger = logging.getLogger(__name__)


class InstagramScraperService:
    def __init__(
        self,
        client: InstagramClient,
        storage: MediaStorage,
        settings: Settings,
        comment_fetcher: InstagramCrawleeCommentFetcher | None = None,
        view_fetcher: InstagramCrawleeViewFetcher | None = None,
        profile_fetcher: InstagramProfileFetcher | None = None,
    ) -> None:
        self._client = client
        self._storage = storage
        self._settings = settings
        self._comment_fetcher = comment_fetcher
        self._view_fetcher = view_fetcher
        self._profile_fetcher = profile_fetcher

    async def scrape(
        self,
        url: str,
        *,
        download_video: bool = True,
    ) -> ScrapedMedia:
        parsed_url = parse_instagram_url(url)
        payload = await self._client.fetch_media_info(parsed_url.canonical_url)

        if self._settings.log_instagram_raw:
            formatted = json.dumps(payload, ensure_ascii=False, indent=2)
            logger.debug("Raw Instagram payload for %s:\n%s", parsed_url.shortcode, formatted)

        post, comments = parse_info_payload(
            payload,
            include_comments=self._settings.include_comments,
            max_comments=self._settings.max_comments,
        )

        if not post.video_url:
            raise InstagramParsingError("The provided URL does not contain a downloadable video")

        if (
            self._settings.include_comments
            and self._comment_fetcher
            and len(comments) < self._settings.max_comments
        ):
            existing_ids = [comment.id for comment in comments if comment.id]
            try:
                extra_comments = await self._comment_fetcher.fetch_comments(
                    shortcode=post.shortcode,
                    limit=self._settings.max_comments,
                    existing_ids=existing_ids,
                )
            except InstagramCommentFetchError as exc:
                logger.warning(
                    "Unable to fetch additional Instagram comments for %s: %s",
                    post.shortcode,
                    exc,
                )
            else:
                if extra_comments:
                    comments.extend(extra_comments)
                    logger.debug("Enriched %d additional Instagram comments for %s", len(extra_comments), post.shortcode)

        if self._settings.include_comments and len(comments) > self._settings.max_comments:
            comments = comments[: self._settings.max_comments]

        if self._settings.include_comments:
            original_count = post.comment_count or 0
            post.comment_count = max(original_count, len(comments))

        owner_stub: dict[str, Any] | None = None
        if self._view_fetcher:
            try:
                details = await self._view_fetcher.fetch_media_details(post.shortcode)
            except InstagramViewFetchError as exc:
                logger.warning(
                    "Unable to fetch Instagram metrics for %s: %s",
                    post.shortcode,
                    exc,
                )
            else:
                view_count = details.get("view_count")
                if view_count is not None:
                    post.view_count = view_count
                    logger.debug(
                        "Enriched Instagram view count for %s to %s",
                        post.shortcode,
                        view_count,
                    )
                total_comments = details.get("comment_count")
                if total_comments is not None:
                    current_count = post.comment_count or 0
                    post.comment_count = max(current_count, total_comments)
                caption = details.get("caption")
                if caption:
                    post.caption = caption
                audio = details.get("audio") or {}
                post.audio_title = audio.get("title")
                post.audio_artist = audio.get("artist")
                post.audio_id = audio.get("audio_id")
                post.audio_url = audio.get("audio_url")
                owner_stub = details.get("owner") if isinstance(details.get("owner"), dict) else None
                if owner_stub:
                    username = owner_stub.get("username")
                    full_name = owner_stub.get("full_name")
                    if username:
                        post.username = username
                    if full_name:
                        post.full_name = full_name
                    post.owner_profile = InstagramProfile(
                        username=username or post.username,
                        full_name=full_name or post.full_name,
                        biography=owner_stub.get("biography"),
                        posts=owner_stub.get("posts"),
                        followers=owner_stub.get("followers"),
                        following=owner_stub.get("following"),
                        profile_pic_url=owner_stub.get("profile_pic_url"),
                    )
        profile_lookup: dict[str, InstagramProfile] = {}
        if self._profile_fetcher:
            owner_username: str | None = None
            if post.owner_profile and post.owner_profile.username:
                owner_username = post.owner_profile.username
            elif owner_stub and owner_stub.get("username"):
                owner_username = owner_stub.get("username")
            elif post.username:
                owner_username = post.username

            limit = min(len(comments), self._settings.max_comments, 30)
            seen: set[str] = set()
            comment_usernames: list[str] = []
            for comment in comments:
                if len(comment_usernames) >= limit:
                    break
                username = (comment.username or "").strip()
                if not username:
                    continue
                key = username.lower()
                if key in seen:
                    continue
                seen.add(key)
                comment_usernames.append(username)

            for username in comment_usernames:
                key = username.lower()
                if key in profile_lookup:
                    continue
                try:
                    profile = await self._profile_fetcher.fetch_profile(username)
                except InstagramProfileFetchError as exc:
                    logger.warning(
                        "Unable to fetch profile for %s: %s",
                        username,
                        exc,
                    )
                    continue
                if profile:
                    profile_lookup[key] = profile

            for comment in comments:
                profile = profile_lookup.get((comment.username or "").lower())
                if profile:
                    comment.profile = profile

            if owner_username:
                owner_key = owner_username.lower()
                owner_profile = profile_lookup.get(owner_key)
                if owner_profile is None:
                    try:
                        owner_profile = await self._profile_fetcher.fetch_profile(owner_username)
                    except InstagramProfileFetchError as exc:
                        logger.warning(
                            "Unable to fetch profile for owner %s: %s",
                            owner_username,
                            exc,
                        )
                    else:
                        if owner_profile:
                            profile_lookup[owner_key] = owner_profile
                if owner_profile:
                    post.owner_profile = owner_profile

        video_path: Path | None = None
        if download_video:
            destination = self._storage.build_video_path(post.shortcode)
            try:
                await self._client.download_media(parsed_url.canonical_url, destination)
            except InstagramScraperError as exc:
                raise MediaDownloadError("Failed to download Instagram video") from exc
            video_path = destination

        normalized_comments: List[InstagramComment] = (
            comments if self._settings.include_comments else []
        )
        return ScrapedMedia(
            post=post,
            comments=normalized_comments,
            video_path=str(video_path) if video_path else None,
            fetched_comment_count=len(normalized_comments),
        )
