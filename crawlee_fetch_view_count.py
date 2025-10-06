
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from typing import Any

from app.config import get_settings
from app.instagram.exceptions import InstagramViewFetchError
from app.instagram.url_utils import parse_instagram_url
from app.instagram.view_fetcher import InstagramCrawleeViewFetcher


async def _run(url: str, log: logging.Logger) -> dict[str, Any]:
    settings = get_settings()
    fetcher = InstagramCrawleeViewFetcher(settings)
    parsed = parse_instagram_url(url)

    try:
        details = await fetcher.fetch_media_details(parsed.shortcode)
    except InstagramViewFetchError as exc:  # pragma: no cover - CLI surface
        log.error("Failed fetching metrics for %s: %s", parsed.shortcode, exc)
        raise

    return {
        "shortcode": parsed.shortcode,
        "view_count": details.get("view_count"),
        "comment_count": details.get("comment_count"),
        "caption": details.get("caption"),
        "audio": details.get("audio"),
        "owner": details.get("owner"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Instagram view count using Crawlee")
    parser.add_argument("url", help="Instagram post/reel URL")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, ...)")

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("crawlee.fetch_view")

    try:
        result = asyncio.run(_run(args.url, log))
    except InstagramViewFetchError:
        raise SystemExit(1)
    except Exception as exc:  # pragma: no cover - CLI surface
        log.error("Unexpected failure while fetching view count: %s", exc)
        raise SystemExit(1) from exc

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
