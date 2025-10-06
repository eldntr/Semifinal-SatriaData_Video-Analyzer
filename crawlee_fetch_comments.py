from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import asdict
from typing import Any

from app.config import get_settings
from app.instagram.comment_fetcher import InstagramCrawleeCommentFetcher
from app.instagram.exceptions import InstagramCommentFetchError, InvalidInstagramUrlError
from app.instagram.url_utils import parse_instagram_url


async def _run(url: str, limit: int) -> dict[str, Any]:
    parsed_url = parse_instagram_url(url)
    settings = get_settings()
    fetcher = InstagramCrawleeCommentFetcher(settings)

    comments = await fetcher.fetch_comments(
        shortcode=parsed_url.shortcode,
        limit=limit,
        existing_ids=[],
    )

    return {
        "shortcode": parsed_url.shortcode,
        "fetched_count": len(comments),
        "comments": [
            {key: value.isoformat() if hasattr(value, "isoformat") else value for key, value in asdict(comment).items()}
            for comment in comments
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Instagram comments using Crawlee only")
    parser.add_argument("url", help="Instagram post/reel URL")
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum number of comments to retrieve (default: 200)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ...)",
    )

    args = parser.parse_args()

    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except AttributeError:  # pragma: no cover - Python <3.7
        pass

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    try:
        payload = asyncio.run(_run(args.url, args.limit))
    except (InstagramCommentFetchError, InvalidInstagramUrlError) as exc:
        parser.error(str(exc))
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
