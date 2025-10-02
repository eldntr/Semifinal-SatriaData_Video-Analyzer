class InstagramScraperError(Exception):
    """Base error for Instagram scraping failures."""


class InvalidInstagramUrlError(InstagramScraperError):
    """Raised when the supplied URL cannot be parsed."""


class InstagramRequestError(InstagramScraperError):
    """Raised when Instagram returns a non-success response."""


class InstagramParsingError(InstagramScraperError):
    """Raised when the response cannot be parsed into the expected schema."""


class MediaDownloadError(InstagramScraperError):
    """Raised when the video download fails."""
