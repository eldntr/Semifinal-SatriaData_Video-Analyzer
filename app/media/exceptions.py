class MediaConversionError(Exception):
    """Base error for media conversion operations."""


class UnsupportedMediaFormatError(MediaConversionError):
    """Raised when an unsupported target format is requested."""


class MediaProcessingError(MediaConversionError):
    """Raised when ffmpeg or file processing fails."""
