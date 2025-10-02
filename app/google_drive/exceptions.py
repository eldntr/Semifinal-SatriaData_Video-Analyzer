class GoogleDriveError(Exception):
    """Base error for Google Drive download failures."""


class InvalidGoogleDriveUrlError(GoogleDriveError):
    """Raised when Google Drive URL cannot be parsed or lacks a file id."""


class GoogleDriveRequestError(GoogleDriveError):
    """Raised when Google Drive returns an unexpected response."""


class GoogleDriveDownloadError(GoogleDriveError):
    """Raised when downloading from Google Drive fails."""
