from __future__ import annotations


class VideoAnalysisError(Exception):
    """Base exception for video analysis failures."""


class VisualAnalysisError(VideoAnalysisError):
    """Raised when visual analysis fails."""


class AudioAnalysisError(VideoAnalysisError):
    """Raised when audio analysis fails."""
