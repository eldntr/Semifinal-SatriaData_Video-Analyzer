class TranscriptionError(Exception):
    """Base error for transcription failures."""


class TranscriptionModelError(TranscriptionError):
    """Raised when WhisperX model cannot be loaded."""


class TranscriptionProcessingError(TranscriptionError):
    """Raised when an audio file cannot be transcribed."""
