from __future__ import annotations

import uuid
from pathlib import Path


class TranscriptionStorage:
    def __init__(self, root: Path) -> None:
        self._root = root
        self._audio_dir = self._root / "audio"
        self._tmp_dir = self._root / "tmp"
        self._audio_dir.mkdir(parents=True, exist_ok=True)
        self._tmp_dir.mkdir(parents=True, exist_ok=True)

    def build_audio_path(self, original_name: str) -> Path:
        stem = Path(original_name).stem or uuid.uuid4().hex
        suffix = Path(original_name).suffix or ".wav"
        filename = f"{stem}{suffix}"
        return (self._audio_dir / filename).resolve()

    def build_temp_path(self, suffix: str) -> Path:
        normalized_suffix = suffix if suffix.startswith('.') else f".{suffix}" if suffix else ".tmp"
        filename = f"{uuid.uuid4().hex}{normalized_suffix}"
        return (self._tmp_dir / filename).resolve()

    @staticmethod
    def cleanup(path: Path) -> None:
        try:
            if path.exists():
                path.unlink()
        except OSError:
            pass
