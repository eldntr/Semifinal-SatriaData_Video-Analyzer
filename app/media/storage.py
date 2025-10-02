from __future__ import annotations

import uuid
from pathlib import Path


class ConversionStorage:
    def __init__(self, root: Path) -> None:
        self._root = root
        self._audio_dir = self._root / "audio"
        self._tmp_dir = self._root / "tmp"
        self._audio_dir.mkdir(parents=True, exist_ok=True)
        self._tmp_dir.mkdir(parents=True, exist_ok=True)

    @property
    def audio_dir(self) -> Path:
        return self._audio_dir

    @property
    def tmp_dir(self) -> Path:
        return self._tmp_dir

    def build_output_path(self, stem: str, extension: str) -> Path:
        safe_stem = stem or uuid.uuid4().hex
        filename = f"{safe_stem}.{extension}"
        return self._audio_dir / filename

    def build_temp_path(self, suffix: str) -> Path:
        if not suffix.startswith('.'):
            suffix = f'.{suffix}' if suffix else ''
        stem = uuid.uuid4().hex
        return self._tmp_dir / f"{stem}{suffix}"

    @staticmethod
    def cleanup(path: Path) -> None:
        try:
            if path.exists():
                path.unlink()
        except OSError:
            pass
