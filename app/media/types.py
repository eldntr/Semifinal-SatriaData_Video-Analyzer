from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ConvertedAudio:
    path: Path
    format: str
    size_bytes: int
