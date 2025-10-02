from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class GoogleDriveFile:
    file_id: str
    file_name: str
    mime_type: Optional[str]
    size_bytes: Optional[int]
    local_path: Path
