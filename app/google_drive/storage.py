from __future__ import annotations

from pathlib import Path


class GoogleDriveStorage:
    def __init__(self, root: Path) -> None:
        self._root = root
        self._root.mkdir(parents=True, exist_ok=True)

    @property
    def root(self) -> Path:
        return self._root

    def ensure_within_root(self, path: Path) -> Path:
        try:
            path.relative_to(self._root)
        except ValueError:
            target = self._root / path.name
            path.replace(target)
            return target
        return path
