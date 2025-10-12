from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from uuid import uuid4


@dataclass(frozen=True)
class VideoAnalysisWorkspace:
    identifier: str
    directory: Path
    video_path: Path
    audio_path: Path
    brightness_plot_path: Path
    spectrogram_plot_path: Path
    visual_stats_path: Path
    audio_stats_path: Path
    combined_stats_path: Path


class VideoAnalysisStorage:
    def __init__(self, root: Path) -> None:
        self._root = root
        self._root.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _sanitize_identifier(value: Optional[str]) -> str:
        if value:
            sanitized = re.sub(r"[^a-zA-Z0-9_\-]", "-", value.strip())
            sanitized = re.sub(r"-{2,}", "-", sanitized).strip("-")
            if sanitized:
                return sanitized.lower()
        return uuid4().hex

    def create_workspace(self, requested_id: Optional[str], filename: Optional[str]) -> VideoAnalysisWorkspace:
        identifier = self._sanitize_identifier(requested_id)
        workspace_dir = self._root / identifier
        if workspace_dir.exists():
            shutil.rmtree(workspace_dir, ignore_errors=True)
        workspace_dir.mkdir(parents=True, exist_ok=True)

        suffix = Path(filename or "").suffix or ".mp4"
        video_path = workspace_dir / f"source{suffix}"
        audio_path = workspace_dir / "temp_audio.wav"
        brightness_plot_path = workspace_dir / "brightness_plot.html"
        spectrogram_plot_path = workspace_dir / "spectrogram_plot.html"
        visual_stats_path = workspace_dir / "visual_stats.json"
        audio_stats_path = workspace_dir / "audio_stats.json"
        combined_stats_path = workspace_dir / "combined_stats.json"

        return VideoAnalysisWorkspace(
            identifier=identifier,
            directory=workspace_dir,
            video_path=video_path,
            audio_path=audio_path,
            brightness_plot_path=brightness_plot_path,
            spectrogram_plot_path=spectrogram_plot_path,
            visual_stats_path=visual_stats_path,
            audio_stats_path=audio_stats_path,
            combined_stats_path=combined_stats_path,
        )

    @staticmethod
    def cleanup_temp_audio(workspace: VideoAnalysisWorkspace) -> None:
        try:
            if workspace.audio_path.exists():
                workspace.audio_path.unlink()
        except OSError:
            pass
