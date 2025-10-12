from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass(frozen=True)
class VisualAnalysisResult:
    analysis_id: str
    average_brightness: float
    std_dev_brightness: float
    scene_cut_timestamps: List[float]
    brightness_plot_path: Path
    stats_path: Optional[Path] = None


@dataclass(frozen=True)
class AudioAnalysisResult:
    analysis_id: str
    average_pitch_hz: float
    std_dev_pitch_hz: float
    spectrogram_plot_path: Path
    stats_path: Optional[Path] = None


@dataclass(frozen=True)
class CombinedAnalysisResult:
    analysis_id: str
    visual: VisualAnalysisResult
    audio: AudioAnalysisResult
    stats_path: Path
