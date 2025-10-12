from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Dict, Optional, Tuple

import cv2
import librosa
import numpy as np
import plotly.graph_objects as go
from fastapi import UploadFile

from app.video_analysis.exceptions import AudioAnalysisError, VideoAnalysisError, VisualAnalysisError
from app.video_analysis.storage import VideoAnalysisStorage, VideoAnalysisWorkspace
from app.video_analysis.types import (
    AudioAnalysisResult,
    CombinedAnalysisResult,
    VisualAnalysisResult,
)


class VideoAnalysisService:
    _CHUNK_SIZE = 1024 * 1024
    _FFMPEG_ENV_VAR = "FFMPEG_PATH"

    def __init__(self, storage: VideoAnalysisStorage) -> None:
        self._storage = storage
        self._ffmpeg_path: Optional[str] = self._resolve_ffmpeg_path()

    async def analyze_visual(
        self,
        upload: UploadFile,
        video_id: Optional[str] = None,
    ) -> VisualAnalysisResult:
        workspace = self._storage.create_workspace(video_id, upload.filename)
        await self._save_upload(upload, workspace.video_path)
        try:
            result, _ = await asyncio.to_thread(
                self._perform_visual_analysis,
                workspace,
                workspace.visual_stats_path,
            )
            return result
        finally:
            self._storage.cleanup_temp_audio(workspace)

    async def analyze_audio(
        self,
        upload: UploadFile,
        video_id: Optional[str] = None,
    ) -> AudioAnalysisResult:
        workspace = self._storage.create_workspace(video_id, upload.filename)
        await self._save_upload(upload, workspace.video_path)
        try:
            result, _ = await asyncio.to_thread(
                self._perform_audio_analysis,
                workspace,
                workspace.audio_stats_path,
            )
            return result
        finally:
            self._storage.cleanup_temp_audio(workspace)

    async def analyze_full(
        self,
        upload: UploadFile,
        video_id: Optional[str] = None,
    ) -> CombinedAnalysisResult:
        workspace = self._storage.create_workspace(video_id, upload.filename)
        await self._save_upload(upload, workspace.video_path)
        try:
            visual_result, visual_stats = await asyncio.to_thread(
                self._perform_visual_analysis,
                workspace,
                None,
            )
            audio_result, audio_stats = await asyncio.to_thread(
                self._perform_audio_analysis,
                workspace,
                None,
            )
            combined_stats = {"visual": visual_stats, "audio": audio_stats}
            self._write_json(workspace.combined_stats_path, combined_stats)

            combined_visual = VisualAnalysisResult(
                analysis_id=visual_result.analysis_id,
                average_brightness=visual_result.average_brightness,
                std_dev_brightness=visual_result.std_dev_brightness,
                scene_cut_timestamps=visual_result.scene_cut_timestamps,
                brightness_plot_html=visual_result.brightness_plot_html,
                stats_path=workspace.combined_stats_path,
            )
            combined_audio = AudioAnalysisResult(
                analysis_id=audio_result.analysis_id,
                average_pitch_hz=audio_result.average_pitch_hz,
                std_dev_pitch_hz=audio_result.std_dev_pitch_hz,
                spectrogram_plot_html=audio_result.spectrogram_plot_html,
                stats_path=workspace.combined_stats_path,
            )
            return CombinedAnalysisResult(
                analysis_id=workspace.identifier,
                visual=combined_visual,
                audio=combined_audio,
                stats_path=workspace.combined_stats_path,
            )
        finally:
            self._storage.cleanup_temp_audio(workspace)

    async def _save_upload(self, upload: UploadFile, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            with destination.open("wb") as buffer:
                while True:
                    chunk = await upload.read(self._CHUNK_SIZE)
                    if not chunk:
                        break
                    buffer.write(chunk)
        finally:
            await upload.close()

    def _perform_visual_analysis(
        self,
        workspace: VideoAnalysisWorkspace,
        stats_path: Optional[Path],
    ) -> Tuple[VisualAnalysisResult, Dict[str, object]]:
        metrics = self._compute_visual_metrics(workspace.video_path)
        plot_html = self._create_brightness_plot(
            metrics["timestamps"],
            metrics["brightness_scores"],
            workspace.identifier,
        )

        visual_stats = {
            "rata_rata_kecerahan": round(metrics["average_brightness"], 2),
            "std_dev_kecerahan": round(metrics["std_dev_brightness"], 2),
            "total_pergantian_adegan": len(metrics["scene_cuts"]),
            "waktu_pergantian_adegan": metrics["scene_cuts"],
        }
        if stats_path:
            self._write_json(stats_path, {"visual": visual_stats})

        result = VisualAnalysisResult(
            analysis_id=workspace.identifier,
            average_brightness=float(visual_stats["rata_rata_kecerahan"]),
            std_dev_brightness=float(visual_stats["std_dev_kecerahan"]),
            scene_cut_timestamps=metrics["scene_cuts"],
            brightness_plot_html=plot_html,
            stats_path=stats_path,
        )
        return result, visual_stats

    def _perform_audio_analysis(
        self,
        workspace: VideoAnalysisWorkspace,
        stats_path: Optional[Path],
    ) -> Tuple[AudioAnalysisResult, Dict[str, object]]:
        metrics = self._compute_audio_metrics(workspace)
        plot_html = self._create_spectrogram_plot(
            metrics["spectrogram_db"],
            metrics["times"],
            metrics["frequencies"],
            workspace.identifier,
        )

        audio_stats = {
            "rata_rata_pitch_hz": round(metrics["average_pitch_hz"], 2),
            "std_dev_pitch_hz": round(metrics["std_dev_pitch_hz"], 2),
        }
        if stats_path:
            self._write_json(stats_path, {"audio": audio_stats})

        result = AudioAnalysisResult(
            analysis_id=workspace.identifier,
            average_pitch_hz=float(audio_stats["rata_rata_pitch_hz"]),
            std_dev_pitch_hz=float(audio_stats["std_dev_pitch_hz"]),
            spectrogram_plot_html=plot_html,
            stats_path=stats_path,
        )
        return result, audio_stats

    @staticmethod
    def _compute_visual_metrics(video_path: Path) -> Dict[str, object]:
        capture = cv2.VideoCapture(str(video_path))
        if not capture.isOpened():
            raise VisualAnalysisError(f"Tidak dapat membuka video '{video_path.name}'.")

        fps = capture.get(cv2.CAP_PROP_FPS) or 0.0
        fps = fps if fps > 0 else 1.0
        frame_count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))

        timestamps = []
        brightness_scores = []
        scene_cuts = []

        ret, prev_frame = capture.read()
        if not ret or prev_frame is None:
            capture.release()
            raise VisualAnalysisError("Tidak dapat membaca frame pertama video.")

        timestamps.append(0.0)
        prev_gray = cv2.cvtColor(prev_frame, cv2.COLOR_BGR2GRAY)
        brightness_scores.append(float(np.mean(prev_gray)))
        prev_hist = cv2.calcHist([prev_frame], [0, 1, 2], None, [8, 8, 8], [0, 256, 0, 256, 0, 256])
        cv2.normalize(prev_hist, prev_hist, alpha=0, beta=1, norm_type=cv2.NORM_MINMAX)

        for frame_num in range(1, frame_count):
            ret, frame = capture.read()
            if not ret or frame is None:
                break
            timestamp = frame_num / fps
            timestamps.append(float(timestamp))
            gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            brightness_scores.append(float(np.mean(gray_frame)))

            current_hist = cv2.calcHist([frame], [0, 1, 2], None, [8, 8, 8], [0, 256, 0, 256, 0, 256])
            cv2.normalize(current_hist, current_hist, alpha=0, beta=1, norm_type=cv2.NORM_MINMAX)

            hist_diff = cv2.compareHist(prev_hist, current_hist, cv2.HISTCMP_CORREL)
            if hist_diff < 0.6:
                scene_cuts.append(float(timestamp))
            prev_hist = current_hist

        capture.release()

        if brightness_scores:
            brightness_array = np.array(brightness_scores, dtype=np.float32)
            average_brightness = float(np.mean(brightness_array))
            std_dev_brightness = float(np.std(brightness_array))
        else:
            average_brightness = 0.0
            std_dev_brightness = 0.0

        return {
            "timestamps": timestamps,
            "brightness_scores": brightness_scores,
            "average_brightness": average_brightness,
            "std_dev_brightness": std_dev_brightness,
            "scene_cuts": scene_cuts,
        }

    def _compute_audio_metrics(self, workspace: VideoAnalysisWorkspace) -> Dict[str, object]:
        self._extract_audio(workspace.video_path, workspace.audio_path)
        try:
            y, sr = librosa.load(str(workspace.audio_path))
        except Exception as exc:  # pragma: no cover - heavy dependency
            raise AudioAnalysisError(f"Gagal memuat audio dari '{workspace.audio_path.name}': {exc}") from exc

        if y.size == 0:
            raise AudioAnalysisError("File audio kosong setelah ekstraksi.")

        try:
            f0, _, _ = librosa.pyin(
                y,
                fmin=librosa.note_to_hz("C2"),
                fmax=librosa.note_to_hz("C7"),
            )
        except Exception as exc:  # pragma: no cover - heavy dependency
            raise AudioAnalysisError(f"Gagal menghitung pitch menggunakan librosa: {exc}") from exc

        valid_pitches = f0[~np.isnan(f0)] if f0 is not None else np.array([], dtype=np.float32)
        if valid_pitches.size > 0:
            average_pitch = float(np.mean(valid_pitches))
            std_dev_pitch = float(np.std(valid_pitches))
        else:
            average_pitch = 0.0
            std_dev_pitch = 0.0

        stft = librosa.stft(y)
        spectrogram = np.abs(stft)
        spectrogram_db = librosa.amplitude_to_db(spectrogram, ref=np.max)
        times = librosa.frames_to_time(np.arange(spectrogram_db.shape[1]), sr=sr)
        frequencies = librosa.fft_frequencies(sr=sr)

        return {
            "average_pitch_hz": average_pitch,
            "std_dev_pitch_hz": std_dev_pitch,
            "spectrogram_db": spectrogram_db,
            "times": times,
            "frequencies": frequencies,
        }

    @staticmethod
    def _create_brightness_plot(
        timestamps: list[float],
        brightness_scores: list[float],
        video_id: str,
    ) -> str:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=timestamps,
                y=brightness_scores,
                name="Kecerahan",
                line=dict(color="#5090D3", width=2),
            )
        )
        fig.update_layout(
            xaxis_title="Waktu (detik)",
            yaxis_title="Tingkat Kecerahan (0-255)",
            plot_bgcolor="#1E212A",
            paper_bgcolor="#1E212A",
            font=dict(color="#FFFFFF"),
        )
        fig.update_xaxes(gridcolor="rgba(255, 255, 255, 0.1)")
        fig.update_yaxes(gridcolor="rgba(255, 255, 255, 0.1)")
        return fig.to_html(full_html=False, include_plotlyjs="cdn")

    @staticmethod
    def _create_spectrogram_plot(
        spectrogram_db: np.ndarray,
        times: np.ndarray,
        frequencies: np.ndarray,
        video_id: str,
    ) -> str:
        fig = go.Figure()
        fig.add_trace(
            go.Heatmap(
                z=spectrogram_db,
                x=times,
                y=frequencies,
                colorscale="Viridis",
                colorbar=dict(title="Kekuatan (dB)"),
            )
        )
        fig.update_layout(
            xaxis_title="Waktu (detik)",
            yaxis_title="Frekuensi (Hz)",
            plot_bgcolor="#1E212A",
            paper_bgcolor="#1E212A",
            font=dict(color="#FFFFFF"),
        )
        fig.update_xaxes(gridcolor="rgba(255, 255, 255, 0.1)")
        fig.update_yaxes(gridcolor="rgba(255, 255, 255, 0.1)")
        return fig.to_html(full_html=False, include_plotlyjs="cdn")

    def _extract_audio(self, video_path: Path, audio_path: Path) -> None:
        ffmpeg_executable = self._ffmpeg_path or self._resolve_ffmpeg_path()
        if not ffmpeg_executable:
            message = (
                "Perintah 'ffmpeg' tidak ditemukan. "
                "Pastikan FFmpeg terpasang dan dapat diakses melalui PATH, "
                "atau set variabel lingkungan FFMPEG_PATH dengan lokasi executable FFmpeg."
            )
            raise AudioAnalysisError(message)

        command = [
            ffmpeg_executable,
            "-y",
            "-i",
            str(video_path),
            "-vn",
            str(audio_path),
            "-loglevel",
            "error",
        ]
        process = subprocess.run(command, capture_output=True, text=True)
        if process.returncode != 0:
            message = process.stderr.strip() or process.stdout.strip() or "ffmpeg gagal mengekstrak audio."
            raise AudioAnalysisError(message)

    @staticmethod
    def _write_json(path: Path, data: Dict[str, object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=4, ensure_ascii=False)

    def _resolve_ffmpeg_path(self) -> Optional[str]:
        env_value = os.getenv(self._FFMPEG_ENV_VAR)
        if env_value:
            candidate = Path(env_value).expanduser()
            if candidate.exists():
                return str(candidate)
        discovered = shutil.which("ffmpeg")
        if discovered:
            return discovered
        return None
