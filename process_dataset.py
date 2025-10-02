from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import torch
import whisper

from app.config import Settings, get_settings
from app.instagram.client import InstagramClient
from app.instagram.exceptions import InstagramScraperError
from app.instagram.storage import MediaStorage
from app.services.instagram_scraper import InstagramScraperService
from download_utils import download_video, get_link_type


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Proses dataset video: unduh, scrape (Instagram), konversi audio, dan transkripsi."
    )
    parser.add_argument("--input", "-i", type=Path, help="Path CSV berisi kolom id, video, label")
    parser.add_argument("--output-dir", "-o", type=Path, help="Folder dasar penyimpanan hasil")
    parser.add_argument(
        "--cookies",
        "-c",
        type=Path,
        help="Path file cookies Instagram (fallback ke INSTAGRAM_COOKIES_PATH / cookies.txt)",
    )
    parser.add_argument("--id-column", default="id", help="Nama kolom ID (default: id)")
    parser.add_argument("--url-column", default="video", help="Nama kolom URL (default: video)")
    parser.add_argument("--label-column", default="label", help="Nama kolom label (default: label)")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Lewati entri yang sudah lengkap (semua artefak tersedia)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Timpa artefak yang sudah ada (video, audio, transkrip, metadata)",
    )
    return parser.parse_args(argv)


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _serialize(obj: Any) -> Any:
    if is_dataclass(obj):
        return _serialize(asdict(obj))
    if isinstance(obj, dict):
        return {key: _serialize(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_serialize(item) for item in obj]
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj


def _determine_device(settings: Settings) -> str:
    device = settings.whisper_device
    if device == "auto":
        device = "cuda" if torch.cuda.is_available() else "cpu"
    return device


def _load_whisper_model(settings: Settings) -> whisper.Whisper:
    device = _determine_device(settings)
    try:
        return whisper.load_model(settings.whisper_model, device=device)
    except Exception as exc:  # pragma: no cover - heavy dependency
        raise RuntimeError(f"Gagal memuat model Whisper '{settings.whisper_model}': {exc}") from exc


def _transcribe(model: whisper.Whisper, audio_path: Path, settings: Settings) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {"fp16": _determine_device(settings) == "cuda"}
    language = settings.whisper_language
    if language:
        kwargs["language"] = language
    try:
        result = model.transcribe(str(audio_path), **kwargs)
    except Exception as exc:
        raise RuntimeError(f"Whisper gagal mentranskripsi: {exc}") from exc

    segments = []
    for segment in result.get("segments", []):
        segments.append(
            {
                "id": segment.get("id"),
                "start": segment.get("start"),
                "end": segment.get("end"),
                "text": (segment.get("text") or "").strip(),
            }
        )

    return {
        "text": (result.get("text") or "").strip(),
        "language": result.get("language") or language,
        "segments": segments,
    }


def _run_ffmpeg(mp4_path: Path, mp3_path: Path) -> None:
    command = [
        "ffmpeg",
        "-y",
        "-i",
        str(mp4_path),
        "-vn",
        "-acodec",
        "libmp3lame",
        "-ac",
        "2",
        str(mp3_path),
    ]
    process = subprocess.run(command, capture_output=True, text=True)
    if process.returncode != 0:
        raise RuntimeError(
            f"ffmpeg gagal mengonversi {mp4_path.name}: {process.stderr.strip() or process.stdout.strip()}"
        )


class DatasetProcessor:
    def __init__(
        self,
        settings: Settings,
        dataset_root: Path,
        instagram_service: Optional[InstagramScraperService],
        whisper_model: whisper.Whisper,
        cookie_file: Optional[Path] = None,
        overwrite: bool = False,
        resume: bool = False,
    ) -> None:
        self._settings = settings
        self._dataset_root = dataset_root
        self._instagram_service = instagram_service
        self._whisper_model = whisper_model
        self._cookie_file = cookie_file
        self._overwrite = overwrite
        self._resume = resume

    def process_row(self, video_id: str, url: str, label: Optional[str]) -> None:
        if not url:
            print(f"⚠️ Melewati ID {video_id} karena URL kosong.")
            return

        folder = self._dataset_root / video_id
        folder.mkdir(parents=True, exist_ok=True)

        mp4_path = folder / f"{video_id}.mp4"
        mp3_path = folder / f"{video_id}.mp3"
        scrape_path = folder / "scrape.json"
        transcript_path = folder / "transcript.json"
        metadata_path = folder / "metadata.json"

        link_type = get_link_type(url)

        if self._resume and all(
            path.exists()
            for path in (
                mp4_path,
                mp3_path,
                transcript_path,
                metadata_path,
            )
        ):
            if link_type != "instagram" or scrape_path.exists():
                print(f"⏭️  ID {video_id} sudah lengkap, dilewati (mode resume).")
                return

        self._ensure_video(video_id, url, mp4_path)

        if link_type == "instagram":
            self._ensure_instagram_scrape(url, scrape_path)

        self._ensure_audio(mp4_path, mp3_path)
        self._ensure_transcript(mp3_path, transcript_path)
        self._write_metadata(metadata_path, video_id, url, label, link_type)

    def _ensure_video(self, video_id: str, url: str, destination: Path) -> None:
        if destination.exists() and not self._overwrite:
            print(f"Video untuk ID {video_id} sudah ada, dilewati download.")
            return

        print(f"  ➤ Mengunduh video {video_id}...")
        try:
            download_video(url, destination, cookie_file_path=self._cookie_file)
        except FileNotFoundError as missing_cookie:
            raise RuntimeError(f"Cookie Instagram tidak ditemukan: {missing_cookie}") from missing_cookie
        except Exception as exc:
            raise RuntimeError(f"Gagal mengunduh video {video_id}: {exc}") from exc

    def _ensure_instagram_scrape(self, url: str, scrape_path: Path) -> None:
        if scrape_path.exists() and not self._overwrite:
            return
        if self._instagram_service is None:
            print("⚠️ Layanan Instagram tidak tersedia; melewati scrape.")
            return

        print("  ➤ Mengambil metadata Instagram...")
        try:
            result = asyncio.run(self._instagram_service.scrape(url, download_video=False))
        except InstagramScraperError as exc:
            print(f"⚠️ Gagal scrape Instagram: {exc}")
            return

        data = _serialize(result)
        with scrape_path.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=2, default=_json_default)

    def _ensure_audio(self, mp4_path: Path, mp3_path: Path) -> None:
        if mp3_path.exists() and not self._overwrite:
            return
        if not mp4_path.exists():
            raise RuntimeError(f"Video sumber {mp4_path} tidak ditemukan untuk konversi audio")

        print("  ➤ Mengonversi ke MP3...")
        _run_ffmpeg(mp4_path, mp3_path)

    def _ensure_transcript(self, mp3_path: Path, transcript_path: Path) -> None:
        if transcript_path.exists() and not self._overwrite:
            return
        if not mp3_path.exists():
            raise RuntimeError(f"Audio sumber {mp3_path} tidak ditemukan untuk transkripsi")

        print("  ➤ Menjalankan transkripsi...")
        result = _transcribe(self._whisper_model, mp3_path, self._settings)
        with transcript_path.open("w", encoding="utf-8") as handle:
            json.dump(result, handle, ensure_ascii=False, indent=2, default=_json_default)

    def _write_metadata(
        self,
        metadata_path: Path,
        video_id: str,
        url: str,
        label: Optional[str],
        link_type: str,
    ) -> None:
        if metadata_path.exists() and not self._overwrite:
            return

        print("  ➤ Menyimpan metadata...")
        payload = {
            "id": video_id,
            "video_url": url,
            "label": label,
            "source": link_type,
        }
        with metadata_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)


def _expand_path(path: Path) -> Path:
    return path.expanduser().resolve()


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    settings = get_settings()

    csv_fallback = Path(os.getenv("BATCH_INPUT_CSV", "Data Problem Kedua.csv"))
    csv_path = _expand_path(args.input) if args.input else _expand_path(csv_fallback)

    dataset_fallback = Path(os.getenv("DATASET_ROOT", "dataset"))
    dataset_root = _expand_path(args.output_dir) if args.output_dir else _expand_path(dataset_fallback)

    if args.cookies:
        cookie_path = _expand_path(args.cookies)
    elif settings.cookies_path is not None:
        cookie_path = settings.cookies_path
    else:
        cookie_env = os.getenv("INSTAGRAM_COOKIES_PATH")
        cookie_path = _expand_path(Path(cookie_env)) if cookie_env else Path("cookies.txt").resolve()

    if cookie_path and not cookie_path.exists():
        print(f"⚠️ Peringatan: file cookies '{cookie_path}' tidak ditemukan. Instagram mungkin gagal diunduh.")

    instagram_service: Optional[InstagramScraperService] = None
    try:
        instagram_service = InstagramScraperService(
            client=InstagramClient(settings),
            storage=MediaStorage(settings),
            settings=settings,
        )
    except Exception as exc:  # pragma: no cover - defensive
        print(f"⚠️ Tidak dapat menginisialisasi layanan Instagram: {exc}")

    whisper_model = _load_whisper_model(settings)

    processor = DatasetProcessor(
        settings=settings,
        dataset_root=dataset_root,
        instagram_service=instagram_service,
        whisper_model=whisper_model,
        cookie_file=cookie_path,
        overwrite=args.overwrite,
        resume=args.resume,
    )

    try:
        df = pd.read_csv(csv_path, on_bad_lines="skip")
    except FileNotFoundError:
        print(f"❌ CSV '{csv_path}' tidak ditemukan.")
        return 1
    except Exception as exc:
        print(f"❌ Tidak dapat membaca CSV '{csv_path}': {exc}")
        return 1

    missing_columns = [col for col in (args.id_column, args.url_column, args.label_column) if col not in df.columns]
    if missing_columns:
        print(f"❌ Kolom {missing_columns} tidak ditemukan di CSV. Kolom tersedia: {list(df.columns)}")
        return 1

    for _, row in df.iterrows():
        video_id = str(row.get(args.id_column)).strip()
        url_value = row.get(args.url_column)
        label = row.get(args.label_column)

        if not video_id or video_id.lower() == "nan":
            print("⚠️ Melewati baris tanpa ID yang valid.")
            continue

        url = str(url_value).strip() if url_value and not pd.isna(url_value) else ""

        print(f"\n=== Memproses ID {video_id} ===")
        try:
            processor.process_row(video_id, url, label)
        except Exception as exc:
            print(f"❌ Terjadi kesalahan pada ID {video_id}: {exc}")

    print("\nSelesai memproses seluruh data.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
