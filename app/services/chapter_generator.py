from __future__ import annotations

import asyncio
import json
from typing import List

import google.generativeai as genai

from app.config import Settings
from app.models import ChapterItem, ChapterRequest

PROMPT_TEMPLATE_CHAPTERS = """
# PERAN DAN TUJUAN
Anda adalah asisten AI yang ahli dalam menganalisis data transkrip video untuk membuat pembagian bab (chapters) yang logis. Tugas Anda adalah memproses objek JSON yang berisi transkrip lengkap dan segmen-segmennya untuk mengidentifikasi perubahan topik dan membuat bab yang koheren.

# ATURAN DAN FORMAT OUTPUT
1.  Input akan berupa objek JSON yang memiliki dua kunci utama: "text" (transkrip lengkap dalam satu string) dan "segments" (array transkrip per segmen dengan "id", "start", "end", dan "text").
2.  Gunakan array "segments" sebagai acuan utama untuk menentukan waktu mulai setiap bab. Gunakan "text" lengkap untuk membantu memahami konteks keseluruhan.
3.  Identifikasi 3-7 topik utama yang dibahas secara berurutan.
4.  Setiap bab yang Anda buat harus memiliki `start_time` (diambil dari nilai "start" segmen pertama bab tersebut) dan `title` (judul singkat, deskriptif, dan umum, maksimal 5 kata). Hindari menggabungkan dua konsep menjadi satu judul (contoh: "Penutup dan Ajakan"); pilih satu esensi utamanya saja (misalnya: "Penutup").
5.  Pastikan `start_time` diubah ke format "HH:MM:SS".
6.  Berikan output HANYA dalam format array JSON yang valid. Jangan tambahkan teks atau penjelasan lain di luar JSON.

# CONTOH (FEW-SHOT)
## INPUT CONTOH (Sesuai struktur file lengkap Anda):
{
  "text": "5 latihan beban di rumah untuk Lansia. Yang pertama, situs 10. Gerak duduk berdiri dengan kursi dan bisa ditambah dengan keberan. Yang kedua, trunk extension...",
  "segments": [
    { "id": 0, "start": 0.0, "end": 2.36, "text": "5 latihan beban di rumah untuk Lansia."},
    { "id": 1, "start": 3.7, "end": 5.34, "text": "Yang pertama, situs 10."},
    { "id": 2, "start": 6.44, "end": 10.72, "text": "Gerak duduk berdiri dengan kursi dan bisa ditambah dengan keberan."},
    { "id": 3, "start": 12.32, "end": 14.64, "text": "Yang kedua, trunk extension."}
  ]
}

## OUTPUT CONTOH:
[
  {
    "start_time": "00:00:00",
    "title": "Pengenalan Latihan Lansia"
  },
  {
    "start_time": "00:00:03",
    "title": "Latihan 1: Sit to Stand"
  },
  {
    "start_time": "00:00:12",
    "title": "Latihan 2: Trunk Extension"
  }
]

# TUGAS ANDA
Sekarang, proses seluruh objek JSON transkrip berikut dan hasilkan output JSON sesuai format di atas.

## INPUT:
[PASTE SELURUH OBJEK JSON TRANSKRIP ANDA DI SINI]
""".strip()


class ChapterGenerationError(Exception):
    """Raised when chapter generation fails."""


class ChapterGenerationService:
    """Service for generating video chapters using Google Generative AI."""

    def __init__(self, settings: Settings) -> None:
        api_key = (settings.genai_api_key or "").strip()
        if not api_key:
            raise ChapterGenerationError("GENAI_API_KEY belum dikonfigurasi.")
        genai.configure(api_key=api_key)
        self._model_name = settings.genai_model.strip() or "models/gemini-2.5-pro"

    async def generate(self, request: ChapterRequest) -> List[ChapterItem]:
        payload = json.dumps(request.dict(), ensure_ascii=False, indent=2)
        prompt = PROMPT_TEMPLATE_CHAPTERS.replace(
            "[PASTE SELURUH OBJEK JSON TRANSKRIP ANDA DI SINI]", payload
        )

        model = genai.GenerativeModel(self._model_name)
        response = await asyncio.to_thread(model.generate_content, prompt)

        content = getattr(response, "text", "") if response else ""
        cleaned = content.strip().replace("```json", "").replace("```", "").strip()
        if not cleaned:
            raise ChapterGenerationError("Model tidak mengembalikan respons apa pun.")

        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise ChapterGenerationError(f"Output model bukan JSON yang valid: {exc}") from exc

        if not isinstance(parsed, list):
            raise ChapterGenerationError("Output model harus berupa array JSON.")

        chapters: List[ChapterItem] = []
        for item in parsed:
            if not isinstance(item, dict):
                raise ChapterGenerationError("Elemen array output bukan objek JSON.")
            chapters.append(ChapterItem(**item))
        return chapters
