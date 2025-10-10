from __future__ import annotations

import asyncio
import json

import google.generativeai as genai

from app.config import Settings
from app.models import SummaryRequest, SummaryResponse

PROMPT_TEMPLATE_ANALYSIS = """
# PERAN DAN TUJUAN
Anda adalah seorang AI Content Analyst yang sangat ahli dalam menganalisis, merangkum, dan mengkategorikan konten teks dari transkrip video.

# ATURAN DAN INSTRUKSI
Berdasarkan transkrip lengkap yang diberikan di bawah, ekstrak informasi berikut:
1.  `ringkasan`: Buat ringkasan yang padat dan informatif dari keseluruhan teks. Panjangnya **maksimal 2 paragraf**.
2.  `judul`: Buat satu kalimat judul yang secara akurat **merangkum topik utama yang paling banyak dibahas** dalam video. Hindari menyebut nama orang kecuali nama tersebut adalah fokus utama dari keseluruhan konten. Judul harus bisa berdiri sendiri dan langsung memberi gambaran tentang isi video.
3.  `topik`: Tentukan kategori utama dari konten ini hanya dalam **satu kata**. Contoh: Otomotif, Olahraga, Teknologi, Kesehatan, Pendidikan, Hiburan.

# FORMAT OUTPUT YANG DIHARAPKAN
Sajikan semua output dalam format JSON yang valid dengan key "ringkasan", "judul", dan "topik". Jangan berikan output lain selain objek JSON tersebut.

# TUGAS ANDA
Sekarang, analisis transkrip berikut dan berikan output JSON sesuai format yang telah ditentukan.

# TRANSKRIP LENGKAP:
[PASTE SELURUH TRANSKRIP LENGKAP DI SINI]
""".strip()


class TranscriptSummaryError(Exception):
    """Raised when transcript summarization fails."""


class TranscriptSummaryService:
    """Service that generates transcript summaries using Google Generative AI."""

    def __init__(self, settings: Settings) -> None:
        api_key = (settings.genai_api_key or "").strip()
        if not api_key:
            raise TranscriptSummaryError("GENAI_API_KEY belum dikonfigurasi.")
        genai.configure(api_key=api_key)
        self._model_name = settings.genai_model.strip() or "models/gemini-2.5-pro"

    async def summarize(self, request: SummaryRequest) -> SummaryResponse:
        text = (request.text or "").strip()
        if not text:
            raise TranscriptSummaryError("Teks transkrip tidak boleh kosong.")

        prompt = PROMPT_TEMPLATE_ANALYSIS.replace(
            "[PASTE SELURUH TRANSKRIP LENGKAP DI SINI]", text
        )

        model = genai.GenerativeModel(self._model_name)
        response = await asyncio.to_thread(model.generate_content, prompt)
        content = getattr(response, "text", "") if response else ""
        cleaned = content.strip().replace("```json", "").replace("```", "").strip()
        if not cleaned:
            raise TranscriptSummaryError("Model tidak mengembalikan respons apa pun.")

        try:
            payload = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise TranscriptSummaryError(f"Output model bukan JSON yang valid: {exc}") from exc

        try:
            return SummaryResponse(**payload)
        except TypeError as exc:
            raise TranscriptSummaryError(f"Output model tidak sesuai format: {exc}") from exc
