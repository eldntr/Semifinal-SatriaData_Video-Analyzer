# -----------------------------------------------------------------------------
# KODE LENGKAP UNTUK MENGUNDUH VIDEO DARI BERBAGAI SUMBER (DIPERBAIKI)
# -----------------------------------------------------------------------------

import pandas as pd
import requests
import re
import os
from pytube import YouTube
import yt_dlp

# --- FUNGSI-FUNGSI PEMBANTU (Tidak ada perubahan) ---

def get_link_type(url):
    """Mengidentifikasi tipe link: Google Drive, YouTube, atau Instagram."""
    if not isinstance(url, str): return "unknown"
    if "drive.google.com" in url: return "gdrive"
    elif "youtube.com" in url or "youtu.be" in url: return "youtube"
    elif "instagram.com" in url: return "instagram"
    # Tambahan untuk mengenali URL CDN Instagram secara langsung
    elif "cdninstagram.com" in url: return "instagram"
    else: return "unknown"

def convert_drive_link(url):
    """Mengonversi link Google Drive 'view' ke format 'uc?id=' untuk diunduh."""
    match = re.search(r'/d/([a-zA-Z0-9_-]+)', url)
    if match: return f"https://drive.google.com/uc?id={match.group(1)}"
    match_export = re.search(r'id=([a-zA-Z0-9_-]+)', url)
    if match_export: return f"https://drive.google.com/uc?id={match_export.group(1)}"
    return url

def download_gdrive_requests(url, output_path):
    """Alternatif gdown menggunakan requests untuk mengunduh dari Google Drive."""
    try:
        session = requests.Session()
        response = session.get(url, stream=True)
        token = None
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                token = value
                break
        if token:
            url_with_token = url + "&confirm=" + token
            response = session.get(url_with_token, stream=True)
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(8192):
                if chunk: f.write(chunk)
        return True, None
    except Exception as e:
        return False, str(e)

# --- FUNGSI UTAMA PENGUNDUH ---

def download_video(url, output_path, cookie_file_path):
    """Mengunduh video dari URL yang diberikan menggunakan metode yang sesuai."""
    link_type = get_link_type(url)
    try:
        if link_type == "gdrive":
            clean_url = convert_drive_link(url)
            print(f"Mencoba mengunduh dari Google Drive (via requests): {clean_url}")
            success, error_msg = download_gdrive_requests(clean_url, output_path)
            if success:
                print(f"✅ Berhasil diunduh: {output_path}")
            else:
                print(f"❌ Gagal mengunduh {clean_url}. Alasan: {error_msg}")

        elif link_type == "youtube":
            print(f"Mencoba mengunduh dari YouTube: {url}")
            yt = YouTube(url)
            stream = yt.streams.get_highest_resolution()
            stream.download(filename=output_path)
            print(f"✅ Berhasil diunduh: {output_path}")

        elif link_type == "instagram":
            print(f"Mencoba mengunduh dari Instagram dengan yt-dlp: {url}")
            if not os.path.exists(cookie_file_path):
                print(f"❌ KESALAHAN: File cookie tidak ditemukan di: {cookie_file_path}")
                print("Unduhan dari Instagram tidak dapat dilanjutkan tanpa cookie.")
                return
            
            ydl_opts = {
                'format': 'best',
                'outtmpl': output_path,
                'quiet': True,
                'noplaylist': True,
                'cookiefile': cookie_file_path,
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
            print(f"✅ Berhasil diunduh: {output_path}")

        else:
            print(f"⚠️ Tautan tidak dikenali atau tidak didukung: {url}")

    except Exception as e:
        print(f"❌ Gagal mengunduh {url}. Alasan: {e}")

# --- PENGGUNAAN SCRIPT (BAGIAN INI YANG DIMODIFIKASI) ---

# 1. ⚙️ SESUAIKAN PENGATURAN DI BAWAH INI
file_to_process = '/app/data/satria-data/datatest_revisi.csv'
output_folder = '/app/data/satria-data/test_revisi'
# PENTING: Pastikan path ke file cookies.txt Anda sudah benar
cookie_file = '/app/src/get-data/cookies.txt' 

# 2. Buat folder untuk menyimpan video jika belum ada
if not os.path.exists(output_folder):
    os.makedirs(output_folder)
    print(f"Folder '{output_folder}' berhasil dibuat.")

# 3. Baca file CSV
try:
    # Menambahkan parameter `on_bad_lines='skip'` untuk melewati baris yang error jika ada
    df = pd.read_csv(file_to_process, on_bad_lines='skip')
    print(f"Membaca {file_to_process}. Jumlah total baris: {len(df)}")

    # 4. Iterasi setiap baris dan unduh video
    for index, row in df.iterrows():
        video_id = row['id']
        raw_video_url = row['video']

        # --- PERBAIKAN DIMULAI DI SINI ---
        
        # Cek jika URL kosong/NaN
        if pd.isna(raw_video_url):
            print(f"⚠️ Melewati ID {video_id} karena URL kosong.")
            continue # Lanjut ke baris berikutnya

        # Ubah ke string dan ambil hanya URL pertama jika ada koma
        cleaned_url = str(raw_video_url).split(',')[0].strip()
        
        # --- AKHIR DARI PERBAIKAN ---

        output_filename = os.path.join(output_folder, f"{video_id}.mp4")

        if not os.path.exists(output_filename):
            print(f"\nMemproses ID: {video_id}...")
            # Gunakan URL yang sudah dibersihkan
            download_video(cleaned_url, output_filename, cookie_file)
        else:
            print(f"File untuk ID {video_id} sudah ada, dilewati.")

except FileNotFoundError:
    print(f"❌ ERROR: File CSV '{file_to_process}' tidak ditemukan.")
except Exception as e:
    print(f"❌ Terjadi error saat memproses file: {e}")

print("\n--- Proses Selesai ---")