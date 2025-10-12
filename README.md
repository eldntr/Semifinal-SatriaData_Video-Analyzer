python3  process_dataset.py --input "Data Problem Kedua.csv" --output-dir "dataset" --resume

## Video Analysis API

Endpoint baru tersedia untuk analisis video:

- `POST /video-analysis/visual` – unggah video untuk mendapatkan metrik kecerahan dan plot HTML.
- `POST /video-analysis/audio` – unggah video untuk analisis pitch audio dan spectrogram HTML.
- `POST /video-analysis/full` – menjalankan kedua analisis sekaligus dan menghasilkan ringkasan gabungan.
