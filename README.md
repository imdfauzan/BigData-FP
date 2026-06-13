# Final Project Big Data 2026
Analisis Real-Time Pelanggaran Jalur Bike Lane Kota Surabaya

## Deskripsi Proyek 
Fokus utama dari sistem ini adalah membangun jaringan pipeline data yang mampu mendeteksi, memproses, dan menganalisis pelanggaran lalu lintas di Jalur Sepeda (Bike Lane / Non-Motorized Transport) Kota Surabaya secara real-time.

## 4 Problem Solving
1. Riil
Penyerobotan jalur sepeda dan trotoar di Surabaya (seperti di Jl. Raya Darmo atau Jl. Pemuda) banyak dikeluhkan komunitas pesepeda dan sering masuk berita lokal.
2. Unik
Berbeda dengan sistem ETLE pemerintah yang sifatnya Transaksional, sistem kami sifatnya Analitis (Lakehouse), yaitu mengumpulkan pola pelanggaran.
3. Berdampak
Menghasilkan insight berupa "Hotspot & Jam Rawan Pelanggaran" untuk menentukan di mana barier harus dipasang atau digunakan oleh Dishub untuk efisiensi patroli.
4. Inovatif
Menggabungkan Computer Vision & YOLO di awal, Stream Processing di tengah, dan Data Lakehouse (Medallion Architecture) di akhir.

## Project Structure
```
BigData-FP/
├── CCTV-YOLO.ipynb                 # config cctv, ubah ROI
├── cctv_polygon_configs.json       # hasil json config ROI
├── docker-compose.yml
├── requirements.txt
├── traffic_monitor.py              # menjalankan sistem
└── snapshots/                      # temporary foto untuk melakukan ROI
    ├── CCTV_A.jpg
    ├── CCTV_B.jpg
    ├── CCTV_C.jpg
    ├── dst.
```

## Cara Menjalankan
1. Run Live CCTV (Analysis & Ingestion)
    - Buat .venv jika belum `python -m venv .venv`
    - Masuk .venv `.\.venv\Scripts\Activate`
    - Install Dependencies `pip install opencv-python ultralytics kafka-python numpy streamlink`
    ```python
    python traffic_monitor.py
    ```
    - Akan menghasilkan teks JSON yang siap masuk ke Kafka

2. Run Stream Processing (Kafka & Spark)
    - [ON PROGRESS]

## Arsitektur Sistem & Flow Pipeline
Sistem ini dirancang agar dapat memisahkan beban kerja berat (AI Inference) di sisi hulu dengan beban kerja pemrosesan data (Stream Processing) di sisi tengah.
```
[ Ingestion Layer ] ──(JSON Payload)──> [ Streaming Layer ] ──> [ Storage Layer ] ──> [ Serving Layer ]
  - SITS CCTV Stream                      - Apache Kafka          - Data Lakehouse       - Apache Superset
  - OpenCV + YOLOv8                       - Apache Spark            (Delta Lake/MinIO)   - Real-Time Dashboard
  - Spatial ROI Analysis
```

## Detail Komponen tiap Pipeline Flow

### A. Data Ingestion Layer (Hulu / Edge Processing)
- Komponen ini mengambil live CCTV dan melakukan penyaringan awal objek.
- Komponen Utama: Python, OpenCV, Ultralytics YOLOv8 (Varian Nano).
- Data yang Diambil (Input): Live video streaming dari protokol HLS (.m3u8) atau RTSP milik CCTV SITS Surabaya.
- Proses Di Dalam Komponen: 

    - Membaca stream video frame-by-frame dengan frame-skipping untuk meringankan beban CPU.

    - Melakukan inferensi menggunakan YOLOv8.

    - Analisis Spasial (ROI): Menggunakan fungsi `cv2.pointPolygonTest` untuk memfilter, kendaraan di jalur berlawanan diabaikan (`road_roi_polygon`), lalu kendaraan yang titik tengah bawah bodi-nya masuk ke dalam area `bike_lane_polygon` akan dideteksi sebagai pelanggar.

    - Output tahap ini: teks JSON berisi metadata pelanggaran yang dikirim secara real-time tanpa mengirim file videonya.

Contoh Output:

```json
{
  "camera_id": "CCTV_BASRA_LOOP",
  "location": "Depan TP1",
  "timestamp": "2026-06-13T02:36:02Z",
  "vehicle_type": "motorcycle",
  "confidence_score": 0.3766
}
```

### B. Stream Processing Layer (Tengah / Message Broker)
- Komponen ini berfungsi sebagai jembatan penahan beban data (buffering) dan mesin pemroses data deret waktu secara instan.

- Komponen Utama: Apache Kafka, Apache Spark Structured Streaming.

- Data yang Diambil (Input): Aliran JSON payload dari Kafka Topic traffic-violations.

- Proses Di Dalam Komponen:

    - Apache Kafka bertindak sebagai ingestion broker yang mengantrekan ribuan log dari banyak CCTV sekaligus untuk mencegah data loss saat trafik memuncak.

    - Apache Spark Streaming membaca stream dari Kafka secara mikro-bacth, melakukan schema enforcement pada JSON mentah, dan memisahkan kolom waktu untuk keperluan windowing.

    - Melakukan kalkulasi agregasi bergerak (misal: menghitung total pelanggaran per 5 menit berdasarkan tipe kendaraan).

- Output tahap ini: Stream data bersih yang siap ditulis ke dalam tabel Lakehouse secara transaksional.

### C. Storage Layer (Hilir / Data Lakehouse)
- Menyimpan data dengan keandalan sistem basis data relasional (ACID transactions) namun memiliki kapasitas penyimpanan skala besar layaknya Data Lake.

- Komponen Utama: Delta Lake (atau Apache Iceberg) di atas objek storage MinIO / HDFS.

- Data yang Diambil (Input): Data bersih hasil olahan Apache Spark.

- Proses Di Dalam Komponen (Medallion Architecture):

    - Bronze Layer: Menyimpan seluruh raw data log yang masuk dari Kafka untuk kebutuhan audit data historis.

    - Silver Layer: Menyimpan data pelanggaran terfilter yang tipenya sudah divalidasi dan waktu formatnya sudah disamakan.

    - Gold Layer: Menyimpan data agregat final yang sudah di-query siap pakai (contoh: tabel ringkasan `total_pelanggaran_per_jam_per_lokasi`).

- Output tahap ini: Database historis yang efisien, terstruktur, dan sangat cepat saat di-query.

### D. Data Serving Layer (Dashboard & Visualisasi)
- Tahap akhir yang menerjemahkan angka-angka Big Data menjadi wawasan taktis yang mudah dipahami manusia.

- Komponen Utama: Apache Superset.

- Data yang Diambil (Input): Tabel ringkasan dari Gold Layer di Data Lakehouse.

- Proses Di Dalam Komponen: Melakukan pengambilan data berkala (auto-refresh) menggunakan query SQL ringan untuk divisualisasikan ke dalam bentuk grafik chart.

- Output tahap ini: Dashboard analitik interaktif yang menampilkan:
    - Hotspot (titik lokasi) CCTV dengan tingkat pelanggaran tertinggi di Surabaya.
    - Tren jam dan hari rawan penyerobotan jalur sepeda.
    - Proporsi jenis kendaraan yang paling sering melanggar (motor vs mobil vs lainnya).