# Final Project Big Data 2026
Analisis Real-Time Pelanggaran Jalur Bike Lane Kota Surabaya

## Deskripsi Proyek 
Fokus utama dari sistem ini adalah membangun jaringan pipeline data yang mampu mendeteksi, memproses, dan menganalisis pelanggaran lalu lintas di Jalur Sepeda (Bike Lane / Non-Motorized Transport) Kota Surabaya secara real-time.

## 4 Problem Solving
1. **Riil**
Penyerobotan jalur sepeda dan trotoar di Surabaya (seperti di Jl. Raya Darmo atau Jl. Pemuda) banyak dikeluhkan komunitas pesepeda dan sering masuk berita lokal.
2. **Unik**
Berbeda dengan sistem ETLE pemerintah yang sifatnya Transaksional, sistem kami sifatnya Analitis (Lakehouse), yaitu mengumpulkan pola pelanggaran.
3. **Berdampak**
Menghasilkan insight berupa "Hotspot & Jam Rawan Pelanggaran" untuk menentukan di mana barier harus dipasang atau digunakan oleh Dishub untuk efisiensi patroli.
4. **Inovatif**
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
    - Jalankan Kafka broker:
      ```bash
      docker compose up -d
      ```
    - Buat topic (sekali saja, kalau belum ada):
      ```bash
      docker exec -it kafka-lite /opt/kafka/bin/kafka-topics.sh \
        --create --topic surabaya-traffic-bikeline-violations \
        --bootstrap-server localhost:9092 \
        --partitions 3 --replication-factor 1 --config retention.ms=7200000
      ```
    - Spark Structured Streaming: [ON PROGRESS - Orang 3]

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

## Menjalankan Dashboard Real-Time (Flask)
Dashboard ini berfungsi untuk menampilkan analitik dan live-feed CCTV secara real-time.
Untuk menjalankannya:
1. Pastikan `traffic_monitor.py` sedang berjalan di terminal terpisah. (Ini diperlukan untuk mengirimkan log analitik ke Kafka dan menyediakan live video stream di port 5001).
2. Buka terminal baru dan masuk ke direktori dashboard:
   ```bash
   cd dashboard
   ```
3. Install package Flask jika belum: `pip install Flask`
4. Jalankan server dashboard:
   ```bash
   python dashboard.py
   ```
5. Buka browser dan akses: `http://localhost:5000`
---

## Kafka Setup & Handoff (Orang 2 → Orang 3)

### Status
Kafka broker (`kafka-lite`) sudah running via Docker Compose dengan dual listener (host + internal docker network), topic sudah dibuat dan diverifikasi end-to-end dari producer (`traffic_monitor.py`) sampai consumer test.

### Koneksi ke Kafka

| Konteks | Bootstrap Server |
|---|---|
| Dari **host** (script Python via `.venv`, testing) | `localhost:9092` |
| Dari **container lain** di network `bigdata-net` (misal Spark container) | `kafka-lite:29092` |

> Jika service Spark milik Orang 3 dijalankan via docker-compose, pastikan service tersebut ikut bergabung ke network `bigdata-net` agar bisa resolve hostname `kafka-lite`.

### Topic

- Nama topic: `surabaya-traffic-bikeline-violations`
- Partitions: 3
- Replication factor: 1
- Retention: 2 jam (7.200.000 ms)

Cek konfigurasi:
```bash
docker exec -it kafka-lite /opt/kafka/bin/kafka-topics.sh \
  --describe --topic surabaya-traffic-bikeline-violations \
  --bootstrap-server localhost:9092
```

### Skema Payload JSON

```json
{
  "camera_id": "CCTV_BASRA_LOOP",
  "location": "Depan TP1",
  "timestamp": "2026-06-13T02:36:02Z",
  "vehicle_type": "motorcycle",
  "confidence_score": 0.3766
}
```

Field detail:
- `camera_id` (string): salah satu dari `CCTV_BASUKI_RAHMAT`, `CCTV_BAMBU_RUNCING`, `CCTV_BASRA_LOOP`, `CCTV_DARMO_MERCURE`.
- `location` (string): nama lokasi human-readable.
- `timestamp` (string, ISO 8601 UTC, format `%Y-%m-%dT%H:%M:%SZ`): waktu deteksi.
- `vehicle_type` (string): salah satu dari `car`, `motorcycle`, `bus`, `truck`.
- `confidence_score` (float, 4 desimal): confidence score deteksi YOLO.

Catatan untuk windowing/aggregation Spark:
- Field `timestamp` sudah dalam UTC ISO 8601, cocok dipakai langsung sebagai event-time column untuk `withWatermark` / window aggregation.
- Tidak ada `id` unik per event — jika butuh dedup, bisa kombinasikan `camera_id` + `timestamp` + `vehicle_type`.

### Cara Test Konsumsi Data (PySpark snippet starter)

```python
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-lite:29092")  # dari dalam container
    .option("subscribe", "surabaya-traffic-bikeline-violations")
    .option("startingOffsets", "earliest")
    .load()
)
```

Value-nya berupa bytes JSON — perlu di-parse dengan schema sesuai field di atas menggunakan `from_json`.

### Validasi yang Sudah Dilakukan

1. Kafka broker berhasil start tanpa error listener.
2. Topic berhasil dibuat dengan konfigurasi di atas.
3. `traffic_monitor.py` (producer) berhasil konek ke `localhost:9092` dan mengirim payload setiap ada pelanggaran (log: `✅ Kafka Producer terhubung`).
4. `consumer_test.py` berhasil menerima dan menampilkan payload JSON secara real-time dari topic yang sama.

### File Terkait
- `docker-compose.yml` — definisi service Kafka (+ skeleton service Spark)
- `consumer_test.py` — script verifikasi consumer

## Key Configuration References

**MinIO:**
- Endpoint: http://localhost:9001
- Access Key: minioadmin
- Secret Key: minioadmin123
- Buckets: bronze, silver, gold

**Kafka:**
- Bootstrap: localhost:9092
- Topic: surabaya-traffic-bikeline-violations
- Partitions: 3

**Spark:**
- Master: local[4]
- S3A Endpoint: http://minio-storage:9000
- Delta Lake Checkpoints: s3a://lakehouse/checkpoints/

**Python Environment:**
- Location: ~/.venv/
- Python: 3.12.3
- Packages: kafka-python, delta-spark, pyspark
