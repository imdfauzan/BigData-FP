# Final Project Big Data 2026
Analisis Real-Time Pelanggaran Jalur Bike Lane Kota Surabaya

## Anggota Kelompok 6
|Nama|NRP|
|---|---|
|Kanafira Vanesha Putri|5027241010|
|Adiwidya Budi Pratama|5027241012|
|Oscaryavat Viryavan|5027241053|
|Theodorus Aaron Ugraha|5027241056|
|Imam Mahmud Dalil Fauzan|5027241100|

## Latar Belakang
Kota Surabaya memiliki tingkat mobilitas yang tinggi seiring dengan meningkatnya jumlah kendaraan bermotor. Tingginya intensitas lalu lintas tersebut juga berkontribusi terhadap tingginya angka kecelakaan di jalan raya. Berdasarkan data Polrestabes Surabaya, sepanjang tahun 2024 terjadi 1.488 kasus kecelakaan lalu lintas di Kota Surabaya, 184 diantaranya meninggal dunia. Jumlah ini meningkat sekitar 5% dibandingkan tahun 2023.

Sebagai upaya mendorong transportasi ramah lingkungan dan meningkatkan keselamatan pesepeda, Pemkot Surabaya telah menyediakan jalur sepeda di beberapa ruas jalan. Namun, pelanggaran berupa kendaraan yang menggunakan jalur sepeda sering ditemukan. Kondisi tersebut dapat mengurangi keamanan pesepeda serta menurunkan efektivitas fungsi jalur sepeda. Oleh karena itu, diperlukan sistem pemantauan yang mendeteksi pelanggaran penyerobotan jalur sepeda secara real-time.

## Deskripsi Proyek 
Fokus utama dari sistem ini adalah membangun jaringan pipeline data yang mampu mendeteksi, memproses, dan menganalisis pelanggaran lalu lintas di Jalur Sepeda (Bike Lane / Non-Motorized Transport) Kota Surabaya secara real-time.

## Desain Infrastruktur dan Flow Pipeline

```
[ INGESTION LAYER ]              [ STREAMING LAYER ]            [ STORAGE LAYER ]           [ SERVING LAYER ]
 ┌────────────────┐               ┌────────────────┐             ┌───────────────┐           ┌──────────────┐
 │  CCTV Stream   │               │                │             │  Delta Lake   │           │              │
 │  (HLS / RTSP)  │               │  Apache Kafka  │             │   on MinIO    │           │ Flask Stream │
 └───────┬────────┘               │(Message Broker)│             │  (Lakehouse)  │           │  Dashboard   │
         │                        └───────▲────────┘             └───────▲───────┘           └───────▲──────┘
         ▼ (OpenCV Frame Read)            │                              │                           │
 ┌────────────────┐                       │ (Publish JSON)               │ (Write Structured)        │ (SQL Query)
 │ YOLOv8 Medium  │───────────────────────┘                              │                           │
 │+ Spatial Filter│                                                      │                           │
 └────────────────┘                                              ┌───────┴───────┐                   │
                                                                 │ Apache Spark  │───────────────────┘
                                                                 │ (Streaming)   │
                                                                 └───────────────┘
```

### Alasan Pemilihan dan Justifikasi Teknis
1. Data Ingestion: OpenCV + YOLOv8 Medium + Threaded Worker

- YOLOv8 varian Medium dipilih karena memiliki trade-off terbaik antara kecepatan (FPS tinggi) dan akurasi untuk mendeteksi kendaraan beresolusi rendah pada CCTV kota.
2. Message Broker / Buffering: Apache Kafka

- Kafka dipilih karena memiliki kemampuan throughput yang sangat tinggi dan tingkat fault tolerance yang tinggi. Fungsi kafka sebagai penahan beban ketika terjadi lonjakan data pelanggaran massal di jam sibuk, mendistribusikan beban ke 3 partisi log, sehingga mencegah hilangnya data (data loss) sebelum diproses oleh Spark.

3. Stream Processing Engine: Apache Spark Structured Streaming

- Spark dipilih karena menggunakan arsitektur In-Memory Processing, yang melakukan transformasi data langsung di dalam memori RAM tanpa melalui I/O disk. Fungsi watermarking pada Spark Structured Streaming menangani data yang terlambat masuk akibat gangguan jaringan CCTV.

4. Storage Layer: Data Lakehouse (Delta Lake + MinIO)

- Memadukan efisiensi penyimpanan skala besar dari Object Storage (MinIO S3-compatible). Delta Lake memisahkan struktur data menggunakan **Medallion Architecture** (Bronze $\rightarrow$ Silver $\rightarrow$ Gold), memiliki fitur **ACID** serta **Schema Enforcement** untuk memastikan tidak ada data *corrupt* yang merusak visualisasi dashboard.

5. Data Serving Layer: Flask Web Server (Server-Sent Events) + Chart.js

- Dashboard dibangun menggunakan Flask karena performanya ringan untuk melakukan query SQL ke Gold Layer Delta Lake. Dengan memanfaatkan Server-Sent Events (SSE), dashboard dapat menampilkan seluruh data secara real-time ke browser tanpa perlu refresh web secara manual.


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
├── cctv_polygon_configs.json       # hasil json config polygon ROI
├── dashboard/                      # dashboard & live-feed
    ├── templates/
    ├── static/
    ├── dashboard.py                # start dashboard
├── docker-compose.yml              # kafka & minio
├── requirements.txt
├── traffic_monitor.py              # menjalankan sistem cctv, YOLO, mengirim data ke kafka
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
Sistem ini dirancang agar dapat memisahkan beban kerja berat (AI Inference) dengan beban kerja pemrosesan data (Stream Processing) di sisi tengah.
```
[ INGESTION LAYER ]              [ STREAMING LAYER ]            [ STORAGE LAYER ]           [ SERVING LAYER ]
 ┌────────────────┐               ┌────────────────┐             ┌───────────────┐           ┌──────────────┐
 │  CCTV Stream   │               │                │             │  Delta Lake   │           │              │
 │  (HLS / RTSP)  │               │  Apache Kafka  │             │   on MinIO    │           │ Flask Stream │
 └───────┬────────┘               │(Message Broker)│             │  (Lakehouse)  │           │  Dashboard   │
         │                        └───────▲────────┘             └───────▲───────┘           └───────▲──────┘
         ▼ (OpenCV Frame Read)            │                              │                           │
 ┌────────────────┐                       │ (Publish JSON)               │ (Write Structured)        │ (SQL Query)
 │ YOLOv8 Medium  │───────────────────────┘                              │                           │
 │+ Spatial Filter│                                                      │                           │
 └────────────────┘                                              ┌───────┴───────┐                   │
                                                                 │ Apache Spark  │───────────────────┘
                                                                 │ (Streaming)   │
                                                                 └───────────────┘
```

## Detail Komponen tiap Pipeline Flow

### A. Data Ingestion Layer (Hulu / Edge Processing)
- Komponen ini mengambil live CCTV dan melakukan penyaringan awal objek.
- Komponen Utama: Python, OpenCV, Ultralytics YOLOv8 (Varian Medium).
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


## Kriteria Berdasarkan Rubrik Penilaian
### Latar Belakang
Kota Surabaya memiliki tingkat mobilitas yang tinggi seiring dengan meningkatnya jumlah kendaraan bermotor. Tingginya intensitas lalu lintas tersebut juga berkontribusi terhadap tingginya angka kecelakaan di jalan raya. Berdasarkan data Polrestabes Surabaya, sepanjang tahun 2024 terjadi 1.488 kasus kecelakaan lalu lintas di Kota Surabaya, 184 diantaranya meninggal dunia. Jumlah ini meningkat sekitar 5% dibandingkan tahun 2023.

Sebagai upaya mendorong transportasi ramah lingkungan dan meningkatkan keselamatan pesepeda, Pemkot Surabaya telah menyediakan jalur sepeda di beberapa ruas jalan. Namun, pelanggaran berupa kendaraan yang menggunakan jalur sepeda sering ditemukan. Kondisi tersebut dapat mengurangi keamanan pesepeda serta menurunkan efektivitas fungsi jalur sepeda. Oleh karena itu, diperlukan sistem pemantauan yang mendeteksi pelanggaran penyerobotan jalur sepeda secara real-time.

### Kerangka 5V

| Dimensi | Justifikasi Teknis Pada Proyek |
|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Volume | Sistem menangani stream video berkecepatan 25-30 fps dari beberapa CCTV SITS. Jika dikonversi menjadi metadata koordinat objek, sistem mengelola jutaan baris log event data per minggu pada Bronze Layer. |
| Velocity | Penyerobotan jalur sepeda terjadi dalam hitungan detik. Pipeline data harus dirancang dengan arsitektur low-latency menggunakan Apache Kafka dan Spark Structured Streaming agar pemrosesan data dari awal-akhir selesai dalam waktu < 2 detik. |
| Variety | Data yang dikelola bersifat multi-struktur. Dimulai dari data tidak terstruktur berupa live video stream (protokol HLS/RTSP), diekstrak menjadi data semi-terstruktur berupa muatan payload JSON koordinat bounding box, hingga disimpan menjadi data terstruktur (tabel relasional Delta Lake). |
| Veracity | Validitas data dijamin dengan menyertakan Confidence Score dari model inferensi YOLOv8 (Medium variant). Untuk menyaring data palsu (False Positive) akibat bayangan, terdapat algoritma Spatial Intersection (cv2.pointPolygonTest) dikombinasikan dengan toleransi cooldown 2 detik. |
| Value | Mengubah log mentah menjadi informasi Bike Lane Violation Index (BLVI) per ruas jalan. Hasil ini memberikan nilai prediktif bagi Dishub untuk mengerahkan personel patroli dan memberikan rekomendasi lokasi pemasangan barier. |

### Infrastruktur Big Data

#### Bagaimana Sistem Mendeteksi Pelanggaran
1. YOLO menghasilkan Bounding Box

Setiap frame dari CCTV diproses YOLOv8 yang menghasilkan kotak (bounding box) di sekeliling setiap kendaraan yang terdeteksi, beserta label kelas (car, motorcycle, dll.) dan confidence score (0–1).

2. Area Jalur Sepeda

Jalur sepeda sebelumnya sudah didefinisikan sebagai area dalam polygon, berdasarkan koordinat piksel yang digambar manual di atas snapshot CCTV. Fungsi `cv2.pointPolygonTest()` menggunakan algoritma ray casting.

3. Filter Arah Berlawanan

Sebelum cek jalur sepeda, koordinat roda kendaraan dicek terlebih dahulu apakah ada di dalam `road_roi_polygon` (area jalan satu arah). Kendaraan dari arah berlawanan yang secara visual "terlihat" masuk jalur sepeda sebenarnya tidak relevan, dan dibuang di tahap ini.

4. Cara membedakan Kendaraan yang hanya melintas di Jalur Sepeda

Menggunakan mekanisme Cooldown time (2 detik). Satu event Kafka hanya dikirim maksimal sekali setiap 2 detik per kamera. Kendaraan yang hanya selintas melewati garis polygon (< 1 frame yang diproses) tidak akan men-trigger pengiriman data.

Sederhananya:

| Skenario | Durasi dalam Polygon | Dikirim ke Kafka? |
| --- | --- | --- |
| Kendaraan berbelok, memotong jalur sepeda | < 1 detik | ❌ Tidak |
| Kendaraan menyalip kendaraan lain  | 1–2 detik | ⚠️ Tergantung cooldown |
| Kendaraan berjalan di jalur sepeda | > 2 detik | ✅ Ya, terdeteksi |

### Analisis Gap & Kompetitor

| Fitur / Parameter | ETLE | Aplikasi SITS Surabaya (Eksisting) | Sistem Kami (Bike Lane Command Center) |
|-------------------|---------------------------------------------------|-------------------------------------|-----------------------------------------------------|
| Sifat Sistem | Transaksional Law Enforcement (Fokus pada tindakan hukum individu pelanggar). | Monitoring Pasif (Hanya menampilkan live feed video untuk dipantau manual oleh petugas). | Analitis (Lakehouse) untuk analisis lebih lanjut dan prediksi. |
| Objek Fokus | Pelanggaran marka jalan, lampu merah, penggunaan helm, dan plat nomor. | Live view kondisi jalan secara umum tanpa klasifikasi objek otomatis. | Spesifik pada Jalur Non-Motorized Transport (NMT) yaitu jalur sepeda. |
| Skalabilitas Data | Terbatas pada titik kamera ETLE khusus yang berbiaya sangat tinggi. | Menyimpan rekaman video mentah (raw) yang memakan kapasitas penyimpanan besar. | Sangat efisien karena hanya menyimpan metadata JSON ke dalam arsitektur Medallion Lakehouse. |
| Output Kebijakan | Surat tilang dan denda administratif. | Informasi kemacetan di media sosial Dishub. | Bike Lane Violation Index (BLVI) dan Heatmap Jam Rawan untuk perencanaan infrastruktur kota. |

## Desain Infrastruktur dan Flow Pipeline

```
[ INGESTION LAYER ]              [ STREAMING LAYER ]            [ STORAGE LAYER ]           [ SERVING LAYER ]
 ┌────────────────┐               ┌────────────────┐             ┌───────────────┐           ┌──────────────┐
 │  CCTV Stream   │               │                │             │  Delta Lake   │           │              │
 │  (HLS / RTSP)  │               │  Apache Kafka  │             │   on MinIO    │           │ Flask Stream │
 └───────┬────────┘               │(Message Broker)│             │  (Lakehouse)  │           │  Dashboard   │
         │                        └───────▲────────┘             └───────▲───────┘           └───────▲──────┘
         ▼ (OpenCV Frame Read)            │                              │                           │
 ┌────────────────┐                       │ (Publish JSON)               │ (Write Structured)        │ (SQL Query)
 │ YOLOv8 Medium  │───────────────────────┘                              │                           │
 │+ Spatial Filter│                                                      │                           │
 └────────────────┘                                              ┌───────┴───────┐                   │
                                                                 │ Apache Spark  │───────────────────┘
                                                                 │ (Streaming)   │
                                                                 └───────────────┘
```

### Alasan Pemilihan dan Justifikasi Teknis
1. Data Ingestion: OpenCV + YOLOv8 Medium + Threaded Worker

- YOLOv8 varian Medium dipilih karena memiliki trade-off terbaik antara kecepatan (FPS tinggi) dan akurasi untuk mendeteksi kendaraan beresolusi rendah pada CCTV kota.
2. Message Broker / Buffering: Apache Kafka

- Kafka dipilih karena memiliki kemampuan throughput yang sangat tinggi dan tingkat fault tolerance yang tinggi. Fungsi kafka sebagai penahan beban ketika terjadi lonjakan data pelanggaran massal di jam sibuk, mendistribusikan beban ke 3 partisi log, sehingga mencegah hilangnya data (data loss) sebelum diproses oleh Spark.

3. Stream Processing Engine: Apache Spark Structured Streaming

- Spark dipilih karena menggunakan arsitektur In-Memory Processing, yang melakukan transformasi data langsung di dalam memori RAM tanpa melalui I/O disk. Fungsi watermarking pada Spark Structured Streaming menangani data yang terlambat masuk akibat gangguan jaringan CCTV.

4. Storage Layer: Data Lakehouse (Delta Lake + MinIO)

- Memadukan efisiensi penyimpanan skala besar dari Object Storage (MinIO S3-compatible). Delta Lake memisahkan struktur data menggunakan **Medallion Architecture** (Bronze $\rightarrow$ Silver $\rightarrow$ Gold), memiliki fitur **ACID** serta **Schema Enforcement** untuk memastikan tidak ada data *corrupt* yang merusak visualisasi dashboard.

5. Data Serving Layer: Flask Web Server (Server-Sent Events) + Chart.js

- Dashboard dibangun menggunakan Flask karena performanya ringan untuk melakukan query SQL ke Gold Layer Delta Lake. Dengan memanfaatkan Server-Sent Events (SSE), dashboard dapat menampilkan seluruh data secara real-time ke browser tanpa perlu refresh web secara manual.

### Detail Alur Data End-to-End
1. Tahap 1 (Edge Ingestion):
- Script `traffic_monitor.py` membaca stream CCTV. Setiap frame diolah YOLOv8. Jika titik roda bawah kendaraan berada di dalam `bike_lane_polygon` (diuji dengan `cv2.pointPolygonTest`), status pelanggaran menjadi true. Payload JSON langsung dibuat dan dikirim ke Kafka.

2. Tahap 2 (Stream Buffering):
- Broker Kafka menerima JSON pada topik `surabaya-traffic-bikeline-violations`. Data disimpan sementara dalam *cluster* log terpartisi dengan *retention time* 2 jam.

3. Tahap 3 (Stateful Processing):
- Spark Streaming mengambil data dari Kafka secara mikro-batch. Spark melakukan parsing JSON, membersihkan data duplikat, menghitung Bike Lane Violation Index (BLVI) menggunakan formula pembobotan jenis kendaraan, dan mengelompokkan data berdasarkan *windowing* per 5 menit.

4. Tahap 4 (Lakehouse Multi-Layer Persistence):
- Data mentah masuk ke Bronze Bucket di MinIO dalam format Delta.
- Data bersih hasil validasi tipe kendaraan masuk ke Silver Bucket.
- Data agregat matang (total pelanggaran per jam per lokasi) ditulis ke Gold Bucket dengan partisi berbasis `date` dan `camera_id`.

5. Tahap 5 (Serving Analytics):
- File `dashboard.py` mengeksekusi query SQL ringan ke Gold Table. Hasilnya dilempar ke `index.html` dan langsung dirender oleh `Chart.js`.

### Evaluasi Model YOLO (Object Detection)

| Metrik | Penjelasan |
| :--- | :--- |
| **Precision** | Dari semua yang dilabeli "kendaraan" oleh YOLO, berapa % yang benar? |
| **Recall** | Dari semua kendaraan yang sebenarnya ada, berapa % yang berhasil dideteksi? |
| **mAP@50** | Mean Average Precision pada IoU threshold 0.5 |
| **Confidence Threshold** | Di sistem ini dikalibrasi per kelas: motor 0.20, mobil 0.30 — trade-off precision vs recall |

1. **True Positive** (TP): Sistem deteksi melanggar, memang melanggar
2. **False Positive** (FP): Sistem deteksi melanggar, padahal tidak (misal mobil belok)
3. **False Negative** (FN): Sistem tidak deteksi, padahal melanggar (motor miss)
4. **True Negative** (TN): Sistem tidak deteksi, memang tidak melanggar

**Precision Pelanggaran** = TP / (TP + FP)
**Recall Pelanggaran**    = TP / (TP + FN)

## Sumber
1. [1.488 Kecelakaan Terjadi di Jalanan Surabaya Selama 2024](https://www.jawapos.com/surabaya-raya/2501010263/1488-kecelakaan-terjadi-di-jalanan-surabaya-selama-2024-salah-satunya-gara-gara-pengemudi-mabuk)
2. [51 Orang Tewas dalam Laka Lantas di Surabaya selama 2024](https://jatim.idntimes.com/news/jawa-timur/51-orang-tewas-dalam-laka-lantas-di-surabaya-selama-2024-00-w15v1-tr21kf/amp)
3. [DP3APPKB Surabaya](https://dp3appkb.surabaya.go.id/)