"""
=============================================================================
Surabaya Smart City – Real-Time Traffic Violation Detection
Edge Ingestion Device | Big Data Final Project

Arsitektur:
  - Satu Thread per kamera (TrafficMonitorThread)
  - YOLOv8n untuk deteksi kendaraan (model di-share antar thread)
  - Polygon-based ROI untuk deteksi pelanggaran jalur sepeda
  - Cooldown berbasis waktu untuk throttle event Kafka
  - Auto-reconnect jika stream CCTV putus

Dependencies:
  pip install ultralytics opencv-python kafka-python numpy

Cara menjalankan:
  python traffic_monitor.py

Untuk testing tanpa stream langsung, ganti `url` dengan path video lokal:
  "url": "data/test_video.mp4"
=============================================================================
"""

import os
import cv2
import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import numpy as np
from kafka import KafkaProducer
from kafka.errors import KafkaError
from ultralytics import YOLO

# Gunakan protokol TCP untuk RTSP untuk menghindari packet loss (mengatasi error RTP timestamps / dropped frames)
os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp"

from flask import Flask, Response

stream_app = Flask(__name__)
LATEST_FRAMES = {}
frames_lock = threading.Lock()

def generate_mjpeg(camera_id):
    while True:
        with frames_lock:
            frame = LATEST_FRAMES.get(camera_id)
        if frame is None:
            time.sleep(0.1)
            continue
        ret, buffer = cv2.imencode('.jpg', frame)
        if not ret:
            time.sleep(0.1)
            continue
        frame_bytes = buffer.tobytes()
        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + frame_bytes + b'\r\n')
        time.sleep(0.1)

@stream_app.route('/video_feed/<camera_id>')
def video_feed(camera_id):
    return Response(generate_mjpeg(camera_id), mimetype='multipart/x-mixed-replace; boundary=frame')

def start_stream_server():
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    stream_app.run(host='0.0.0.0', port=5001, debug=False, use_reloader=False)



# LOGGING

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(threadName)-30s] %(levelname)s → %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# KONFIGURASI KAMERA
# Tambah / edit entri di sini untuk kamera baru.
# polygon  = koordinat jalur sepeda yang ingin dipantau
# road_roi = area jalan satu arah (untuk filter kendaraan dari arah berlawanan)

CCTV_CONFIGS: list[dict] = [
    # ✅ CCTV_BASUKI_RAHMAT
    {
        "camera_id"         : "CCTV_BASUKI_RAHMAT",
        "location"          : "Jl. Basuki Rahmat",
        "url"               : "rtsp://edishub:g412uda5u12y426@36.66.208.113:554/Streaming/Channels/12502",
        "bike_lane_polygon" : [[166, 400], [100, 451], [44, 492], [6, 508], [2, 468], [59, 430], [133, 384]],
        "road_roi_polygon"  : [[374, 254], [526, 274], [408, 437], [282, 571], [221, 570], [7, 572], [1, 524], [4, 461], [111, 392], [207, 368]],
    },
    # ✅ CCTV_BAMBU_RUNCING
    {
        "camera_id"         : "CCTV_BAMBU_RUNCING",
        "location"          : "Jl. Panglima Sudirman",
        "url"               : "rtsp://edishub:g412uda5u12y426@36.66.208.113:554/Streaming/Channels/12802",
        "bike_lane_polygon" : [[12, 218], [62, 220], [179, 212], [278, 190], [329, 148], [156, 189], [88, 201], [24, 203]],
        "road_roi_polygon"  : [[28, 226], [104, 230], [205, 219], [315, 197], [366, 178], [423, 162], [467, 141], [397, 67], [298, 78], [206, 100], [128, 128], [73, 149], [25, 173]],
    },
    # ✅ CCTV_BASRA_LOOP
    {
        "camera_id"         : "CCTV_BASRA_LOOP",
        "location"          : "Depan TP1",
        "url"               : "rtsp://edishub:g412uda5u12y426@36.66.208.113:554/Streaming/Channels/5702",
        "bike_lane_polygon" : [[225, 573], [415, 437], [463, 390], [499, 356], [515, 341], [552, 357], [368, 575]],
        "road_roi_polygon"  : [[2, 355], [198, 288], [351, 241], [455, 220], [601, 295], [612, 312], [434, 575], [154, 571], [10, 573], [6, 573]],
    },
    # ✅ CCTV_DARMO_MERCURE
    {
        "camera_id"         : "CCTV_DARMO_MERCURE",
        "location"          : "Jl. Raya Darmo",
        "url"               : "rtsp://edishub:g412uda5u12y426@36.66.208.113:554/Streaming/Channels/7602",
        "bike_lane_polygon" : [[2, 263], [36, 233], [68, 191], [99, 163], [132, 128], [162, 98], [192, 59], [214, 37], [204, 36], [150, 85], [82, 147], [14, 217], [2, 217]],
        "road_roi_polygon"  : [[140, 574], [241, 192], [267, 100], [215, 23], [175, 64], [86, 138], [2, 207], [4, 346], [0, 570]],
    },
]


# KONFIGURASI KAFKA
KAFKA_BOOTSTRAP_SERVERS: list[str] = ["localhost:9092"]
KAFKA_TOPIC: str = "bikeline-violations"


# KONFIGURASI DETEKSI
# Class ID YOLOv8: 2=car, 3=motorcycle, 5=bus, 7=truck
TARGET_CLASSES: list[int] = [2, 3, 5, 7]

CONF_PER_CLASS: dict[int, float] = {
    2: 0.20,   # car
    3: 0.18,   # motorcycle
    5: 0.30,   # bus
    7: 0.30,   # truck
}

IOU_THRESHOLD: float = 0.35          # NMS threshold
MIN_BBOX_HEIGHT_CAR: int = 30        # Filter false positive marka jalan
PROCESS_EVERY_N_FRAMES: int = 3      # Skip frame untuk hemat CPU
VIOLATION_COOLDOWN_SEC: float = 2.0  # Throttle Kafka per kamera
RECONNECT_INTERVAL_SEC: int = 5      # Jeda sebelum reconnect jika stream putus

# CLAHE (Contrast Limited Adaptive Histogram Equalization)
# Sangat membantu mendeteksi motor kecil di siang terik atau kondisi backlit
ENABLE_CLAHE: bool = True
CLAHE_CLIP_LIMIT: float = 2.0
CLAHE_TILE_SIZE: tuple[int, int] = (8, 8)


# KAFKA PRODUCER MANAGER (Singleton, thread-safe)

class KafkaProducerManager:
    """
    Singleton wrapper untuk KafkaProducer.
    kafka-python KafkaProducer secara internal thread-safe,
    sehingga satu instance bisa dipakai oleh semua thread kamera.

    Jika Kafka tidak tersedia (development / testing), sistem tetap
    berjalan dan payload hanya di-log ke console (dry-run mode).
    """

    _instance: Optional["KafkaProducerManager"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "KafkaProducerManager":
        with cls._lock:
            if cls._instance is None:
                obj = super().__new__(cls)
                obj._producer: Optional[KafkaProducer] = None
                obj._dry_run: bool = False
                obj._connect()
                cls._instance = obj
        return cls._instance

    def _connect(self) -> None:
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",        # Pastikan pesan tersimpan di semua replica
                retries=3,
                linger_ms=50,      # Kecil saja; kita butuh low latency
            )
            logger.info("✅ Kafka Producer terhubung ke %s", KAFKA_BOOTSTRAP_SERVERS)
        except KafkaError as exc:
            logger.warning(
                "⚠️  Kafka tidak tersedia (%s). Beralih ke DRY-RUN mode.", exc
            )
            self._producer = None
            self._dry_run = True

    def send(self, payload: dict) -> None:
        """
        Kirim payload ke topic Kafka secara non-blocking.
        Error callback mencegah thread crash akibat kegagalan jaringan.
        """
        if self._dry_run or self._producer is None:
            logger.info("[DRY-RUN] %s", json.dumps(payload, ensure_ascii=False))
            return

        def _on_error(exc: Exception) -> None:
            logger.error("Gagal kirim ke Kafka: %s", exc)

        try:
            future = self._producer.send(KAFKA_TOPIC, value=payload)
            future.add_errback(_on_error)
        except KafkaError as exc:
            logger.error("KafkaError saat send(): %s", exc)

    def close(self) -> None:
        if self._producer:
            self._producer.flush(timeout=5)
            self._producer.close()
            logger.info("Kafka Producer ditutup.")


# THREAD PER KAMERA

class TrafficMonitorThread(threading.Thread):
    """
    Thread independen yang mengelola satu stream CCTV dari awal hingga akhir.

    Siklus hidup:
      1. Buka koneksi ke stream (auto-reconnect jika gagal)
      2. Baca frame → skip jika bukan kelipatan PROCESS_EVERY_N_FRAMES
      3. Preprocessing CLAHE (opsional) untuk tingkatkan deteksi motor
      4. Deteksi kendaraan dengan YOLOv8
      5. Filter per confidence kelas
      6. Cek titik roda di road_roi_polygon (filter arah berlawanan)
      7. Cek titik roda di bike_lane_polygon (deteksi pelanggaran)
      8. Kirim event ke Kafka (dengan cooldown)
      9. Tampilkan frame teranotasi di jendela OpenCV
    """

    # Model YOLOv8 di-load SEKALI dan di-share ke semua thread.
    # Ultralytics YOLO inference thread-safe (model weights read-only).
    _shared_model: Optional[YOLO] = None
    _model_lock = threading.Lock()

    # CLAHE object di-share (tidak menyimpan state per frame)
    _clahe: Optional[cv2.CLAHE] = None

    def __init__(
        self,
        config: dict,
        kafka_manager: KafkaProducerManager,
        show_window: bool = True,
    ) -> None:
        super().__init__(
            name=f"Cam-{config['camera_id']}",
            daemon=True,   # Ikut mati bersama main thread saat Ctrl+C
        )
        self.camera_id: str = config["camera_id"]
        self.location: str = config["location"]
        self.url: str = config["url"]

        # Polygon ROI dalam format numpy int32 (syarat cv2.pointPolygonTest)
        self.bike_lane_poly = np.array(
            config["bike_lane_polygon"], dtype=np.int32
        )
        self.road_roi_poly = np.array(
            config["road_roi_polygon"], dtype=np.int32
        )

        self.kafka = kafka_manager
        self.show_window = show_window
        self._stop_event = threading.Event()

        # Cooldown: timestamp terakhir pelanggaran dikirim ke Kafka
        self._last_sent_time: float = 0.0

        self._ensure_model_loaded()
        self._ensure_clahe_ready()

    # Setup (class-level, dijalankan sekali)
    @classmethod
    def _ensure_model_loaded(cls) -> None:
        with cls._model_lock:
            if cls._shared_model is None:
                logger.info("Memuat model YOLOv8n…")
                cls._shared_model = YOLO("yolov8n.pt")
                logger.info("Model YOLOv8n siap.")

    @classmethod
    def _ensure_clahe_ready(cls) -> None:
        if ENABLE_CLAHE and cls._clahe is None:
            cls._clahe = cv2.createCLAHE(
                clipLimit=CLAHE_CLIP_LIMIT,
                tileGridSize=CLAHE_TILE_SIZE,
            )

    # Properti helper
    @property
    def model(self) -> YOLO:
        return self.__class__._shared_model  # type: ignore[return-value]

    def stop(self) -> None:
        """Sinyal dari luar untuk menghentikan thread ini dengan bersih."""
        self._stop_event.set()

    # Utilitas
    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def _point_in_polygon(polygon: np.ndarray, point: tuple[int, int]) -> bool:
        """Return True jika `point` berada di dalam atau di tepi `polygon`."""
        return cv2.pointPolygonTest(polygon, point, measureDist=False) >= 0

    def _cooldown_ok(self) -> bool:
        """
        Cek apakah sudah melewati cooldown window.
        Jika ya, update timestamp dan kembalikan True.
        """
        now = time.monotonic()
        if (now - self._last_sent_time) >= VIOLATION_COOLDOWN_SEC:
            self._last_sent_time = now
            return True
        return False

    # Preprocessing
    def _enhance_frame(self, frame: np.ndarray) -> np.ndarray:
        """
        CLAHE pada channel L (LAB color space).

        Mengapa LAB dan bukan HSV / grayscale?
        → CLAHE di channel L hanya menyentuh luminansi tanpa menggeser warna,
          sehingga detektor YOLO tetap mendapat warna asli kendaraan.
        Manfaat untuk motor:
        → Motor kecil di kondisi backlit (langit cerah di belakang) atau
          aspal gelap sering 'tenggelam' karena contrast rendah.
          CLAHE memotong histogram lokal sehingga motor lebih menonjol.
        """
        if not ENABLE_CLAHE or self.__class__._clahe is None:
            return frame

        lab = cv2.cvtColor(frame, cv2.COLOR_BGR2LAB)
        l_ch, a_ch, b_ch = cv2.split(lab)
        l_ch = self.__class__._clahe.apply(l_ch)
        return cv2.cvtColor(cv2.merge((l_ch, a_ch, b_ch)), cv2.COLOR_LAB2BGR)

    # Deteksi & Anotasi
    def _process_frame(
        self, frame: np.ndarray
    ) -> tuple[np.ndarray, int]:
        """
        Proses satu frame:
          1. CLAHE preprocessing
          2. YOLO inference
          3. Per-class confidence filter
          4. Road ROI filter (arah berlawanan)
          5. Bike lane violation check
          6. Annotasi & Kafka send

        Returns:
            (annotated_frame, jumlah_pelanggaran_frame_ini)
        """
        enhanced = self._enhance_frame(frame)

        # Gunakan threshold conf paling rendah agar YOLO tidak miss objek kecil,
        # lalu kita filter manual per kelas di bawah.
        global_conf = min(CONF_PER_CLASS.values())

        results = self.model(
            enhanced,
            classes=TARGET_CLASSES,
            conf=global_conf,
            iou=IOU_THRESHOLD,
            verbose=False,
        )

        violation_count = 0

        for box in results[0].boxes:
            cls_id = int(box.cls[0])
            confidence = float(box.conf[0])

            # Filter 1: Confidence per kelas
            if confidence < CONF_PER_CLASS.get(cls_id, 0.25):
                continue

            x1, y1, x2, y2 = map(int, box.xyxy[0])
            vehicle_type: str = self.model.names[cls_id]
            bbox_h = y2 - y1

            # Filter 2: Tinggi bounding box (marka jalan, noise)
            if vehicle_type == "car" and bbox_h < MIN_BBOX_HEIGHT_CAR:
                continue

            # Titik roda (bottom-center bounding box)
            # CATATAN: Pakai y2 (tepi bawah box), bukan (y1+y2)/2.
            # Titik tengah box ada di badan kendaraan, bukan di roda.
            # Ini bug umum yang bikin cek polygon tidak presisi.
            wheel_x = (x1 + x2) // 2
            wheel_y = y2
            wheel_pt = (wheel_x, wheel_y)

            # Filter 3: Road ROI – filter arah berlawanan
            if not self._point_in_polygon(self.road_roi_poly, wheel_pt):
                # Kendaraan di luar ROI searah → kemungkinan dari arah berlawanan
                continue

            # Cek Pelanggaran
            is_violating = self._point_in_polygon(self.bike_lane_poly, wheel_pt)

            if is_violating:
                violation_count += 1
                # Kotak MERAH + label
                cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 0, 255), 2)
                label = f"LANGGAR: {vehicle_type} {confidence:.0%}"
                cv2.putText(
                    frame, label,
                    (x1, max(y1 - 8, 14)),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 255), 2,
                )
                # Titik roda
                cv2.circle(frame, wheel_pt, 5, (0, 0, 255), -1)

                # Kafka Send (dengan cooldown)
                if self._cooldown_ok():
                    payload = {
                        "camera_id": self.camera_id,
                        "location": self.location,
                        "timestamp": self._utc_now(),
                        "vehicle_type": vehicle_type,
                        "confidence_score": round(confidence, 4),
                    }
                    self.kafka.send(payload)
                    # Print JSON bersih ke terminal agar mudah dibaca manusia
                    print(f"🚨 [KAFKA SEND] → {json.dumps(payload, ensure_ascii=False)}")
            else:
                # Kotak HIJAU – kendaraan di jalur benar
                cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 220, 0), 2)

        # Overlay Polygon & Info
        cv2.polylines(frame, [self.bike_lane_poly], True, (255, 80, 0), 2)
        cv2.polylines(frame, [self.road_roi_poly], True, (0, 230, 230), 1)

        header_text = (
            f"{self.camera_id}  |  Pelanggar frame ini: {violation_count}"
        )
        cv2.rectangle(frame, (0, 0), (len(header_text) * 11, 28), (0, 0, 0), -1)
        cv2.putText(
            frame, header_text,
            (6, 20),
            cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2,
        )

        with frames_lock:
            LATEST_FRAMES[self.camera_id] = frame.copy()

        return frame, violation_count

    # Stream Management
    def _open_stream(self) -> Optional[cv2.VideoCapture]:
        logger.info("[%s] Menghubungkan ke: %s", self.camera_id, self.url)
        cap = cv2.VideoCapture(self.url)
        # Buffer size 1 → selalu baca frame terbaru, hindari lag antrian buffer
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        if not cap.isOpened():
            logger.warning("[%s] Gagal membuka stream.", self.camera_id)
            return None
        logger.info("[%s] Stream terhubung ✅", self.camera_id)
        return cap

#=================================================================================
    # Main Loop
    def run(self) -> None:
        """
        Outer loop : kelola koneksi + reconnect.
        Inner loop : baca & proses frame.
        Kedua loop dijaga oleh self._stop_event agar bisa dihentikan dari luar.
        """
        logger.info("[%s] Thread dimulai.", self.camera_id)
        frame_count = 0

        while not self._stop_event.is_set():
            # Buka / Reconnect Stream
            cap = self._open_stream()
            if cap is None:
                logger.warning(
                    "[%s] Retry dalam %ds…", self.camera_id, RECONNECT_INTERVAL_SEC
                )
                time.sleep(RECONNECT_INTERVAL_SEC)
                continue

            # Inner Frame Loop
            while not self._stop_event.is_set():
                success, frame = cap.read()

                if not success or frame is None:
                    logger.warning(
                        "[%s] Frame gagal → mencoba reconnect.", self.camera_id
                    )
                    break  # Kembali ke outer loop untuk reconnect

                frame_count += 1
                if frame_count % PROCESS_EVERY_N_FRAMES != 0:
                    continue

                try:
                    annotated, _ = self._process_frame(frame)
                except Exception as exc:
                    logger.error(
                        "[%s] Error saat proses frame: %s",
                        self.camera_id, exc,
                        exc_info=True,
                    )
                    continue

                if self.show_window:
                    win_name = f"{self.camera_id} – {self.location}"
                    cv2.imshow(win_name, annotated)
                    # waitKey(1) wajib ada agar jendela OpenCV bisa me-render
                    if cv2.waitKey(1) & 0xFF == ord("q"):
                        self._stop_event.set()
                        break

            cap.release()

            # Jeda sebelum reconnect (jika stop_event belum aktif)
            if not self._stop_event.is_set():
                logger.info(
                    "[%s] Reconnect dalam %ds…",
                    self.camera_id, RECONNECT_INTERVAL_SEC,
                )
                time.sleep(RECONNECT_INTERVAL_SEC)

        logger.info("[%s] Thread selesai.", self.camera_id)


# ENTRYPOINT

def main() -> None:
    sep = "=" * 64
    logger.info(sep)
    logger.info("  Surabaya Smart City – Traffic Violation Detection")
    logger.info("  Jumlah kamera aktif : %d", len(CCTV_CONFIGS))
    logger.info("  Kafka topic         : %s", KAFKA_TOPIC)
    logger.info("  CLAHE preprocessing : %s", "ON" if ENABLE_CLAHE else "OFF")
    logger.info(sep)

    threading.Thread(target=start_stream_server, daemon=True).start()
    logger.info("✅ MJPEG Stream Server berjalan di http://localhost:5001")

    kafka_manager = KafkaProducerManager()
    threads: list[TrafficMonitorThread] = []

    for config in CCTV_CONFIGS:
        thread = TrafficMonitorThread(config, kafka_manager, show_window=True)
        threads.append(thread)
        thread.start()
        time.sleep(0.5)  # Stagger startup agar tidak semua thread load model bersamaan

    logger.info("Semua thread berjalan. Tekan Ctrl+C atau 'q' di jendela manapun untuk berhenti.")

    try:
        # Jaga main thread tetap hidup; daemon threads akan mati sendiri
        while any(t.is_alive() for t in threads):
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Ctrl+C diterima → menghentikan semua thread…")
        for t in threads:
            t.stop()

    # Tunggu semua thread selesai (max 10 detik per thread)
    for t in threads:
        t.join(timeout=10)
        if t.is_alive():
            logger.warning("Thread %s tidak berhenti dalam batas waktu.", t.name)

    cv2.destroyAllWindows()
    kafka_manager.close()
    logger.info("Sistem dihentikan dengan bersih. Sampai jumpa! 👋")


if __name__ == "__main__":
    main()