import json
import logging
import threading
from typing import Iterator

from flask import Flask, Response, render_template_string, jsonify
from kafka import KafkaConsumer
from kafka.errors import KafkaError

app = Flask(__name__)

# Konfigurasi Kafka
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "bikeline-violations"

# Buffer sederhana untuk menyimpan 50 event terakhir di memori agar ketika 
# dashboard baru dibuka, tidak kosong.
MAX_EVENTS = 50
recent_events = []
events_lock = threading.Lock()

# Condition object untuk memberi sinyal ke semua SSE clients saat ada event baru
new_event_condition = threading.Condition(events_lock)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def kafka_consumer_thread():
    """Background thread untuk mendengarkan pesan dari Kafka secara live."""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="dashboard-group",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        logger.info(f"Dashboard Kafka Consumer terhubung ke topic: {KAFKA_TOPIC}")

        for message in consumer:
            payload = message.value
            with new_event_condition:
                recent_events.append(payload)
                if len(recent_events) > MAX_EVENTS:
                    recent_events.pop(0)
                # Beritahu semua client SSE yang sedang menunggu
                new_event_condition.notify_all()
                
    except KafkaError as e:
        logger.error(f"Kafka Consumer error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error di Kafka Consumer thread: {e}")

# Mulai background thread
threading.Thread(target=kafka_consumer_thread, daemon=True).start()

@app.route("/")
def index():
    """Route utama yang merender HTML dashboard."""
    # Akan membaca file index.html di folder templates/
    from flask import render_template
    return render_template('index.html')

@app.route("/api/recent_events")
def get_recent_events():
    """API untuk mengambil daftar event terakhir (untuk inisialisasi tabel/chart)."""
    with events_lock:
        return jsonify(recent_events)

@app.route("/stream")
def stream():
    """Server-Sent Events (SSE) endpoint untuk real-time update."""
    def event_stream() -> Iterator[str]:
        # Kirim ping awal agar koneksi langsung tersambung
        yield ": ping\n\n"
        
        while True:
            with new_event_condition:
                # Tunggu sampai ada event baru
                new_event_condition.wait()
                # Ambil event terakhir yang dimasukkan
                if recent_events:
                    latest_event = recent_events[-1]
                    # Format SSE: data: {"key": "value"}\n\n
                    yield f"data: {json.dumps(latest_event)}\n\n"

    return Response(event_stream(), mimetype="text/event-stream")

if __name__ == "__main__":
    logger.info("Menjalankan Dashboard Server di http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
