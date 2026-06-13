"""
=============================================================================
Surabaya Smart City - Kafka Consumer Test Script
Orang 2 - Data Platform / Message Broker

Tujuan:
  Script ini dipakai untuk MEMVERIFIKASI bahwa payload JSON pelanggaran
  yang dikirim oleh traffic_monitor.py (Orang 1) sudah berhasil masuk
  ke Kafka topic "surabaya-traffic-bikeline-violations".

Cara pakai:
  1. Pastikan kafka-lite sudah running (docker compose up -d)
  2. Jalankan traffic_monitor.py di terminal lain (producer)
  3. Jalankan script ini di terminal terpisah (consumer)
  4. Setiap kali ada pelanggaran terdeteksi, payload JSON akan tercetak di sini

Dependencies:
  pip install kafka-python
=============================================================================
"""

import json
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "surabaya-traffic-bikeline-violations"


def main() -> None:
    print("=" * 64)
    print("  Kafka Consumer Test - Surabaya Bike Lane Violations")
    print(f"  Topic   : {KAFKA_TOPIC}")
    print(f"  Server  : {KAFKA_BOOTSTRAP_SERVERS}")
    print("=" * 64)
    print("Menunggu pesan masuk... (Ctrl+C untuk berhenti)\n")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="test-consumer-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    try:
        for message in consumer:
            payload = message.value
            print(
                f"[partition={message.partition} offset={message.offset}] "
                f"{json.dumps(payload, ensure_ascii=False)}"
            )
    except KeyboardInterrupt:
        print("\nBerhenti oleh user.")
    finally:
        consumer.close()
        print("Consumer ditutup.")


if __name__ == "__main__":
    main()