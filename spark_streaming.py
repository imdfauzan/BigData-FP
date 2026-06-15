import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# ============================================================================
# 1.1 — Import & Konfigurasi
# ============================================================================

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-lite:29092")
KAFKA_TOPIC             = "surabaya-traffic-bikeline-violations"

# MinIO / S3A Configuration
MINIO_ENDPOINT          = os.getenv("MINIO_ENDPOINT", "http://minio-storage:9000")
MINIO_ACCESS_KEY        = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY        = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

# Output paths (Delta Lake di MinIO)
BASE_OUTPUT_PATH = os.getenv("OUTPUT_PATH", "s3a://lakehouse")
BRONZE_PATH      = "s3a://bronze/violations"
SILVER_PATH      = "s3a://silver/violations_clean"
GOLD_PATH        = "s3a://gold/violations_agg"

# Checkpoint paths (untuk Spark recovery)
CHECKPOINT_BRONZE = "s3a://lakehouse/checkpoints/bronze"
CHECKPOINT_SILVER = "s3a://lakehouse/checkpoints/silver"
CHECKPOINT_GOLD   = "s3a://lakehouse/checkpoints/gold"

# Aturan validasi data
VALID_VEHICLE_TYPES = {"car", "motorcycle", "bus", "truck"}
VALID_CAMERA_IDS    = {
    "CCTV_BASUKI_RAHMAT", "CCTV_BAMBU_RUNCING",
    "CCTV_BASRA_LOOP",    "CCTV_DARMO_MERCURE",
}
MIN_CONFIDENCE_SCORE = 0.15
WATERMARK_DELAY      = "10 minutes"
WINDOW_DURATION      = "5 minutes"
SLIDE_DURATION       = "5 minutes"


# ============================================================================
# 1.2 — SparkSession
# ============================================================================

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("SurabayaBikeLaneViolations-StreamProcessor")
        # Delta Lake extensions
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # S3A / MinIO configuration
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Performance tuning
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "true")
        # Delta Lake optimization
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================================
# 1.3 — Schema JSON
# ============================================================================

VIOLATION_SCHEMA = StructType([
    StructField("camera_id",        StringType(), nullable=True),
    StructField("location",         StringType(), nullable=True),
    StructField("timestamp",        StringType(), nullable=True),
    StructField("vehicle_type",     StringType(), nullable=True),
    StructField("confidence_score", FloatType(),  nullable=True),
])


# ============================================================================
# 1.4 — Baca Stream dari Kafka
# ============================================================================

def read_kafka_stream(spark: SparkSession):
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )

    parsed = (
        raw_stream
        .select(
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ingest_time"),
            F.from_json(F.col("value").cast("string"), VIOLATION_SCHEMA).alias("payload")
        )
        .select("kafka_partition", "kafka_offset", "kafka_ingest_time", "payload.*")
    )

    return parsed


# ============================================================================
# 1.5 — Bronze Layer (Raw Sink)
# ============================================================================

def write_bronze(stream_df):
    bronze_df = stream_df.withColumn("ingest_date", F.to_date("kafka_ingest_time"))

    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_BRONZE)
        .option("mergeSchema", "true")
        .partitionBy("ingest_date")
        .start(BRONZE_PATH)
    )
    return query


# ============================================================================
# 1.6 — Fungsi Pembersihan Data (dipakai Silver & Gold)
# ============================================================================

def clean_and_validate(stream_df):
    # Step 1: Parse timestamp string → TimestampType
    df = stream_df.withColumn(
        "event_time",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )

    # Step 2: Buang baris dengan field krusial NULL
    df = df.filter(
        F.col("camera_id").isNotNull()
        & F.col("vehicle_type").isNotNull()
        & F.col("confidence_score").isNotNull()
        & F.col("event_time").isNotNull()
    )

    # Step 3: Validasi vehicle_type
    df = df.filter(F.col("vehicle_type").isin(list(VALID_VEHICLE_TYPES)))

    # Step 4: Validasi camera_id
    df = df.filter(F.col("camera_id").isin(list(VALID_CAMERA_IDS)))

    # Step 5: Filter confidence terlalu rendah (noise YOLO)
    df = df.filter(F.col("confidence_score") >= MIN_CONFIDENCE_SCORE)

    # Step 6: Tambah kolom derivasi
    df = (
        df
        .withColumn("hour_of_day",       F.hour("event_time"))
        .withColumn("day_of_week",        F.dayofweek("event_time"))
        .withColumn("event_date",         F.to_date("event_time"))
        .withColumn("confidence_score",   F.round("confidence_score", 4))
        .drop("timestamp")
    )

    return df


# ============================================================================
# 1.7 — Silver Layer (Clean Sink)
# ============================================================================

def write_silver(stream_df):
    silver_df = clean_and_validate(stream_df)

    # Watermark WAJIB dipasang agar Gold bisa window dengan benar
    silver_with_watermark = silver_df.withWatermark("event_time", WATERMARK_DELAY)

    query = (
        silver_with_watermark.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_SILVER)
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .start(SILVER_PATH)
    )
    return query


# ============================================================================
# 1.8 — Gold Layer (Aggregated Sink) - Enhanced with Multiple Queries
# ============================================================================

def write_gold(stream_df):
    """
    Gold Layer yang menyimpan multiple pre-aggregated tables untuk dashboard:
    1. gold_hourly: Pelanggaran per jam per kamera per jenis kendaraan
    2. gold_vehicle: Agregasi per jenis kendaraan
    3. gold_camera: Agregasi per kamera
    4. gold_confidence: Statistik confidence score
    5. gold_daily: Trend harian
    """
    
    silver_df = clean_and_validate(stream_df)
    silver_with_watermark = silver_df.withWatermark("event_time", WATERMARK_DELAY)
    
    queries = []
    
    # ========================================================================
    # GOLD TABLE 1: Pelanggaran per Jam (5-minute window)
    # ========================================================================
    gold_hourly = (
        silver_with_watermark
        .groupBy(
            F.window("event_time", WINDOW_DURATION, SLIDE_DURATION),
            F.col("camera_id"),
            F.col("location"),
            F.col("vehicle_type"),
        )
        .agg(
            F.count("*").alias("total_violations"),
            F.round(F.avg("confidence_score"), 4).alias("avg_confidence"),
            F.round(F.max("confidence_score"), 4).alias("max_confidence"),
            F.round(F.min("confidence_score"), 4).alias("min_confidence"),
            F.round(F.stddev("confidence_score"), 4).alias("stddev_confidence"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("event_date", F.to_date("window_start"))
        .withColumn("hour_of_day", F.hour("window_start"))
        .drop("window")
    )
    
    q1 = (
        gold_hourly.writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", f"{CHECKPOINT_GOLD}/hourly")
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .start(f"{GOLD_PATH}/hourly")
    )
    queries.append(q1)
    
    # ========================================================================
    # GOLD TABLE 2: Pelanggaran per Jenis Kendaraan (Hourly)
    # ========================================================================
    gold_vehicle = (
        silver_with_watermark
        .groupBy(
            F.window("event_time", WINDOW_DURATION, SLIDE_DURATION),
            F.col("vehicle_type"),
        )
        .agg(
            F.count("*").alias("total_violations"),
            F.countDistinct("camera_id").alias("affected_cameras"),
            F.round(F.avg("confidence_score"), 4).alias("avg_confidence"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("event_date", F.to_date("window_start"))
        .drop("window")
    )
    
    q2 = (
        gold_vehicle.writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", f"{CHECKPOINT_GOLD}/vehicle")
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .start(f"{GOLD_PATH}/vehicle")
    )
    queries.append(q2)
    
    # ========================================================================
    # GOLD TABLE 3: Pelanggaran per Kamera (Hotspot Analysis)
    # ========================================================================
    gold_camera = (
        silver_with_watermark
        .groupBy(
            F.window("event_time", WINDOW_DURATION, SLIDE_DURATION),
            F.col("camera_id"),
            F.col("location"),
        )
        .agg(
            F.count("*").alias("total_violations"),
            F.round(F.avg("confidence_score"), 4).alias("avg_confidence"),
            F.countDistinct("vehicle_type").alias("distinct_vehicle_types"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("event_date", F.to_date("window_start"))
        .drop("window")
    )
    
    q3 = (
        gold_camera.writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", f"{CHECKPOINT_GOLD}/camera")
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .start(f"{GOLD_PATH}/camera")
    )
    queries.append(q3)
    
    # ========================================================================
    # GOLD TABLE 4: Confidence Score Statistics
    # ========================================================================
    gold_confidence = (
        silver_with_watermark
        .groupBy(
            F.window("event_time", WINDOW_DURATION, SLIDE_DURATION),
            F.col("vehicle_type"),
        )
        .agg(
            F.round(F.avg("confidence_score"), 4).alias("avg_confidence"),
            F.round(F.max("confidence_score"), 4).alias("max_confidence"),
            F.round(F.min("confidence_score"), 4).alias("min_confidence"),
            F.round(F.percentile_approx("confidence_score", 0.5), 4).alias("median_confidence"),
            F.round(F.percentile_approx("confidence_score", 0.95), 4).alias("p95_confidence"),
            F.count("*").alias("sample_size"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("event_date", F.to_date("window_start"))
        .drop("window")
    )
    
    q4 = (
        gold_confidence.writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", f"{CHECKPOINT_GOLD}/confidence")
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .start(f"{GOLD_PATH}/confidence")
    )
    queries.append(q4)
    
    # ========================================================================
    # GOLD TABLE 5: Daily Trend (Aggregated per hari)
    # ========================================================================
    gold_daily = (
        silver_with_watermark
        .groupBy(F.to_date("event_time").alias("event_date"))
        .agg(
            F.count("*").alias("total_violations"),
            F.countDistinct("camera_id").alias("affected_cameras"),
            F.round(F.avg("confidence_score"), 4).alias("avg_confidence"),
            F.countDistinct("vehicle_type").alias("distinct_vehicle_types"),
        )
    )
    
    q5 = (
        gold_daily.writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", f"{CHECKPOINT_GOLD}/daily")
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .start(f"{GOLD_PATH}/daily")
    )
    queries.append(q5)
    
    return queries


# ============================================================================
# 1.9 — Console Debug Sink (Development Only)
# ============================================================================

def write_console_debug(stream_df):
    silver_df = clean_and_validate(stream_df)
    silver_with_watermark = silver_df.withWatermark("event_time", WATERMARK_DELAY)

    debug_df = (
        silver_with_watermark
        .groupBy(F.window("event_time", WINDOW_DURATION), F.col("camera_id"), F.col("vehicle_type"))
        .agg(F.count("*").alias("violations"), F.round(F.avg("confidence_score"), 3).alias("avg_conf"))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

    query = (
        debug_df.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(processingTime="30 seconds")
        .start()
    )
    return query


# ============================================================================
# 1.10 — Entrypoint main()
# ============================================================================

def main():
    spark = create_spark_session()
    raw_stream = read_kafka_stream(spark)

    queries = []
    
    print("=" * 80)
    print("SURABAYA BIKE LANE VIOLATIONS - STREAM PROCESSOR")
    print("Storage & Lakehouse Architecture (Anggota 4)")
    print("=" * 80)
    print()
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print()
    print(f"MinIO Endpoint: {MINIO_ENDPOINT}")
    print(f"Bronze Path: {BRONZE_PATH}")
    print(f"Silver Path: {SILVER_PATH}")
    print(f"Gold Path (Multiple Tables): {GOLD_PATH}/*")
    print()
    print("Starting streams...")
    print()
    
    # ========================================================================
    # Bronze Layer: Raw data sink (unchanged)
    # ========================================================================
    try:
        q_bronze = write_bronze(raw_stream)
        queries.append(("BRONZE", q_bronze))
        print("✓ Bronze layer stream started")
    except Exception as e:
        print(f"✗ Bronze layer failed: {e}")
    
    # ========================================================================
    # Silver Layer: Cleaned data sink (unchanged)
    # ========================================================================
    try:
        q_silver = write_silver(raw_stream)
        queries.append(("SILVER", q_silver))
        print("✓ Silver layer stream started")
    except Exception as e:
        print(f"✗ Silver layer failed: {e}")
    
    # ========================================================================
    # Gold Layer: Multiple aggregated tables (ENHANCED)
    # ========================================================================
    try:
        gold_queries = write_gold(raw_stream)
        for idx, q in enumerate(gold_queries, 1):
            queries.append((f"GOLD-{idx}", q))
        print(f"✓ Gold layer streams started ({len(gold_queries)} tables)")
    except Exception as e:
        print(f"✗ Gold layer failed: {e}")
    
    # ========================================================================
    # Debug Console (Development Only)
    # ========================================================================
    try:
        q_console = write_console_debug(raw_stream)
        queries.append(("DEBUG-CONSOLE", q_console))
        print("✓ Console debug stream started (development only)")
    except Exception as e:
        print(f"✗ Console debug stream failed: {e}")
    
    print()
    print("=" * 80)
    print(f"All streams are running. Monitoring {len(queries)} active queries...")
    print("Press Ctrl+C to stop.")
    print("=" * 80)
    print()
    
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print()
        print("=" * 80)
        print("Stopping all streams...")
        print("=" * 80)
        for name, q in queries:
            try:
                q.stop()
                print(f"✓ {name} stopped")
            except Exception as e:
                print(f"✗ {name} stop failed: {e}")
    finally:
        spark.stop()
        print("✓ Spark session closed")

if __name__ == "__main__":
    main()
