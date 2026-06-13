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

# Output paths (Delta Lake)
BASE_OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/tmp/lakehouse")
BRONZE_PATH      = f"{BASE_OUTPUT_PATH}/bronze/violations"
SILVER_PATH      = f"{BASE_OUTPUT_PATH}/silver/violations_clean"
GOLD_PATH        = f"{BASE_OUTPUT_PATH}/gold/violations_agg"

# Checkpoint paths (wajib ada untuk Spark recovery)
CHECKPOINT_BRONZE = f"{BASE_OUTPUT_PATH}/checkpoints/bronze"
CHECKPOINT_SILVER = f"{BASE_OUTPUT_PATH}/checkpoints/silver"
CHECKPOINT_GOLD   = f"{BASE_OUTPUT_PATH}/checkpoints/gold"

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
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
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
# 1.8 — Gold Layer (Aggregated Sink)
# ============================================================================

def write_gold(stream_df):
    silver_df = clean_and_validate(stream_df)
    silver_with_watermark = silver_df.withWatermark("event_time", WATERMARK_DELAY)

    gold_df = (
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
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .withColumn("event_date",   F.to_date("window_start"))
        .drop("window")
    )

    query = (
        gold_df.writeStream
        .format("delta")
        .outputMode("complete")          # Gunakan "complete" untuk Delta Lake dengan aggregasi
        .option("checkpointLocation", CHECKPOINT_GOLD)
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .start(GOLD_PATH)
    )
    return query


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
    queries.append(write_bronze(raw_stream))
    queries.append(write_silver(raw_stream))
    queries.append(write_gold(raw_stream))
    queries.append(write_console_debug(raw_stream))  # hapus di production

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        for q in queries:
            q.stop()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
