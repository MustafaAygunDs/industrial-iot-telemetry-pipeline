#!/usr/bin/env python3
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, lit, current_timestamp, to_timestamp, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "sensor_telemetry"
PG_JDBC_URL = "jdbc:postgresql://localhost:5433/veri_db"
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_TABLE = "maintenance_logs"

RPM_WARNING_THRESHOLD = 3000
RPM_CRITICAL_THRESHOLD = 3500
TEMP_WARNING_THRESHOLD = 85
TEMP_CRITICAL_THRESHOLD = 90

print("=" * 70)
print("[CONFIG] Industrial IoT Telemetry Processing Engine")
print("=" * 70)

spark = SparkSession.builder.appName("PredictiveMaintenanceEngine").config("spark.driver.memory", "2g").config("spark.executor.memory", "2g").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

sensor_telemetry_schema = StructType([
    StructField("machine_id", StringType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("bearing_temperature", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

print("[INFO] Kafka'dan veri okuma başlıyor...")

try:
    kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).option("subscribe", KAFKA_TOPIC).option("startingOffsets", "latest").option("failOnDataLoss", "false").load()
    print(f"[✅] Kafka source başarıyla bağlandı: {KAFKA_TOPIC}")
except Exception as e:
    print(f"[❌] Kafka bağlantısı başarısız: {e}")
    sys.exit(1)

try:
    json_df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), sensor_telemetry_schema).alias("data")).select("data.*")
    json_df = json_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    json_df_processed = json_df.withColumn("processed_at", current_timestamp())
    print("[✅] JSON parse ve veri dönüşümü başarılı")
except Exception as e:
    print(f"[❌] JSON parse hatası: {e}")
    sys.exit(1)

anomaly_df = json_df_processed.withColumn(
    "anomaly_level",
    when((col("rpm") >= RPM_CRITICAL_THRESHOLD) | (col("bearing_temperature") >= TEMP_CRITICAL_THRESHOLD), lit("CRITICAL"))
    .when((col("rpm") >= RPM_WARNING_THRESHOLD) | (col("bearing_temperature") >= TEMP_WARNING_THRESHOLD), lit("WARNING"))
    .otherwise(lit("NORMAL"))
)

alert_df = anomaly_df.withColumn(
    "alert_message",
    when(col("anomaly_level") == "CRITICAL", concat_ws(" | ", lit("🚨 KRİTİK UYARI"), col("machine_id")))
    .when(col("anomaly_level") == "WARNING", concat_ws(" | ", lit("⚠️ UYARI: Bakım Yaklaşıyor"), col("machine_id")))
    .otherwise(concat_ws(" | ", lit("✅ NORMAL"), col("machine_id")))
)

critical_alerts_df = alert_df.filter(col("anomaly_level").isin(["WARNING", "CRITICAL"]))

def write_batch_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"[BATCH {batch_id:03d}] Boş batch, skip")
        return
    try:
        record_count = batch_df.count()
        print(f"[BATCH {batch_id:03d}] {record_count} satır yazılıyor...")
        batch_df.write.format("jdbc").option("url", PG_JDBC_URL).option("dbtable", PG_TABLE).option("user", PG_USER).option("password", PG_PASSWORD).option("driver", "org.postgresql.Driver").option("batchsize", "1000").mode("append").save()
        print(f"[BATCH {batch_id:03d}] ✅ Başarıyla yazıldı")
    except Exception as e:
        print(f"[BATCH {batch_id:03d}] ❌ HATA: {e}")
        raise

print("[INFO] Streaming queries başlatılıyor...\n")

query_console = alert_df.writeStream.queryName("konsol_tumverileri").outputMode("append").format("console").option("truncate", "false").option("numRows", 100).start()
query_postgres = alert_df.writeStream.queryName("postgres_maintenance_logs").foreachBatch(write_batch_to_postgres).outputMode("append").start()

print("\n" + "=" * 70)
print("✅ PREDICTIVE MAINTENANCE ENGINE BAŞLATILDI!")
print("=" * 70 + "\n")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n[SHUTDOWN] Streaming durdurulması başladı...")
    spark.streams.stopAll()
    print("[SHUTDOWN] Tüm streams durduruldu.")
