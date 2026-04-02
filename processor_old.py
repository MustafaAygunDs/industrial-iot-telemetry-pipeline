import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, lit, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

KAFKA_TOPIC = "hiz_verisi"
PG_JDBC_URL = os.environ.get(
    "PG_JDBC_URL", 
    "jdbc:postgresql://localhost:5433/veri_db"
)
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "postgres")
PG_TABLE = os.environ.get("PG_TABLE", "hiz_kayitlari")

print(f"[INFO] PostgreSQL URL: {PG_JDBC_URL}")
print(f"[INFO] PostgreSQL User: {PG_USER}")
print(f"[INFO] PostgreSQL Table: {PG_TABLE}")

spark = SparkSession.builder \
    .appName("HizDenetimi") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("arac_id", StringType(), True),
    StructField("hiz", IntegerType(), True),
    StructField("motor_sicakligi", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

print("[INFO] Kafka'dan okuma başlıyor...")

try:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
except Exception as e:
    print(f"[ERROR] Kafka bağlantısı başarısız: {e}")
    sys.exit(1)

try:
    json_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    
    # TIMESTAMP'ı TIMESTAMP türüne ÇEVIR (String'den)
    json_df = json_df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    )
    
    json_df_with_timestamp = json_df.withColumn(
        "processed_at",
        current_timestamp()
    )
    
except Exception as e:
    print(f"[ERROR] JSON parse hatası: {e}")
    sys.exit(1)

ihlal_df = json_df_with_timestamp.filter(
    col("hiz").isNotNull() & (col("hiz") > 120)
).withColumn(
    "uyari",
    when(col("hiz") > 160, lit("🚨 DİKKAT: Kritik hız ihlali (160+ km/h)"))
    .when(col("hiz") > 140, lit("⚠️  DİKKAT: Çok yüksek hız (140-160 km/h)"))
    .otherwise(lit("⚠️  DİKKAT: Hız limiti aşıldı (120-140 km/h)"))
)

def write_batch_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"[BATCH {batch_id}] Boş batch, skip")
        return
    
    try:
        print(f"[BATCH {batch_id}] {batch_df.count()} satır yazılıyor...")
        batch_df.write \
            .format("jdbc") \
            .option("url", PG_JDBC_URL) \
            .option("dbtable", PG_TABLE) \
            .option("user", PG_USER) \
            .option("password", PG_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", "1000") \
            .option("fetchsize", "1000") \
            .option("isolationLevel", "READ_COMMITTED") \
            .mode("append") \
            .save()
        print(f"[BATCH {batch_id}] ✅ Başarılı")
    except Exception as e:
        print(f"[BATCH {batch_id}] ❌ HATA: {e}")
        raise

print("[INFO] Konsol streaming başlıyor (Sadece hız ihlalleri)...")
query_console = ihlal_df.writeStream \
    .queryName("konsol_ihlalleri") \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 100) \
    .start()

print("[INFO] PostgreSQL streaming başlıyor (Tüm kayıtlar)...")
query_pg = json_df_with_timestamp.writeStream \
    .queryName("postgres_kayitlari") \
    .foreachBatch(write_batch_to_postgres) \
    .outputMode("append") \
    .start()

try:
    print("\n" + "="*60)
    print("✅ Streaming aktif! Durdurmak için Ctrl+C")
    print("="*60)
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n[INFO] Streaming durdurulması başladı...")
    spark.streams.stopAll()
    print("[INFO] Streaming durduruldu.")
