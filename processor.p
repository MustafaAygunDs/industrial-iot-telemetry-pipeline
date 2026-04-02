import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Spark Oturumunu Başlat
# Paket sürümlerini senin loglarındaki 3.5.0 ile tam eşitledim
spark = SparkSession.builder \
    .appName("OtomobilHizDenetimi") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.2") \
    .getOrCreate()

# Log kalabalığını engellemek için sadece uyarıları göster
spark.sparkContext.setLogLevel("WARN")

print("🕵️ Spark Dedektifi çalışıyor, hız ihlalleri bekleniyor...")

# 2. Kafka'dan Veri Oku
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "otomobil_verisi") \
    .load()

# 3. JSON Şemasını Tanımla
schema = StructType([
    StructField("arac_id", StringType(), True),
    StructField("hiz", IntegerType(), True),
    StructField("motor_sicakligi", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# 4. Veriyi Çöz ve Filtrele (120 km/s üstü ihlaldir)
processed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .filter(col("hiz") > 120)

# 5. Ekrana Yazdır
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
