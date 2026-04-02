#!/usr/bin/env python3
import json
import time
import random
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys

KAFKA_TOPIC = "hiz_verisi"
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

print("🚀 Kafka Producer Başlıyor...")
print(f"   Topic: {KAFKA_TOPIC}")
print(f"   Broker: {KAFKA_BOOTSTRAP_SERVERS}")

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3,
        max_in_flight_requests_per_connection=1
    )
except Exception as e:
    print(f"❌ Kafka bağlantısı başarısız: {e}")
    sys.exit(1)

araclar = ["Corolla-01", "Corolla-02", "Corolla-03", "Corolla-Hybrid"]

print("📡 Veri üretimi başladı... Durdurmak için Ctrl+C\n")

veri_sayisi = 0

try:
    while True:
        if random.random() < 0.15:
            hiz = random.randint(121, 165)
        else:
            hiz = random.randint(70, 120)
        
        veri = {
            "arac_id": random.choice(araclar),
            "hiz": hiz,
            "motor_sicakligi": random.randint(85, 105),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        def on_send_success(record_metadata):
            global veri_sayisi
            veri_sayisi += 1
            emoji = "⚠️ " if veri['hiz'] > 120 else "✅"
            print(
                f"{emoji} [{veri_sayisi:04d}] "
                f"Arac: {veri['arac_id']:15} | "
                f"Hiz: {veri['hiz']:3d} km/h | "
                f"Motor: {veri['motor_sicakligi']}°C | "
                f"Partition: {record_metadata.partition} | "
                f"Offset: {record_metadata.offset}"
            )
        
        def on_send_error(exc):
            print(f"❌ Gönderme hatası: {exc}")
        
        future = producer.send(KAFKA_TOPIC, veri)
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)
        
        time.sleep(2)

except KeyboardInterrupt:
    print("\n\n👋 Üretim durduruldu.")
    print(f"   Toplam gönderilen veri: {veri_sayisi}")
finally:
    producer.flush()
    producer.close()
    print("✅ Producer kapatıldı.")
