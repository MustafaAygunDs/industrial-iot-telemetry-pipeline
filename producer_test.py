from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# processor.py'daki isme (otomobil_verisi) sadık kalıyoruz:
topic_ismi = 'otomobil_verisi'

data = {
    "arac_id": "Corolla-Hybrid",
    "hiz": 150,  # 120'den büyük yapıyoruz ki filtreye takılmasın
    "motor_sicakligi": 92,
    "timestamp": "2026-03-23 17:40:00"
}

print(f"{topic_ismi} kanalına veri gönderiliyor...")
producer.send(topic_ismi, value=data)
producer.flush()
print("Veri başarıyla uçtu! Şimdi Terminal 1'e bak.")
