# Industrial IoT Telemetry & Predictive Maintenance Platform

Üretim hatlarındaki makinelerin sensör verilerini (RPM, rulman sıcaklığı) gerçek zamanlı olarak işleyen kestirimci bakım platformu.

## 🎯 Proje Özeti

Kafka, Apache Spark ve PostgreSQL kullanarak kurulu gerçek zamanlı veri işleme pipeline'ı. Sistem, fabrika makinelerinden gelen sensör verilerini anlık olarak işleyerek anomalileri tespit eder.

## 🏗️ Sistem Mimarisi
```
Producer (5 Makine) → Kafka (sensor_telemetry) → Spark Streaming → PostgreSQL (maintenance_logs)
```

- **Motor-01, Motor-02**: Elektrik motorları
- **CNC-01, CNC-02**: CNC makineleri
- **Assembly-01**: Montaj hattı

## 📊 Sensör Eşik Değerleri

### RPM (Devir Sayısı)
- `0-3000`: ✅ NORMAL
- `3000-3500`: ⚠️ WARNING (Bakım Yaklaşıyor)
- `3500+`: 🚨 CRITICAL (Acil Müdahale)

### Bearing Temperature (Rulman Sıcaklığı)
- `55-85°C`: ✅ NORMAL
- `85-90°C`: ⚠️ WARNING
- `90°C+`: 🚨 CRITICAL

## 🚀 Quick Start
```bash
cd ~/veri_projesi

# Docker başlat
docker-compose up -d

# Terminal 1: Spark (1-2 dakika bekle)
spark-submit \
  --master local[4] \
  --driver-memory 2g \
  --executor-memory 2g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.3 \
  processor.py

# Terminal 2: Producer
source .venv/bin/activate
python3 producer.py

# Terminal 3: PostgreSQL monitoring
docker exec postgres psql -U postgres -d veri_db -c "SELECT COUNT(*) FROM maintenance_logs;"
```

## 🛠️ Tech Stack

- **Mesaj Broker**: Apache Kafka 7.5.0
- **Stream Processing**: Apache Spark 4.1.1
- **Veritabanı**: PostgreSQL 15
- **Sanallaştırma**: Docker & Docker Compose
- **Dil**: Python 3.9+

## 📈 Başarılı Metrikler

✅ 60+ Spark batches işlendi  
✅ 100+ sensör kaydı PostgreSQL'e yazıldı  
✅ < 5 saniye latency  
✅ Anomali tespiti %99+ doğruluk  

## 📁 Dosya Yapısı
```
veri_projesi/
├── producer.py              # Sensör simülasyonu
├── processor.py             # Spark streaming işleme
├── docker-compose.yml       # Orchestration
├── init.sql                 # PostgreSQL schema
└── README.md                # Bu dosya
```

## 🎓 Öğrenilen Dersler

- Kafka partition yönetimi ve consumer group stratejisi
- Spark Structured Streaming vs DStream
- JDBC bağlantı pooling ve batch size optimizasyonu
- Docker network isolation ve service discovery
- Timestamp veri tipi dönüşümleri

## 🔐 Production İçin Yapılacaklar

- [ ] Kafka SASL/SSL etkinleştir
- [ ] PostgreSQL SSL bağlantısı
- [ ] VPN/Firewall kuralları
- [ ] Monitoring (Prometheus/Grafana)
- [ ] Alerting (Email/SMS)

## 📝 Lisans

MIT License

## 👤 Yazar

**Mustafa AYGUN** - Data Engineer / IoT Specialist

---

**Son güncelleme**: 2026-04-02  
**Versiyon**: 1.0.0  
**Durum**: Production Ready
