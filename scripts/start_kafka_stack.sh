#!/usr/bin/env bash
# WSL'de çalıştır: Zookeeper + Kafka + Spark processor (Producer ayrı terminalde)
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/home/mustafa/veri_projesi/kafka_2.12-3.6.1}"
SPARK_HOME="${SPARK_HOME:-/home/mustafa/spark-4.1.1-bin-hadoop3}"
PROJ="${PROJ:-/home/mustafa/veri_projesi}"

echo "==> 2181 / 9092 temizliği (sudo şifre isteyebilir)"
sudo fuser -k 2181/tcp 9092/tcp 2>/dev/null || true
sleep 2

echo "==> Zookeeper arka planda"
cd "$KAFKA_HOME"
nohup bin/zookeeper-server-start.sh config/zookeeper.properties >> /tmp/zookeeper.log 2>&1 &
echo "    log: /tmp/zookeeper.log"
for _ in $(seq 1 40); do
  ss -tln 2>/dev/null | grep -q ':2181' && break
  sleep 1
done
ss -tln 2>/dev/null | grep -q ':2181' || { echo "HATA: 2181 açılmadı"; tail -30 /tmp/zookeeper.log; exit 1; }
echo "    OK: 2181 dinliyor"

echo "==> Kafka arka planda"
nohup bin/kafka-server-start.sh config/server.properties >> /tmp/kafka.log 2>&1 &
echo "    log: /tmp/kafka.log"
for _ in $(seq 1 60); do
  ss -tln 2>/dev/null | grep -q ':9092' && break
  sleep 1
done
ss -tln 2>/dev/null | grep -q ':9092' || { echo "HATA: 9092 açılmadı"; tail -40 /tmp/kafka.log; exit 1; }
echo "    OK: 9092 dinliyor"

echo "==> Spark Streaming (processor.py)"
cd "$PROJ"
nohup "$SPARK_HOME/bin/spark-submit" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.3 \
  processor.py >> /tmp/spark-processor.log 2>&1 &
echo "    PID $!"
echo "    log: /tmp/spark-processor.log"
echo ""
echo "Tamam. Producer için: cd $PROJ && .venv/bin/python producer.py"
echo "Spark konsol çıktısı için: tail -f /tmp/spark-processor.log"
