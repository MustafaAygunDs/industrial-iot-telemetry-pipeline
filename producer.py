#!/usr/bin/env python3
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

MACHINES = {
    "Motor-01": {"rpm_baseline": 2500, "temp_baseline": 72},
    "Motor-02": {"rpm_baseline": 2400, "temp_baseline": 70},
    "CNC-01": {"rpm_baseline": 3100, "temp_baseline": 78},
    "CNC-02": {"rpm_baseline": 3000, "temp_baseline": 76},
    "Assembly-01": {"rpm_baseline": 1800, "temp_baseline": 65}
}

RPM_THRESHOLDS = {"normal": (0, 3000), "warning": (3000, 3500), "critical": (3500, 5000)}
TEMP_THRESHOLDS = {"normal": (55, 85), "warning": (85, 90), "critical": (90, 120)}

class IndustrialIoTProducer:
    def __init__(self, bootstrap_servers="localhost:9092", topic="sensor_telemetry"):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        logger.info(f"[INIT] Producer başlatıldı. Topic: {topic}")
    
    def generate_machine_telemetry(self, machine_id):
        baseline = MACHINES[machine_id]
        rpm = int(baseline["rpm_baseline"] + random.gauss(0, 150))
        rpm = max(0, min(5000, rpm))
        
        rpm_temp_effect = (rpm / 3000) * 10
        bearing_temperature = int(
            baseline["temp_baseline"] + rpm_temp_effect + random.gauss(0, 3)
        )
        bearing_temperature = max(55, min(120, bearing_temperature))
        
        return {
            "machine_id": machine_id,
            "rpm": rpm,
            "bearing_temperature": bearing_temperature,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def classify_anomaly(self, rpm, bearing_temperature):
        if rpm >= RPM_THRESHOLDS["critical"][0] or bearing_temperature >= TEMP_THRESHOLDS["critical"][0]:
            return "CRITICAL"
        elif rpm >= RPM_THRESHOLDS["warning"][0] or bearing_temperature >= TEMP_THRESHOLDS["warning"][0]:
            return "WARNING"
        else:
            return "NORMAL"
    
    def produce_telemetry(self, message_count=None, interval=2):
        counter = 0
        try:
            logger.info("[START] Sensör telemetri üretimi başladı...")
            while message_count is None or counter < message_count:
                machine_id = random.choice(list(MACHINES.keys()))
                telemetry = self.generate_machine_telemetry(machine_id)
                anomaly_class = self.classify_anomaly(telemetry["rpm"], telemetry["bearing_temperature"])
                
                status_icon = {"NORMAL": "✅", "WARNING": "⚠️", "CRITICAL": "🚨"}.get(anomaly_class, "❓")
                logger.info(
                    f"{status_icon} [{counter:05d}] {machine_id} | "
                    f"RPM: {telemetry['rpm']:4d} | "
                    f"Bearing: {telemetry['bearing_temperature']:3d}°C | "
                    f"Status: {anomaly_class}"
                )
                
                self.producer.send(self.topic, telemetry)
                counter += 1
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("\n[STOP] Üretim durduruldu (Ctrl+C)")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info("[CLEANUP] Producer kapatıldı")

if __name__ == "__main__":
    producer = IndustrialIoTProducer(bootstrap_servers="localhost:9092", topic="sensor_telemetry")
    producer.produce_telemetry(message_count=None, interval=2)
