import json
import random
import time
from kafka import KafkaProducer
from datetime import datetime

# Ini Kafka producer dengan konfigurasi koneksi dan serializer
try:
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
except Exception as e:
    print(f"[ERROR] Kafka connection failed: {e}")
    exit(1)

# List ID device yang akan digunakan untuk simulasi
device_ids = [f"device_{i}" for i in range(1, 6)]

print("Producer started. Ctrl+C to stop.")

# Loop untuk terus mengirimkan data sensor secara periodik
while True:
    # Simulasi data sensor
    data = {
        "device_id": random.choice(device_ids),
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2),
        "timestamp": datetime.utcnow().isoformat(),
    }

    # kirim data ke topik kafka
    try:
        producer.send("ilya_iot_sensor_data", value=data)
        print(f"[Produced] {data}")
    except Exception as e:
        print(f"[ERROR] Failed to send message: {e}")

    time.sleep(1)
