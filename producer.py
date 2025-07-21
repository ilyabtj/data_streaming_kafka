import json
import random
import time
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

device_ids = [f"device_{i}" for i in range(1, 6)]

print("Producer started. Ctrl+C to stop.")

while True:
    data = {
        "device_id": random.choice(device_ids),
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2),
        "timestamp": datetime.utcnow().isoformat(),
    }
    producer.send("ilya_iot_sensor_data", value=data)
    print(f"[Produced] {data}")
    time.sleep(1)
