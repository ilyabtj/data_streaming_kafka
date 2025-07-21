import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

consumer = KafkaConsumer(
    "ilya_iot_sensor_data",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

conn = psycopg2.connect(
    dbname="postgres_db",
    user="postgres_user",
    password="postgres_password",
    host="5.189.154.248",
    port="5432",
)
cur = conn.cursor()

print("Consumer started....")

for msg in consumer:
    data = msg.value
    device_id = data["device_id"]

    cur.execute(
        """
        SELECT device_name, location, manufacturer
        FROM ilya_device_metadata
        WHERE device_id = %s
    """,
        (device_id,),
    )
    metadata = cur.fetchone()

    if metadata:
        device_name, location, manufacturer = metadata
    else:
        device_name = location = manufacturer = "Tidak Dikenal"

    cur.execute(
        """
        INSERT INTO ilya_iot_sensor_readings (
            device_id, device_name, temperature, humidity,
            timestamp, location, manufacturer
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """,
        (
            device_id,
            device_name,
            data["temperature"],
            data["humidity"],
            data["timestamp"],
            location,
            manufacturer,
        ),
    )
    conn.commit()
    print(f"[Stored] {device_id} @ {data['timestamp']}")
