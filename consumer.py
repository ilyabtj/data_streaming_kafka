import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

try:
    consumer = KafkaConsumer(
        "ilya_iot_sensor_data",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
except Exception as e:
    print(f"[ERROR] Kafka connection failed: {e}")
    exit(1)

try:
    conn = psycopg2.connect(
        dbname="postgres_db",
        user="postgres_user",
        password="postgres_password",
        host="5.189.154.248",
        port="5432",
    )
    cur = conn.cursor()
except Exception as e:
    print(f"[ERROR] PostgreSQL connection failed: {e}")
    exit(1)

print("Ilya's Consumer started. Listening...")

for msg in consumer:
    try:
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

    except Exception as e:
        print(f"[ERROR] Processing message failed: {e}")
