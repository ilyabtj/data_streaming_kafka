import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

# Inisialisasi KafkaConsumer untuk membaca dari topic "ilya_iot_sensor_data"
try:
    consumer = KafkaConsumer(
        "ilya_iot_sensor_data",
        bootstrap_servers="localhost:9092",  # alamat broker Kafka
        auto_offset_reset="earliest",  # mulai baca dari pesan paling awal kalo belum ada offset
        value_deserializer=lambda m: json.loads(
            m.decode("utf-8")
        ),  # konversi pesan JSON ke dict
    )
except Exception as e:
    # Menangani error jika koneksi ke Kafka gagal
    print(f"[ERROR] Kafka connection failed: {e}")
    exit(1)

# Inisialisasi koneksi ke database PostgreSQL
try:
    conn = psycopg2.connect(
        dbname="postgres_db",  # nama database
        user="postgres_user",  # username database
        password="postgres_password",  # password
        host="5.189.154.248",  # alamat host PostgreSQL
        port="5432",  # port PostgreSQL
    )
    cur = conn.cursor()  # buat objek cursor untuk eksekusi query SQL
except Exception as e:
    # Menangani error jika koneksi ke PostgreSQL gagal
    print(f"[ERROR] PostgreSQL connection failed: {e}")
    exit(1)

print("Ilya's Consumer started. Listening...")

# Loop untuk memproses setiap pesan Kafka
for msg in consumer:
    try:
        data = msg.value  # Ngambil isi pesan Kafka sebagai dict
        device_id = data["device_id"]  # Ambil device_id dari data

        # Ambil metadata dari device berdasarkan device_id
        cur.execute(
            """
            SELECT device_name, location, manufacturer
            FROM ilya_device_metadata
            WHERE device_id = %s
            """,
            (device_id,),
        )
        metadata = cur.fetchone()  # Ambil satu baris hasil query

        # Jika metadata ditemukan, assign ke variabel terkait
        if metadata:
            device_name, location, manufacturer = metadata
        else:
            # Jika tidak ditemukan, gunakan fallback "Tidak Dikenal"
            device_name = location = manufacturer = "Tidak Dikenal"

        # Simpan data sensor + metadata ke dalam tabel PostgreSQL
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
        conn.commit()  # Commit transaksi untuk simpan ke database
        print(f"[Stored] {device_id} @ {data['timestamp']}")  # Log berhasil simpan

    except Exception as e:
        # Menangani error jika ada kegagalan dalam proses data atau SQL
        print(f"[ERROR] Processing message failed: {e}")
