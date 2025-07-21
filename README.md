# 📡 Kafka Streaming Project: IoT Sensor Data with Enrichment

## 👤 Author
**Ilya Aryaputra**  
BTJ Academy — Data Streaming & Ingestion Module

---

## 📘 Deskripsi Proyek

Proyek ini merupakan simulasi alur **real-time data streaming** menggunakan **Apache Kafka** dan **PostgreSQL**.  
Tujuan utamanya buat mengirimkan data sensor IoT secara kontinu, melakukan **enrichment** berdasarkan metadata, lalu menyimpan hasilnya ke database.

---

## 🧱 Arsitektur Sederhana

```text
[Producer.py] --> [Kafka Topic: ilya_iot_sensor_data] --> [Consumer.py] --> [PostgreSQL Table: ilya_iot_sensor_readings]
                                ↑
                         Enrichment via lookup
                    (Table: ilya_device_metadata)
