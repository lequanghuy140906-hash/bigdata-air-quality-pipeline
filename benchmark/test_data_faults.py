import json
import time
import socket
import logging
from datetime import datetime
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [FAULT-TEST] - %(levelname)s - %(message)s')
logging.getLogger('kafka').setLevel(logging.CRITICAL)

def get_bootstrap_servers():
    candidates = ['localhost:19092', 'localhost:19093', 'localhost:19094', 'redpanda-1:9092', 'redpanda-2:9092', 'redpanda-3:9092']
    alive = []
    for srv in candidates:
        host, port = srv.split(':')
        try:
            with socket.create_connection((host, int(port)), timeout=1.5):
                alive.append(srv)
        except Exception:
            pass
    return alive if alive else ['localhost:19092']

def run_data_quality_fault_tests(topic_name='air_quality_stream'):
    print("=" * 80)
    print(f"   KIEM THU DU LIEU LOI, THIEU VA TRUNG LAP (MUC 7.2) -> TOPIC: {topic_name}")
    print("=" * 80)

    servers = get_bootstrap_servers()
    raw_producer = KafkaProducer(
        bootstrap_servers=servers,
        request_timeout_ms=4000,
        acks=1
    )

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # TC-B1: Du lieu sai dinh dang JSON (Malformed JSON)
    print("\n[TC-B1] Gui ban tin sai dinh dang cu phap JSON (Corrupted Payload)...")
    corrupted_payload = b"{station_id: BROKEN_JSON_PAYLOAD_WITHOUT_QUOTES_12345"
    raw_producer.send(topic_name, corrupted_payload)
    raw_producer.flush()
    print("  -> Payload: {station_id: BROKEN_JSON_PAYLOAD_WITHOUT_QUOTES_12345")
    print("  -> Ky vong: ClickHouse Kafka Engine bo qua (kafka_skip_broken_messages=100), khong dung luong.")

    # TC-B2: Du lieu thieu thuoc tinh / NULL (Missing Values)
    print("\n[TC-B2] Gui ban tin bi khuyet gia tri do (pm25=null, pm10=null, co=null)...")
    missing_val_record = {
        "source": "FAULT_TEST_SUITE",
        "station_id": "IOT_TEST_MISSING",
        "station_name": "Tram Kiem Thu Du Lieu Khuyet",
        "city": "TP. Hồ Chí Minh",
        "latitude": 10.7769,
        "longitude": 106.7009,
        "timestamp": now_str,
        "pm25": None,
        "pm10": None,
        "co": None,
        "no2": 25.0,
        "so2": 10.0,
        "ozone": 30.0,
        "temperature": 30.0,
        "humidity": 75.0,
        "traffic_density": None,
        "is_anomaly": False
    }
    raw_producer.send(topic_name, json.dumps(missing_val_record).encode('utf-8'))
    raw_producer.flush()
    print("  -> Da gui ban tin station_id='IOT_TEST_MISSING' voi pm25=null, pm10=null, co=null.")

    # TC-B3: Du lieu trung lap do gui lai (Duplicate Messages)
    print("\n[TC-B3] Gui 2 ban tin trung lap cung (station_id='IOT_TEST_DUP_001', timestamp)...")
    dup_record = {
        "source": "FAULT_TEST_SUITE",
        "station_id": "IOT_TEST_DUP_001",
        "station_name": "Tram Kiem Thu Trung Lap",
        "city": "Hà Nội",
        "latitude": 21.0285,
        "longitude": 105.8542,
        "timestamp": now_str,
        "pm25": 185.5,
        "pm10": 240.0,
        "co": 1800.0,
        "no2": 45.0,
        "so2": 15.0,
        "ozone": 55.0,
        "temperature": 28.5,
        "humidity": 70.0,
        "traffic_density": "CONGESTION",
        "is_anomaly": True
    }
    raw_producer.send(topic_name, json.dumps(dup_record).encode('utf-8'))
    raw_producer.send(topic_name, json.dumps(dup_record).encode('utf-8'))
    raw_producer.flush()
    print(f"  -> Da gui 2 ban tin trung lap lien tiep (station_id='IOT_TEST_DUP_001', timestamp='{now_str}').")

    # TC-B4: Ban tin kiem chung hop le ngay sau cac ban tin loi
    print("\n[TC-B4] Gui ban tin kiem chung hop le ngay sau cac ban tin loi (Health Verification)...")
    valid_record = {
        "source": "FAULT_TEST_VERIFY",
        "station_id": "IOT_TEST_OK_999",
        "station_name": "Tram Xac Nhan Luong Hoan Hao",
        "city": "Đà Nẵng",
        "latitude": 16.0544,
        "longitude": 108.2022,
        "timestamp": now_str,
        "pm25": 42.0,
        "pm10": 65.0,
        "co": 410.0,
        "no2": 20.0,
        "so2": 8.0,
        "ozone": 35.0,
        "temperature": 29.0,
        "humidity": 78.0,
        "traffic_density": "NORMAL",
        "is_anomaly": False
    }
    raw_producer.send(topic_name, json.dumps(valid_record).encode('utf-8'))
    raw_producer.flush()
    raw_producer.close()
    print("  -> Da gui ban tin xac nhan station_id='IOT_TEST_OK_999' (pm25=42.0) thanh cong.")
    print("=" * 80)

if __name__ == "__main__":
    run_data_quality_fault_tests()
