import json
import time
import random
import logging
from kafka import KafkaProducer
import sys
import os

# Thêm đường dẫn thư mục data_producers
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
data_producers_dir = os.path.join(base_dir, 'data_producers')
if data_producers_dir not in sys.path:
    sys.path.append(data_producers_dir)
if '/opt/airflow/data_producers' not in sys.path:
    sys.path.append('/opt/airflow/data_producers')

from openaq_producer import MONITORING_CITIES, fetch_real_air_quality
from sensor_simulator import STATIONS, generate_sensor_record

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [P1-PRODUCER] - %(levelname)s - %(message)s')

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:19092,localhost:9092').split(',')
TOPIC_NAME = 'p1_air_quality'

def start_pipeline1_producer(duration_seconds=120):
    producer = None
    # Thử kết nối lần lượt các bootstrap server
    for server in KAFKA_BOOTSTRAP_SERVERS:
        server = server.strip()
        try:
            logging.info(f"Đang thử kết nối Kafka tại: {server}...")
            producer = KafkaProducer(
                bootstrap_servers=[server],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=5000
            )
            logging.info(f"Kết nối thành công tới Kafka tại: {server}")
            break
        except Exception as err:
            logging.warning(f"Không thể kết nối {server}: {err}")

    if not producer:
        logging.error("Lỗi: Không thể kết nối tới bất kỳ Kafka Broker nào!")
        return

    end_time = time.time() + duration_seconds
    cycle = 0
    total = 0

    logging.info(f"Bắt đầu đẩy dữ liệu vào Kafka topic '{TOPIC_NAME}' trong {duration_seconds} giây...")

    while time.time() < end_time:
        cycle += 1
        # Chọn ngẫu nhiên 20 trạm gửi vào Kafka
        batch = random.sample(STATIONS, 20)
        for st in batch:
            rec = generate_sensor_record(st)
            producer.send(TOPIC_NAME, rec)
            total += 1

        if cycle % 5 == 1:
            for city in MONITORING_CITIES:
                real_data = fetch_real_air_quality(city)
                if real_data:
                    producer.send(TOPIC_NAME, real_data)
                    total += 1

        producer.flush()
        logging.info(f"[Đợt #{cycle}] Đã bắn 20 bản ghi vào Kafka -> Tổng: {total}")
        time.sleep(1.0)

    logging.info("Hoàn tất phiên gửi dữ liệu cho Pipeline 1.")

if __name__ == "__main__":
    start_pipeline1_producer()
