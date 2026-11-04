import os
import json
import time
import random
import socket
import warnings
import logging
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from openaq_producer import MONITORING_CITIES, fetch_real_air_quality
from sensor_simulator import STATIONS, generate_sensor_record

warnings.filterwarnings("ignore", category=DeprecationWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [BIGDATA-STREAM] - %(levelname)s - %(message)s')
# Tắt log traceback nội bộ của thư viện kafka-python khi chuyển đổi dự phòng (Failover)
logging.getLogger('kafka').setLevel(logging.CRITICAL)

# Tự động nhận diện địa chỉ Broker từ biến môi trường (trong Docker Airflow) hoặc localhost (trên WSL)
DEFAULT_BROKERS = 'localhost:19092,localhost:19093,localhost:19094'
REDPANDA_BOOTSTRAP_SERVERS = [
    b.strip() for b in os.getenv('REDPANDA_BROKERS', DEFAULT_BROKERS).split(',') if b.strip()
]
TOPIC_NAME = 'air_quality_stream'

def get_alive_brokers():
    alive = []
    for srv in REDPANDA_BOOTSTRAP_SERVERS:
        host, port_str = srv.split(':')
        try:
            with socket.create_connection((host, int(port_str)), timeout=0.8):
                alive.append(srv)
        except Exception:
            pass
    return alive

def ensure_topic_exists(bootstrap_servers):
    """Khởi tạo chủ đề air_quality_stream với 3 phân vùng (Partitions = 3) và hệ số nhân bản 3 (Replication Factor = 3)"""
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, request_timeout_ms=4000)
        existing_topics = admin.list_topics()
        if TOPIC_NAME not in existing_topics:
            topic = NewTopic(name=TOPIC_NAME, num_partitions=3, replication_factor=3)
            admin.create_topics(new_topics=[topic], validate_only=False)
            logging.info(f"[OK] Đã khởi tạo Topic '{TOPIC_NAME}' (num_partitions=3, replication_factor=3)")
        admin.close()
    except TopicAlreadyExistsError:
        pass
    except Exception as e:
        logging.debug(f"Bỏ qua bước tạo mới Topic (đã tồn tại hoặc đang đồng bộ): {e}")

def create_producer():
    alive_servers = get_alive_brokers()
    if len(alive_servers) < 2:
        logging.error(
            f"[MAT QUORUM RAFT] Chỉ còn {len(alive_servers)}/3 Broker sống ({alive_servers}). "
            f"Cụm 3 Node cần tối thiểu 2/3 Broker hoạt động (Quorum = N/2 + 1) để bầu chọn Leader!"
        )
        return None

    ensure_topic_exists(alive_servers)

    try:
        producer = KafkaProducer(
            bootstrap_servers=alive_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            retry_backoff_ms=300,
            request_timeout_ms=4000,
            metadata_max_age_ms=2000,
            linger_ms=10,
            batch_size=32768,
            acks=1
        )
        logging.info(f"[OK] Đã kết nối Cụm Redpanda (Brokers đang sống {len(alive_servers)}/3: {alive_servers})")
        return producer
    except Exception as e:
        logging.error(f"Lỗi kết nối Redpanda Cluster: {e}")
        return None

def fetch_all_cities_parallel():
    """Gọi API Open-Meteo của cả 5 thành phố song song cùng lúc (nhanh gấp 5 lần gọi tuần tự)"""
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_real_air_quality, city) for city in MONITORING_CITIES]
        for fut in futures:
            try:
                data = fut.result(timeout=5)
                if data:
                    results.append(data)
            except Exception:
                pass
    return results

def start_high_throughput_streaming(producer=None, duration_seconds=180):
    """
    Bơm luồng dữ liệu tốc độ cao: Mỗi chu kỳ (0.5 giây) đẩy 50 sự kiện cảm biến (~100 bản ghi/giây)
    kết hợp gọi song song API thực tế 5 thành phố.
    """
    if producer is None:
        producer = create_producer()

    end_time = time.time() + duration_seconds
    cycle = 0
    total_sent = 0

    logging.info(f"=== BẮT ĐẦU BƠM DỮ LIỆU TỐC ĐỘ CAO (500 TRẠM -> 3 REDPANDA BROKERS) TRONG {duration_seconds} GIÂY ===")

    while time.time() < end_time:
        cycle += 1

        try:
            if producer is None:
                producer = create_producer()
                if producer is None:
                    time.sleep(2.0)
                    continue

            # Mỗi đợt (0.5s) chọn ngẫu nhiên 50 trạm cảm biến rải khắp cả nước (~100 sự kiện/giây)
            batch_stations = random.sample(STATIONS, 50)
            for st in batch_stations:
                sensor_data = generate_sensor_record(st)
                producer.send(TOPIC_NAME, sensor_data)

            # Cứ mỗi 10 chu kỳ (5 giây) gọi song song API quan trắc thực tế 5 thành phố
            extra = 0
            if cycle % 10 == 1:
                real_records = fetch_all_cities_parallel()
                for real_data in real_records:
                    producer.send(TOPIC_NAME, real_data)
                    extra += 1

            producer.flush(timeout=4.0)
            total_sent += (50 + extra)
            logging.info(f"[Đợt #{cycle}] Đã bắn {50 + extra} bản ghi vào Cụm Redpanda -> Tổng cộng: {total_sent} sự kiện")
        except Exception:
            alive_now = get_alive_brokers()
            logging.warning(
                f"[FAILOVER Đợt #{cycle}] Phát hiện sự cố Broker! "
                f"Các Broker đang sống ({len(alive_now)}/3): {alive_now}. Đang chờ bầu chọn lại Raft Leader..."
            )
            try:
                producer.close(timeout=0.5)
            except Exception:
                pass
            time.sleep(3.0)
            producer = create_producer()

        time.sleep(0.5)

    logging.info(f"=== HOÀN TẤT BƠM {total_sent} SỰ KIỆN BIG DATA VÀO HỆ THỐNG ===")
    if producer:
        try:
            producer.close(timeout=1.0)
        except Exception:
            pass

if __name__ == "__main__":
    prod = create_producer()
    start_high_throughput_streaming(prod, duration_seconds=180)
