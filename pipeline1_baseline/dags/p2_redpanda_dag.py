from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys
import os
import socket
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
}

def check_redpanda_quorum_func():
    """Tác vụ 1: Kiểm tra trạng thái Quorum Raft (tối thiểu 2/3 Brokers sống) của cụm Redpanda"""
    brokers = [('redpanda-1', 9092), ('redpanda-2', 9092), ('redpanda-3', 9092)]
    alive = []
    for host, port in brokers:
        try:
            with socket.create_connection((host, port), timeout=3):
                alive.append(f"{host}:{port}")
        except Exception as e:
            logging.warning(f"Broker {host}:{port} không phản hồi: {e}")

    logging.info(f"Trạng thái Cụm Redpanda: {len(alive)}/3 Brokers đang hoạt động ({alive})")
    if len(alive) < 2:
        raise RuntimeError(f"[MAT QUORUM RAFT] Chỉ có {len(alive)}/3 Broker hoạt động. Cần tối thiểu 2/3 Broker!")
    return True

def check_openmeteo_api_func():
    """Tác vụ 2: Kiểm tra trạng thái sẵn sàng của nguồn dữ liệu thực tế Open-Meteo API"""
    import urllib.request
    url = "https://air-quality-api.open-meteo.com/v1/air-quality?latitude=10.8231&longitude=106.6297&current=pm2_5"
    req = urllib.request.urlopen(url, timeout=10)
    if req.getcode() == 200:
        logging.info("Nguồn dữ liệu Open-Meteo Air Quality API hoạt động ổn định (HTTP 200).")
        return True
    raise RuntimeError(f"Open-Meteo API trả về mã lỗi: {req.getcode()}")

def verify_clickhouse_schema_func():
    """Tác vụ 3: Kiểm tra kết nối và tự động khởi tạo bảng MergeTree / Kafka Engine / Materialized View trên ClickHouse"""
    import urllib.request
    import urllib.parse
    import base64

    sql_path = '/opt/airflow/pipeline2_modern/clickhouse_init.sql'
    if not os.path.exists(sql_path):
        logging.info("Bỏ qua bước đọc file SQL vì không tìm thấy đường dẫn mount, kiểm tra HTTP ping...")
        req = urllib.request.urlopen("http://clickhouse-server:8123/ping", timeout=5)
        return req.getcode() == 200

    auth_str = base64.b64encode(b"default:clickhouse123").decode('utf-8')

    # Kiểm tra nếu Materialized View đã tồn tại thì giữ nguyên kết nối Consumer đang chạy để không mất thời gian Rebalance
    check_req = urllib.request.Request(
        "http://clickhouse-server:8123/",
        data=b"EXISTS VIEW default.air_quality_mv",
        headers={'Authorization': f'Basic {auth_str}'},
        method='POST'
    )
    try:
        with urllib.request.urlopen(check_req, timeout=5) as resp:
            if resp.read().decode('utf-8').strip() == '1':
                logging.info("Bảng và Materialized View trên ClickHouse đã sẵn sàng. Giữ nguyên kết nối Consumer nóng!")
                return True
    except Exception:
        pass

    with open(sql_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    # Tách các câu lệnh SQL theo dấu chấm phẩy
    statements = [s.strip() for s in sql_content.split(';') if s.strip()]

    for stmt in statements:
        # Bỏ qua các dòng comment đầu câu lệnh
        lines = [line for line in stmt.splitlines() if not line.strip().startswith('--')]
        clean_query = '\n'.join(lines).strip()
        if not clean_query:
            continue
        req = urllib.request.Request(
            "http://clickhouse-server:8123/",
            data=clean_query.encode('utf-8'),
            headers={'Authorization': f'Basic {auth_str}'},
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            logging.info(f"Thực thi ClickHouse DDL thành công (HTTP {resp.getcode()}): {clean_query[:60]}...")
    return True

def stream_to_redpanda_func():
    """Tác vụ 4: Bơm luồng dữ liệu đa nguồn (Open-Meteo API + 500 trạm IoT) vào Cụm 3 Redpanda Brokers"""
    import subprocess
    try:
        import kafka
        import requests
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "kafka-python", "requests"])

    producers_dir = '/opt/airflow/data_producers'
    if os.path.exists(producers_dir) and producers_dir not in sys.path:
        sys.path.append(producers_dir)

    os.environ['REDPANDA_BROKERS'] = 'redpanda-1:9092,redpanda-2:9092,redpanda-3:9092'
    from stream_to_redpanda import create_producer, start_high_throughput_streaming
    prod = create_producer()
    start_high_throughput_streaming(prod, duration_seconds=120)

with DAG(
    'p2_modern_redpanda_clickhouse_dag',
    default_args=default_args,
    description='Pipeline 2: Điều phối thu thập dữ liệu đa nguồn vào Cụm 3 Redpanda Brokers -> ClickHouse -> Grafana',
    schedule_interval='@hourly',
    catchup=False,
    tags=['smart_city', 'redpanda', 'clickhouse', 'pipeline2']
) as dag:

    t1_check_brokers = PythonOperator(
        task_id='1_check_redpanda_raft_quorum',
        python_callable=check_redpanda_quorum_func,
        dag=dag
    )

    t2_check_api = PythonOperator(
        task_id='2_check_openmeteo_api_source',
        python_callable=check_openmeteo_api_func,
        dag=dag
    )

    t3_init_clickhouse = PythonOperator(
        task_id='3_verify_clickhouse_streaming_mv',
        python_callable=verify_clickhouse_schema_func,
        dag=dag
    )

    t4_stream_data = PythonOperator(
        task_id='4_stream_500_iot_and_api_to_redpanda',
        python_callable=stream_to_redpanda_func,
        dag=dag
    )

    # Chuỗi phụ thuộc 4 tác vụ tuần tự trên Airflow Graph
    t1_check_brokers >> t2_check_api >> t3_init_clickhouse >> t4_stream_data
