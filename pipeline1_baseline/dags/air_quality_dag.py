from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys
import os
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
}

def check_kafka_cluster_func():
    """Tác vụ 1: Kiểm tra kết nối tới cụm Kafka Brokers trước khi bơm dữ liệu"""
    import socket
    brokers = [('kafka1', 19092), ('localhost', 9092)]
    for host, port in brokers:
        try:
            with socket.create_connection((host, port), timeout=5):
                logging.info(f"Kết nối thành công tới Kafka Broker tại {host}:{port}")
                return True
        except Exception as e:
            logging.warning(f"Không thể kết nối {host}:{port} -> {e}")
    raise RuntimeError("Không thể kết nối tới cụm Kafka! Hủy chu kỳ chạy để chờ retry.")

def check_openmeteo_api_func():
    """Tác vụ 2: Kiểm tra trạng thái sẵn sàng của nguồn dữ liệu thực tế Open-Meteo API"""
    import urllib.request
    url = "https://air-quality-api.open-meteo.com/v1/air-quality?latitude=10.8231&longitude=106.6297&current=pm2_5"
    req = urllib.request.urlopen(url, timeout=10)
    if req.getcode() == 200:
        logging.info("Nguồn dữ liệu Open-Meteo Air Quality API hoạt động ổn định (HTTP 200).")
        return True
    raise RuntimeError(f"Open-Meteo API trả về mã lỗi: {req.getcode()}")

def stream_iot_to_kafka_func():
    """Tác vụ 3: Thu thập dữ liệu đa nguồn (API + 500 trạm IoT) và đẩy vào Kafka"""
    import subprocess
    import sys
    
    # Đảm bảo cài đặt thư viện cần thiết khi worker thực thi task
    try:
        import kafka
        import requests
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "kafka-python", "requests"])

    # Bổ sung đường dẫn thư mục code vào Python path
    paths_to_add = [
        '/opt/airflow/pipeline1_baseline',
        '/opt/airflow/data_producers',
        os.path.abspath(os.path.join(os.path.dirname(__file__), '..')),
        os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data_producers'))
    ]
    for p in paths_to_add:
        if os.path.exists(p) and p not in sys.path:
            sys.path.append(p)

    from producer_to_kafka import start_pipeline1_producer
    # Bắn dữ liệu mô phỏng trong 60 giây mỗi lần trigger
    start_pipeline1_producer(duration_seconds=60)

with DAG(
    'p1_smart_city_air_quality_dag',
    default_args=default_args,
    description='Quy trình thu thập và điều phối dữ liệu chất lượng không khí đa nguồn vào Kafka',
    schedule_interval='@hourly',
    catchup=False,
    tags=['smart_city', 'iot', 'kafka', 'pipeline1']
) as dag:

    check_kafka_task = PythonOperator(
        task_id='1_check_kafka_brokers_health',
        python_callable=check_kafka_cluster_func,
        dag=dag
    )

    check_api_task = PythonOperator(
        task_id='2_check_openmeteo_api_source',
        python_callable=check_openmeteo_api_func,
        dag=dag
    )

    stream_task = PythonOperator(
        task_id='3_stream_iot_and_api_to_kafka',
        python_callable=stream_iot_to_kafka_func,
        dag=dag
    )

    # Thiết lập luồng phụ thuộc giữa các tác vụ (Task Dependencies)
    check_kafka_task >> check_api_task >> stream_task
