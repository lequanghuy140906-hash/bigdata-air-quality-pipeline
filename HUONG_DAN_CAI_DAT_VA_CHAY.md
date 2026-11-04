# HƯỚNG DẪN CÀI ĐẶT, TRIỂN KHAI VÀ VẬN HÀNH HỆ THỐNG

Tài liệu này hướng dẫn chi tiết từng bước cài đặt môi trường, khởi chạy hạ tầng Docker Compose, nạp lược đồ cơ sở dữ liệu, kích hoạt luồng thu thập dữ liệu và thực hiện các kịch bản kiểm thử cho đề tài **Hệ thống thu thập và phân tích dữ liệu chất lượng không khí từ API và cảm biến IoT giả lập**.

---

## 1. YÊU CẦU MÔI TRƯỜNG (PREREQUISITES)

1. **Hệ điều hành:** Windows 10/11 (đã bật WSL2) hoặc Linux (Ubuntu 20.04/22.04+).
2. **Docker & Docker Compose:**
   - Docker Engine phiên bản `24.0+`
   - Docker Compose phiên bản `v2.0+`
   - Dung lượng RAM cấp phát cho Docker tối thiểu `4 GB` (Pipeline 2 chỉ tiêu thụ `~1,03 GiB RAM`).
3. **Python & Thư viện phụ thuộc (nếu chạy Producer/Test trực tiếp ngoài host):**
   - Python `3.8+` (khuyên dùng Python `3.10` hoặc `3.11`).
   - Cài đặt các gói thư viện từ tệp `requirements.txt`:
     ```bash
     pip install -r requirements.txt
     ```

---

## 2. QUY TRÌNH KHỞI CHẠY PIPELINE 2 (KIẾN TRÚC TRỌNG TÂM: AIRFLOW + REDPANDA + CLICKHOUSE + GRAFANA)

### Bước 2.1. Tải kho mã nguồn và di chuyển vào thư mục dự án
```bash
git clone https://github.com/pquang0407/bigdata-air-quality-pipeline.git
cd bigdata-air-quality-pipeline
```

### Bước 2.2. Khởi động cụm 9 container của Pipeline 2
```bash
docker compose -f docker-compose-pipeline2.yml up -d
```

Kiểm tra trạng thái các container đang chạy:
```bash
docker compose -f docker-compose-pipeline2.yml ps
```
Danh sách 9 container của Pipeline 2 gồm:
- `redpanda-1`, `redpanda-2`, `redpanda-3` (Cụm 3 Broker Redpanda đồng thuận Raft)
- `redpanda-console` (Giao diện quản trị cụm Redpanda tại cổng `8088`)
- `clickhouse-server` (Cơ sở dữ liệu phân tích cột ClickHouse tại cổng `8123` / `9000`)
- `grafana` (Giao diện trực quan hóa bản đồ nhiệt và biểu đồ tại cổng `3000`)
- `p2-postgres`, `p2-airflow_webserver`, `p2-airflow_scheduler` (Hệ thống điều phối Apache Airflow tại cổng `8089`)

### Bước 2.3. Khởi tạo bảng hàng đợi Kafka Engine, bảng đích MergeTree và Materialized View trên ClickHouse
Chạy câu lệnh dưới đây để nạp tệp cấu hình DDL `pipeline2_modern/clickhouse_init.sql` vào container `clickhouse-server`:
```bash
docker exec -i clickhouse-server clickhouse-client --multiquery < pipeline2_modern/clickhouse_init.sql
```

Kiểm tra nhanh 3 đối tượng đã được tạo trong ClickHouse (`air_quality_queue`, `air_quality_data`, `air_quality_mv`):
```bash
docker exec -it clickhouse-server clickhouse-client -q "SHOW TABLES FROM default;"
```

### Bước 2.4. Kích hoạt luồng thu thập dữ liệu (Open-Meteo API + 500 cảm biến IoT)

Bạn có thể chọn **1 trong 2 phương thức** sau:

- **Phương thức A – Điều phối tự động qua giao diện Apache Airflow:**
  1. Mở trình duyệt truy cập: `http://localhost:8089`
  2. Đăng nhập với tài khoản: `airflow` / Mật khẩu: `airflow`
  3. Tại danh sách DAGs, bật công tắc (Unpause) cho DAG **`p2_modern_redpanda_clickhouse_dag`**.
  4. Nhấn nút **Trigger DAG** (biểu tượng nút Play ở góc phải) để Airflow thực thi tuần tự 4 tác vụ:
     - `1_verify_redpanda_cluster_health`
     - `2_verify_openmeteo_api_source`
     - `3_verify_clickhouse_olap_ready`
     - `4_stream_data_to_redpanda` (thu thập và phát luồng dữ liệu trong 120 giây)

- **Phương thức B – Chạy trực tiếp chương trình Producer trên cửa sổ dòng lệnh:**
  ```bash
  python3 data_producers/stream_to_redpanda.py
  ```

### Bước 2.5. Truy cập các giao diện giám sát và kiểm tra dữ liệu

1. **Redpanda Console (`http://localhost:8088`):**
   - Vào mục **Cluster** để kiểm tra trạng thái `3 of 3 Brokers Online`.
   - Vào mục **Topics** $\rightarrow$ `air_quality_stream` để xem các bản tin JSON 17 thuộc tính đang chảy vào theo thời gian thực.

2. **ClickHouse CLI:**
   - Kiểm tra tổng số bản ghi đã được Materialized View chuyển sang bảng đích `MergeTree`:
     ```bash
     docker exec -it clickhouse-server clickhouse-client -q "SELECT count(*) AS total_records FROM default.air_quality_data;"
     ```
   - Xem 10 bản ghi mới nhất kèm nhãn phân loại `aqi_status`:
     ```bash
     docker exec -it clickhouse-server clickhouse-client -q "SELECT city, station_id, pm25, aqi_status, timestamp FROM default.air_quality_data ORDER BY timestamp DESC LIMIT 10;"
     ```

3. **Grafana Dashboard (`http://localhost:3000`):**
   - Đăng nhập với tài khoản: `admin` / Mật khẩu: `admin`.
   - Vào mục **Dashboards** $\rightarrow$ mở **Real-time Air Quality Monitoring** để quan sát bản đồ nhiệt ArcGIS Geomap và các biểu đồ biến động bụi mịn $\text{PM}_{2.5}, \text{PM}_{10}$ tự động làm mới mỗi 5 giây.

---

## 3. HƯỚNG DẪN CHẠY CÁC KỊCH BẢN KIỂM THỬ (TESTING & FAULT TOLERANCE)

### 3.1. Kiểm thử xử lý dữ liệu lỗi cú pháp, khuyết thiếu, giá trị âm và trùng lặp (`TC-B1` .. `TC-B4`)
Khi cụm Pipeline 2 đang hoạt động, chạy tập lệnh kiểm thử tự động:
```bash
python3 benchmark/test_data_faults.py
```
Tập lệnh sẽ tự động phát 4 kịch bản dữ liệu bất thường vào Redpanda, đợi 5 giây để ClickHouse tiêu thụ và in trực tiếp bảng kết quả đối chiếu từ `default.air_quality_data` lên màn hình Terminal.

### 3.2. Kiểm thử khả năng chịu lỗi khi dừng đột ngột cơ sở dữ liệu ClickHouse
1. Mở một cửa sổ Terminal chạy Producer:
   ```bash
   python3 data_producers/stream_to_redpanda.py
   ```
2. Mở cửa sổ Terminal thứ hai và dừng đột ngột container ClickHouse:
   ```bash
   docker stop clickhouse-server
   ```
   - Quan sát cửa sổ Producer vẫn tiếp tục gửi bản tin vào Redpanda mà không bị văng ngoại lệ.
   - Mở `http://localhost:8088/topics/air_quality_stream` trên Redpanda Console để xác nhận các bản tin mới vẫn được Redpanda lưu đệm an toàn trên đĩa.
   - Mở `http://localhost:8089` và Trigger DAG trên Airflow để quan sát Tác vụ 3 tự động thử lại 3 lần (`Tries = 3`), chuyển sang `failed` và chặn Tác vụ 4 ở trạng thái `upstream_failed`.
3. Khởi động lại ClickHouse để hệ thống tự động đọc bù các bản tin tồn đọng từ Redpanda:
   ```bash
   docker start clickhouse-server
   ```

### 3.3. Đo lường mức tiêu thụ tài nguyên RAM (`docker stats`) và tốc độ truy vấn phân tích
- Đo dung lượng RAM tiêu thụ của toàn cụm container:
  ```bash
  docker stats --no-stream
  ```
- Đo tốc độ truy vấn gom nhóm (`GROUP BY`) trên ClickHouse:
  ```bash
  docker exec -it clickhouse-server clickhouse-client -q "SELECT station_id, count(*) AS records, round(avg(pm25), 2) AS avg_pm25 FROM default.air_quality_data GROUP BY station_id ORDER BY avg_pm25 DESC LIMIT 10;"
  ```

---

## 4. QUY TRÌNH KHỞI CHẠY PIPELINE 1 (KIẾN TRÚC ĐỐI CHỨNG: KAFKA + ZOOKEEPER + SPARK + CASSANDRA)

Để chạy Pipeline 1 phục vụ đối chứng trên cùng một máy trạm, trước tiên giải phóng tài nguyên của Pipeline 2:

```bash
docker compose -f docker-compose-pipeline2.yml down
```

1. **Khởi động cụm Kafka 3 Brokers, ZooKeeper, Cassandra và Airflow của Pipeline 1:**
   ```bash
   docker compose -f docker-compose-pipeline1.yml up -d
   docker compose -f docker-compose-pipeline1-airflow.yml up -d
   ```

2. **Tạo chủ đề trên Kafka và khởi tạo Keyspace/Bảng trên Cassandra:**
   ```bash
   docker exec -it p1-kafka1 kafka-topics --bootstrap-server localhost:9092 --create --topic p1_air_quality --partitions 3 --replication-factor 1
   docker exec -i p1-cassandra cqlsh -u cassandra -p cassandra < pipeline1_baseline/cassandra_init.sql
   ```

3. **Chạy tiến trình Spark Structured Streaming và Producer:**
   ```bash
   python3 pipeline1_baseline/spark_streaming_processor.py
   ```
   Mở thêm một cửa sổ Terminal để phát dữ liệu vào Kafka:
   ```bash
   python3 pipeline1_baseline/producer_to_kafka.py
   ```

4. **Kiểm tra thời gian truy vấn kèm Tracing trên Cassandra (`cqlsh`):**
   ```bash
   docker exec -it p1-cassandra cqlsh -u cassandra -p cassandra -e "TRACING ON; SELECT station_id, count(*) AS records, avg(pm25) AS avg_pm25 FROM smart_city.air_quality_stream GROUP BY station_id;"
   ```
