-- 1. BẢNG ĐÍCH (TARGET TABLE): LƯU TRỮ DỮ LIỆU CẢM BIẾN VẬT LÝ DẠNG CỘT (MERGETREE)
CREATE TABLE IF NOT EXISTS default.air_quality_data (
    source LowCardinality(String),
    station_id LowCardinality(String),
    station_name String,
    city LowCardinality(String),
    latitude Float64,
    longitude Float64,
    timestamp DateTime,
    pm25 Float32,
    pm10 Float32,
    co Float32,
    no2 Float32,
    so2 Float32,
    ozone Float32,
    temperature Float32,
    humidity Float32,
    traffic_density LowCardinality(String),
    is_anomaly UInt8,
    -- Trường tính toán thêm: Phân loại mức độ ô nhiễm không khí theo tiêu chuẩn WHO/AQI
    aqi_status LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
PRIMARY KEY (station_id, timestamp)
ORDER BY (station_id, timestamp);


-- 2. BẢNG KAFKA ENGINE (QUEUE TABLE): KẾT NỐI VÀ HÚT DỮ LIỆU TRỰC TIẾP TỪ CỤM 3 REDPANDA BROKERS
DROP VIEW IF EXISTS default.air_quality_mv;
DROP TABLE IF EXISTS default.air_quality_queue;

CREATE TABLE IF NOT EXISTS default.air_quality_queue (
    source String,
    station_id String,
    station_name String,
    city String,
    latitude Float64,
    longitude Float64,
    timestamp String,
    pm25 Float32,
    pm10 Float32,
    co Float32,
    no2 Float32,
    so2 Float32,
    ozone Float32,
    temperature Float32,
    humidity Float32,
    traffic_density String,
    is_anomaly UInt8
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'redpanda-1:9092,redpanda-2:9092,redpanda-3:9092',
    kafka_topic_list = 'air_quality_stream',
    kafka_group_name = 'clickhouse_air_quality_group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 100;


-- 3. MATERIALIZED VIEW: BỘ NÃO XỬ LÝ DÒNG (STREAM PROCESSOR)
-- Tự động kích hoạt khi có message mới: ép kiểu DateTime và phân loại ô nhiễm theo mức độ PM2.5
CREATE MATERIALIZED VIEW IF NOT EXISTS default.air_quality_mv TO default.air_quality_data AS
SELECT
    source,
    station_id,
    station_name,
    city,
    latitude,
    longitude,
    parseDateTimeBestEffort(timestamp) AS timestamp,
    pm25,
    pm10,
    co,
    no2,
    so2,
    ozone,
    temperature,
    humidity,
    traffic_density,
    is_anomaly,
    multiIf(
        pm25 > 150.0, 'NGUY HẠI (CẢNH BÁO ĐỎ)',
        pm25 > 100.0, 'RẤT KÉM (CAM ĐẬM)',
        pm25 > 50.0,  'KÉM (VÀNG)',
        'TỐT (XANH LÁ)'
    ) AS aqi_status
FROM default.air_quality_queue;
