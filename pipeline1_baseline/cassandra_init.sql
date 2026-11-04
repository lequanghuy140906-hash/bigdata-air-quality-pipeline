-- Khởi tạo Keyspace smart_city cho Pipeline 1
CREATE KEYSPACE IF NOT EXISTS smart_city
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Bảng lưu trữ chuỗi thời gian cảm biến không khí
CREATE TABLE IF NOT EXISTS smart_city.air_quality_stream (
    source text,
    station_id text,
    station_name text,
    city text,
    latitude float,
    longitude float,
    timestamp timestamp,
    pm25 float,
    pm10 float,
    co float,
    no2 float,
    so2 float,
    ozone float,
    temperature float,
    humidity float,
    traffic_density text,
    is_anomaly boolean,
    aqi_status text,
    PRIMARY KEY ((city), timestamp, station_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, station_id ASC);
