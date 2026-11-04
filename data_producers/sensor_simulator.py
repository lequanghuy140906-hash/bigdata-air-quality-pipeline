import json
import time
import random
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [SIMULATOR] - %(levelname)s - %(message)s')

# Cấu hình 500 trạm cảm biến phân bổ toàn quốc
CITY_HUBS = [
    {"city": "TP. Hồ Chí Minh", "prefix": "HCM", "count": 200, "lat": 10.7769, "lon": 106.7009, "radius": 0.12},
    {"city": "Hà Nội",          "prefix": "HN",  "count": 150, "lat": 21.0285, "lon": 105.8542, "radius": 0.12},
    {"city": "Đà Nẵng",         "prefix": "DN",  "count": 60,  "lat": 16.0544, "lon": 108.2022, "radius": 0.08},
    {"city": "Hải Phòng",       "prefix": "HP",  "count": 50,  "lat": 20.8449, "lon": 106.6881, "radius": 0.07},
    {"city": "Cần Thơ",         "prefix": "CT",  "count": 40,  "lat": 10.0452, "lon": 105.7469, "radius": 0.07}
]

def generate_nationwide_stations():
    """
    Khởi tạo mạng lưới 500 trạm cảm biến IoT thông minh phủ kín toàn quốc
    """
    stations = []
    total_idx = 0
    for hub in CITY_HUBS:
        for i in range(1, hub["count"] + 1):
            total_idx += 1
            lat = round(hub["lat"] + random.uniform(-hub["radius"], hub["radius"]), 6)
            lon = round(hub["lon"] + random.uniform(-hub["radius"], hub["radius"]), 6)
            stations.append({
                "station_id": f"IOT_{hub['prefix']}_{i:03d}",
                "station_name": f"Trạm cảm biến {hub['city']} #{i:03d}",
                "city": hub["city"],
                "latitude": lat,
                "longitude": lon
            })
    return stations

STATIONS = generate_nationwide_stations()

def generate_sensor_record(station: dict) -> dict:
    """
    Sinh sự kiện dữ liệu cảm biến đo đạc theo thời gian thực
    Mô phỏng 12% xác suất xuất hiện điểm nóng ô nhiễm khẩn cấp (Anomaly / Peak Pollution)
    """
    is_anomaly = random.random() < 0.12

    if is_anomaly:
        pm25 = round(random.uniform(151.0, 320.0), 2)
        pm10 = round(random.uniform(200.0, 420.0), 2)
        co = round(random.uniform(1500.0, 3500.0), 2)
        traffic_density = "CONGESTION"
    else:
        pm25 = round(random.uniform(15.0, 68.0), 2)
        pm10 = round(random.uniform(25.0, 95.0), 2)
        co = round(random.uniform(300.0, 750.0), 2)
        traffic_density = random.choice(["LOW", "NORMAL", "HIGH"])

    no2 = round(random.uniform(10.0, 65.0), 2)
    so2 = round(random.uniform(5.0, 30.0), 2)
    ozone = round(random.uniform(20.0, 80.0), 2)
    temperature = round(random.uniform(25.0, 36.5), 1)
    humidity = round(random.uniform(55.0, 92.0), 1)

    record = {
        "source": "IOT_NATIONWIDE_SENSOR",
        "station_id": station["station_id"],
        "station_name": station["station_name"],
        "city": station["city"],
        "latitude": station["latitude"],
        "longitude": station["longitude"],
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "pm25": pm25,
        "pm10": pm10,
        "co": co,
        "no2": no2,
        "so2": so2,
        "ozone": ozone,
        "temperature": temperature,
        "humidity": humidity,
        "traffic_density": traffic_density,
        "is_anomaly": is_anomaly
    }
    return record

if __name__ == "__main__":
    logging.info(f"Đã khởi tạo mạng lưới {len(STATIONS)} trạm cảm biến IoT trên toàn quốc.")
    for _ in range(5):
        sample = random.choice(STATIONS)
        data = generate_sensor_record(sample)
        logging.info(f"Dữ liệu sinh: {json.dumps(data, ensure_ascii=False)}")
        time.sleep(0.3)
