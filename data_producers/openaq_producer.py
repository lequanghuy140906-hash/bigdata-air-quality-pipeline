import requests
import json
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [API-SOURCE] - %(levelname)s - %(message)s')

# Danh sách các trạm quan trắc thực tế tại Việt Nam
MONITORING_CITIES = [
    {"city": "Ho Chi Minh City", "station_id": "STATION_HCM_01", "lat": 10.8231, "lon": 106.6297},
    {"city": "Ha Noi", "station_id": "STATION_HN_01", "lat": 21.0285, "lon": 105.8542},
    {"city": "Da Nang", "station_id": "STATION_DN_01", "lat": 16.0544, "lon": 108.2022},
    {"city": "Can Tho", "station_id": "STATION_CT_01", "lat": 10.0452, "lon": 105.7469},
    {"city": "Hai Phong", "station_id": "STATION_HP_01", "lat": 20.8449, "lon": 106.6881}
]

def fetch_real_air_quality(city_info: dict) -> dict:
    """
    Gọi Open-Meteo Air Quality API lấy số liệu đo đạc thực tế
    """
    url = (
        f"https://air-quality-api.open-meteo.com/v1/air-quality?"
        f"latitude={city_info['lat']}&longitude={city_info['lon']}&"
        f"current=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone"
    )
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json().get("current", {})
            record = {
                "source": "REAL_OPEN_METEO_API",
                "station_id": city_info["station_id"],
                "station_name": f"Trạm khí tượng {city_info['city']}",
                "city": city_info["city"],
                "latitude": float(city_info["lat"]),
                "longitude": float(city_info["lon"]),
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "pm25": float(data.get("pm2_5") or 0.0),
                "pm10": float(data.get("pm10") or 0.0),
                "co": float(data.get("carbon_monoxide") or 0.0),
                "no2": float(data.get("nitrogen_dioxide") or 0.0),
                "so2": float(data.get("sulphur_dioxide") or 0.0),
                "ozone": float(data.get("ozone") or 0.0),
                "temperature": 29.5,
                "humidity": 75.0,
                "traffic_density": "NORMAL",
                "is_anomaly": False
            }
            return record
    except Exception as e:
        logging.error(f"Lỗi khi gọi API cho {city_info['city']}: {e}")
    return None

if __name__ == "__main__":
    logging.info("Bắt đầu chạy thử nghiệm thu thập dữ liệu từ Open-Meteo API...")
    for city in MONITORING_CITIES:
        data = fetch_real_air_quality(city)
        if data:
            logging.info(f"Dữ liệu nhận được: {json.dumps(data, ensure_ascii=False)}")
        time.sleep(1)
