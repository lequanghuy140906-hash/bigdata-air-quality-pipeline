import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, FloatType, StringType, BooleanType
)
from pyspark.sql.functions import from_json, col, when, to_timestamp

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [SPARK-P1] - %(levelname)s - %(message)s')

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:19092,kafka2:19093,kafka3:19094")

def create_spark_session():
    """
    Tạo SparkSession nạp các connector Kafka và Cassandra
    """
    spark = (
        SparkSession.builder
        .appName("Pipeline1_SparkStreaming")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.auth.username", "cassandra")
        .config("spark.cassandra.auth.password", "cassandra")
        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_stream():
    spark = create_spark_session()

    # Định nghĩa cấu trúc Schema JSON cảm biến
    schema = StructType([
        StructField("source", StringType(), True),
        StructField("station_id", StringType(), False),
        StructField("station_name", StringType(), True),
        StructField("city", StringType(), False),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("timestamp", StringType(), False),
        StructField("pm25", FloatType(), True),
        StructField("pm10", FloatType(), True),
        StructField("co", FloatType(), True),
        StructField("no2", FloatType(), True),
        StructField("so2", FloatType(), True),
        StructField("ozone", FloatType(), True),
        StructField("temperature", FloatType(), True),
        StructField("humidity", FloatType(), True),
        StructField("traffic_density", StringType(), True),
        StructField("is_anomaly", BooleanType(), True)
    ])

    # 1. Đọc stream từ Apache Kafka topic p1_air_quality
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "p1_air_quality")
        .option("startingOffsets", "earliest")
        .load()
    )

    # 2. Parse JSON, Làm sạch dữ liệu (Data Cleaning) và gán nhãn AQI bằng Spark
    parsed_df = (
        raw_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
        # Làm sạch 1: Loại bỏ bản ghi lỗi định dạng JSON hoặc thiếu khóa chính (Primary Key)
        .filter(col("city").isNotNull() & col("station_id").isNotNull() & col("timestamp").isNotNull())
        # Làm sạch 2: Xử lý dữ liệu khuyết (Missing Values) bằng giá trị mặc định
        .na.fill(0.0, subset=["pm25", "pm10", "co", "no2", "so2", "ozone", "temperature", "humidity"])
        .na.fill("UNKNOWN", subset=["source", "station_name", "traffic_density"])
        .na.fill(False, subset=["is_anomaly"])
        # Làm sạch 3: Lọc bỏ các giá trị cảm biến âm bất hợp lý (Nhiễu phần cứng)
        .filter((col("pm25") >= 0.0) & (col("pm10") >= 0.0))
        # Chuyển đổi kiểu dữ liệu thời gian và phân loại mức độ ô nhiễm AQI
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn(
            "aqi_status",
            when(col("pm25") > 150.0, "NGUY HẠI (CẢNH BÁO ĐỎ)")
            .when(col("pm25") > 100.0, "RẤT KÉM (CAM ĐẬM)")
            .when(col("pm25") > 50.0,  "KÉM (VÀNG)")
            .otherwise("TỐT (XANH LÁ)")
        )
    )

    logging.info("Đang bắt đầu ghi luồng Structured Streaming vào Cassandra smart_city.air_quality_stream...")

    # 3. Ghi vào Cassandra Table
    query = (
        parsed_df.writeStream
        .format("org.apache.spark.sql.cassandra")
        .outputMode("append")
        .options(table="air_quality_stream", keyspace="smart_city")
        .option("checkpointLocation", "/tmp/spark_checkpoint_p1")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    process_stream()
