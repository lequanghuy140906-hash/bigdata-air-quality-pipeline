import streamlit as st
import pandas as pd

st.set_page_config(page_title="PIPELINE 1 - CASSANDRA DASHBOARD", layout="wide")

st.title("PIPELINE 1 (BASELINE): HE THONG GIAM SAT KAFKA - SPARK - CASSANDRA")
st.markdown("**Kien truc Truyen thong:** `Airflow / Producer` -> `Apache Kafka (3 Brokers)` -> `Spark Structured Streaming` -> `Cassandra`")

def load_cassandra_data():
    try:
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
    except ImportError:
        st.error("Thieu thu vien `cassandra-driver`. Vui long chay lenh: `pip install cassandra-driver` tren terminal.")
        return pd.DataFrame()

    try:
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(['localhost'], port=9042, auth_provider=auth_provider)
        session = cluster.connect('smart_city')
        query = "SELECT city, station_name, timestamp, pm25, pm10, co, aqi_status, source FROM air_quality_stream LIMIT 200;"
        rows = session.execute(query)
        df = pd.DataFrame(list(rows))
        return df
    except Exception as e:
        st.error(f"Loi ket noi Cassandra: {e}")
        return pd.DataFrame()

df = load_cassandra_data()

if not df.empty:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ban ghi hien thi", f"{len(df)} dong")
    c2.metric("PM2.5 Trung Binh", f"{round(df['pm25'].mean(), 2)} ug/m3")
    c3.metric("Canh bao O Nhiem Do", f"{len(df[df['pm25'] > 150])}")
    c4.metric("So Thanh Pho", f"{df['city'].nunique()}")

    st.write("---")
    st.subheader("Bang Du Lieu Thoi Gian Thuc Lay Tu Apache Cassandra (Keyspace: smart_city)")
    st.dataframe(df, use_container_width=True)
else:
    st.warning("Dang ket noi Cassandra va cho du lieu...")
