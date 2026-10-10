"""Hourly weather -> raw JSON -> partitioned Parquet -> MySQL. Daily Airflow DAG."""
import glob, json, os
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

DATA = os.getenv("DATA_DIR", "/opt/airflow/data")
RAW, PROCESSED = f"{DATA}/raw", f"{DATA}/processed/weather"
MYSQL_URL = os.getenv("MYSQL_URL")
API = "https://api.open-meteo.com/v1/forecast"          # free, no API key
CITIES = {"Chennai": (13.08, 80.27), "Bengaluru": (12.97, 77.59),
          "Mumbai": (19.08, 72.88), "Delhi": (28.61, 77.21)}
RENAME = {"time": "obs_time", "temperature_2m": "temp_c", "relative_humidity_2m": "humidity_pct",
          "precipitation": "rain_mm", "wind_speed_10m": "wind_kmh"}


def extract(ds, **_):
    out = f"{RAW}/dt={ds}"
    os.makedirs(out, exist_ok=True)
    for city, (lat, lon) in CITIES.items():
        r = requests.get(API, timeout=30, params={
            "latitude": lat, "longitude": lon, "timezone": "Asia/Kolkata",
            "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
            "start_date": ds, "end_date": ds})
        r.raise_for_status()
        with open(f"{out}/{city}.json", "w") as f:        # raw layer: untouched API response
            json.dump(r.json(), f)
        print(f"raw saved: {city}")


def transform(ds, **_):
    frames = []
    for path in glob.glob(f"{RAW}/dt={ds}/*.json"):
        city = os.path.basename(path).replace(".json", "")
        with open(path) as f:
            df = pd.DataFrame(json.load(f)["hourly"])
        df["city"] = city
        frames.append(df)
    df = pd.concat(frames, ignore_index=True).rename(columns=RENAME)
    df["obs_time"] = pd.to_datetime(df["obs_time"])
    for col in ["temp_c", "humidity_pct", "rain_mm", "wind_kmh"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
    df["date"] = df["obs_time"].dt.strftime("%Y-%m-%d")
    for day, part in df.groupby("date"):                  # one folder per date = partition
        folder = f"{PROCESSED}/date={day}"
        os.makedirs(folder, exist_ok=True)
        part.drop(columns="date").to_parquet(f"{folder}/data.parquet", index=False)  # overwrite = idempotent
        print(f"processed {len(part)} rows -> {folder}")


def validate(ds, **_):
    df = pd.read_parquet(f"{PROCESSED}/date={ds}/data.parquet")
    problems = []
    if df.empty:
        problems.append("no rows")
    if df["city"].nunique() != len(CITIES):
        problems.append("missing cities")
    if df[["temp_c", "obs_time"]].isna().any().any():
        problems.append("null temperature/time")
    if not df["temp_c"].between(-50, 60).all():
        problems.append("temperature out of range")
    if (df["rain_mm"].dropna() < 0).any():
        problems.append("negative rainfall")
    if problems:
        raise ValueError(f"Validation failed for {ds}: {problems}")
    print(f"validation passed: {len(df)} rows")


def load(ds, **_):
    from sqlalchemy import create_engine, text
    df = pd.read_parquet(f"{PROCESSED}/date={ds}/data.parquet")
    df = df.astype(object).where(df.notna(), None)
    engine = create_engine(MYSQL_URL)
    with open("/opt/airflow/sql/aggregations.sql") as f:
        views = [s.strip() for s in f.read().split(";") if s.strip()]
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE IF NOT EXISTS weather_hourly (
            city VARCHAR(50), obs_time DATETIME, temp_c FLOAT, humidity_pct FLOAT,
            rain_mm FLOAT, wind_kmh FLOAT, PRIMARY KEY (city, obs_time))"""))
        conn.execute(text("""INSERT INTO weather_hourly
            (city, obs_time, temp_c, humidity_pct, rain_mm, wind_kmh)
            VALUES (:city, :obs_time, :temp_c, :humidity_pct, :rain_mm, :wind_kmh)
            ON DUPLICATE KEY UPDATE temp_c=VALUES(temp_c), humidity_pct=VALUES(humidity_pct),
                                    rain_mm=VALUES(rain_mm), wind_kmh=VALUES(wind_kmh)"""),
            df.to_dict("records"))
        for v in views:
            conn.execute(text(v))
    print(f"loaded {len(df)} rows into weather_hourly")


with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["weather", "etl"],
) as dag:
    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="validate", python_callable=validate)
    t4 = PythonOperator(task_id="load", python_callable=load)
    t1 >> t2 >> t3 >> t4
