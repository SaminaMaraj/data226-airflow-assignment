from __future__ import annotations

from datetime import datetime, timedelta
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def get_snowflake_cursor(conn_id: str = "snowflake_conn"):
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn = hook.get_conn()
    return conn, conn.cursor()


with DAG(
    dag_id="WeatherData_ETL_HW5",
    start_date=datetime(2026, 2, 27),
    catchup=False,
    schedule="30 2 * * *",
    default_args=DEFAULT_ARGS,
    tags=["ETL"],
) as dag:

    @task
    def extract(latitude: float, longitude: float) -> dict:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "past_days": 60,
            "forecast_days": 0,
            "daily": ["temperature_2m_max", "temperature_2m_min", "weather_code"],
            "timezone": "America/Los_Angeles",
        }
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()

    @task
    def transform(raw_data: dict, latitude: float, longitude: float, city: str) -> list[dict]:
        daily = raw_data.get("daily")
        if not daily:
            raise ValueError("Missing 'daily' in API response")

        times = daily.get("time", [])
        tmax = daily.get("temperature_2m_max", [])
        tmin = daily.get("temperature_2m_min", [])
        wcode = daily.get("weather_code", [])

        records: list[dict] = []
        for d, mx, mn, wc in zip(times, tmax, tmin, wcode):
            records.append(
                {
                    "latitude": float(latitude),
                    "longitude": float(longitude),
                    "date": d,  # YYYY-MM-DD string is fine
                    "temp_max": float(mx) if mx is not None else None,
                    "temp_min": float(mn) if mn is not None else None,
                    "weather_code": str(wc) if wc is not None else None,
                    "city": city,
                }
            )
        return records

    @task
    def load(records: list[dict], target_table: str = "RAW.WEATHER_DATA_HW5") -> str:
        conn, cur = get_snowflake_cursor("snowflake_conn")
        try:
            # Make sure session context is correct (even if connection has it)
            cur.execute("USE ROLE TRAINING_ROLE;")
            cur.execute("USE WAREHOUSE DOG_WH;")
            cur.execute('USE DATABASE "USER_DB_DOG";')
            cur.execute("USE SCHEMA RAW;")

            cur.execute("BEGIN;")

            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {target_table} (
                    latitude FLOAT,
                    longitude FLOAT,
                    date DATE,
                    temp_max FLOAT,
                    temp_min FLOAT,
                    weather_code VARCHAR(10),
                    city VARCHAR(100),
                    PRIMARY KEY (latitude, longitude, date, city)
                );
                """
            )

            cur.execute(f"DELETE FROM {target_table};")

            insert_sql = f"""
                INSERT INTO {target_table} (
                    latitude, longitude, date, temp_max, temp_min, weather_code, city
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """

            data = [
                (
                    r["latitude"],
                    r["longitude"],
                    r["date"],
                    r["temp_max"],
                    r["temp_min"],
                    r["weather_code"],
                    r["city"],
                )
                for r in records
            ]

            cur.executemany(insert_sql, data)

            cur.execute("COMMIT;")
            return f"Loaded {len(records)} rows into {target_table}."
        except Exception:
            cur.execute("ROLLBACK;")
            raise
        finally:
            try:
                cur.close()
            finally:
                conn.close()

    LATITUDE = float(Variable.get("latitude"))    
    LONGITUDE = float(Variable.get("longitude"))
    CITY = "Georgetown"

    raw = extract(LATITUDE, LONGITUDE)
    rows = transform(raw, LATITUDE, LONGITUDE, CITY)
    load(rows)