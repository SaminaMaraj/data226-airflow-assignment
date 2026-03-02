from __future__ import annotations

from datetime import datetime
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()


def full_refresh_country_capital(cur, target_table: str, records: list[list[str]]):
    try:
        cur.execute("BEGIN;")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                country VARCHAR PRIMARY KEY,
                capital VARCHAR
            );
            """
        )
        cur.execute(f"DELETE FROM {target_table};")
        cur.executemany(
            f"INSERT INTO {target_table} (country, capital) VALUES (%s, %s);",
            records,
        )
        cur.execute("COMMIT;")
    except Exception:
        cur.execute("ROLLBACK;")
        raise


def full_refresh_daily_snow(cur, target_table: str, rows: list[dict]):
    try:
        cur.execute("BEGIN;")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                obs_date DATE PRIMARY KEY,
                snowfall_sum FLOAT
            );
            """
        )
        cur.execute(f"DELETE FROM {target_table};")
        payload = [(r["date"], r["snowfall_sum"]) for r in rows]
        cur.executemany(
            f"INSERT INTO {target_table} (obs_date, snowfall_sum) VALUES (%s, %s);",
            payload,
        )
        cur.execute("COMMIT;")
    except Exception:
        cur.execute("ROLLBACK;")
        raise


with DAG(
    dag_id="country_capital_and_snow_to_snowflake",
    start_date=datetime(2026, 2, 23),
    catchup=False,
    schedule="30 2 * * *",
    tags=["ETL"],
) as dag:

    @task
    def read_config():
        return {
            "country_capital_url": Variable.get("country_capital_url"),
            "latitude": float(Variable.get("latitude")),
            "longitude": float(Variable.get("longitude")),
        }

    @task
    def extract_country_capital(cfg: dict) -> str:
        resp = requests.get(cfg["country_capital_url"], timeout=60)
        resp.raise_for_status()
        return resp.text

    @task
    def transform_country_capital(csv_text: str) -> list[list[str]]:
        lines = [ln.strip() for ln in csv_text.strip().split("\n") if ln.strip()]
        rows: list[list[str]] = []
        for ln in lines[1:]:
            parts = [p.strip() for p in ln.split(",")]
            if len(parts) >= 2:
                rows.append([parts[0], parts[1]])
        return rows

    @task
    def extract_daily_snow(cfg: dict) -> dict:
        lat = cfg["latitude"]
        lon = cfg["longitude"]
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            "&daily=snowfall_sum"
            "&timezone=auto"
        )
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        return resp.json()

    @task
    def transform_daily_snow(api_json: dict) -> list[dict]:
        daily = api_json.get("daily", {})
        times = daily.get("time", [])
        snowfall = daily.get("snowfall_sum", [])
        rows: list[dict] = []
        for d, s in zip(times, snowfall):
            rows.append({"date": d, "snowfall_sum": float(s) if s is not None else 0.0})
        return rows

    from airflow.exceptions import AirflowException

    @task
    def load_to_snowflake(country_rows: list[list[str]], snow_rows: list[dict]) -> str:
        cur = get_snowflake_cursor()
        try:
            # Use objects you have access to
            cur.execute("USE WAREHOUSE SNOWFLAKE_LEARNING_WH;")
            cur.execute('USE DATABASE "SNOWFLAKE_LEARNING_DB";')
            cur.execute("USE SCHEMA DOG_SCHEMA;")

            # Full refresh (your helper functions already use BEGIN/COMMIT/ROLLBACK)
            full_refresh_country_capital(
                cur,
                '"SNOWFLAKE_LEARNING_DB".DOG_SCHEMA.COUNTRY_CAPITAL',
                country_rows,
            )
            full_refresh_daily_snow(
                cur,
                '"SNOWFLAKE_LEARNING_DB".DOG_SCHEMA.DAILY_SNOW',
                snow_rows,
            )

            return f"Loaded {len(country_rows)} countries and {len(snow_rows)} snow rows."
        except Exception as e:
            raise AirflowException(f"Snowflake load failed: {e}")
        finally:
            try:
                cur.close()
            except Exception:
                pass

    @task
    def done(msg: str):
        print(msg)

    # DAG wiring
    cfg = read_config()

    cc_text = extract_country_capital(cfg)
    cc_rows = transform_country_capital(cc_text)

    snow_json = extract_daily_snow(cfg)
    snow_rows = transform_daily_snow(snow_json)

    result = load_to_snowflake(cc_rows, snow_rows)
    done(result)