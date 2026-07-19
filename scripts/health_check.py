"""Проверяет доступность и базовую "живость" всех сервисов экосистемы —
PostgreSQL (etl_portfolio), ClickHouse, Ollama, MLflow — и печатает один JSON
с посервисным статусом (OK/WARNING/ERROR). Возвращает код выхода 1, если
хотя бы один сервис не OK — чтобы Airflow мог зафейлить задачу на этом.

STALE_DAYS: датасет синтетический, даты заказов заморожены в 2025 году
(см. etl-portfolio/scripts/generate_data.py) — свежесть здесь означает
"максимальная order_date не отстаёт от текущей даты больше чем на N дней",
и со временем (когда система запускается в будущем) эта проверка рано или
поздно честно уйдёт в WARNING, если датасет не перегенерировать. Это
ожидаемое поведение для замороженных синтетических данных, не баг проверки.
"""
import json
import os
import sys
from datetime import date

import clickhouse_connect
import requests
from sqlalchemy import create_engine, text

STALE_DAYS = int(os.environ.get("HEALTH_CHECK_STALE_DAYS", "400"))


def get_postgres_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', '')}"
        f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'etl_portfolio')}"
    )


def check_postgres() -> dict:
    try:
        engine = get_postgres_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            row_counts = {
                table: conn.execute(text(f"SELECT count(*) FROM {table}")).scalar()
                for table in ("stg_customers", "stg_orders", "stg_products")
            }
            max_order_date = conn.execute(text("SELECT max(order_date) FROM stg_orders")).scalar()

        result = {"status": "OK", "row_counts": row_counts,
                   "max_order_date": str(max_order_date) if max_order_date else None}

        if any(count == 0 for count in row_counts.values()):
            result["status"] = "WARNING"
            result["reason"] = "one or more tables are empty"
        elif max_order_date and (date.today() - max_order_date).days > STALE_DAYS:
            result["status"] = "WARNING"
            result["reason"] = f"max_order_date older than {STALE_DAYS} days"

        return result
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}


def check_clickhouse() -> dict:
    try:
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        )
        row_count = client.query("SELECT count(*) FROM analytics.order_events").result_rows[0][0]
        status = "OK" if row_count > 0 else "WARNING"
        result = {"status": status, "order_events_rows": row_count}
        if status == "WARNING":
            result["reason"] = "analytics.order_events is empty"
        return result
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}


def check_ollama() -> dict:
    try:
        host = os.getenv("OLLAMA_HOST", "http://localhost:11434")
        resp = requests.get(f"{host}/api/tags", timeout=5)
        resp.raise_for_status()
        models = [m["name"] for m in resp.json().get("models", [])]
        return {"status": "OK", "models": models}
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}


def check_mlflow() -> dict:
    try:
        tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5501")
        resp = requests.get(f"{tracking_uri}/health", timeout=5)
        resp.raise_for_status()
        return {"status": "OK"}
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}


def main() -> int:
    report = {
        "postgres": check_postgres(),
        "clickhouse": check_clickhouse(),
        "ollama": check_ollama(),
        "mlflow": check_mlflow(),
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if all(r["status"] == "OK" for r in report.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
