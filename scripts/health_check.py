"""Проверяет доступность и базовую "живость" сервисов экосистемы —
PostgreSQL (etl_portfolio), ClickHouse, Ollama, MLflow — и печатает один JSON
с посервисным статусом (OK/WARNING/ERROR).

Коды выхода:
  0 — все проверенные сервисы OK или WARNING
  1 — хотя бы один ERROR (сервис недоступен)

WARNING сознательно НЕ роняет задачу: "таблица пустая" или "данные
несвежие" — это сигнал, а не отказ инфраструктуры. Раньше WARNING возвращал
1 и, поскольку health check стоит корнем DAG с retries=0, любой варнинг
блокировал все 16 задач, включая те, что от свежести не зависят вообще.
С замороженным датасетом (order_date не позже 2025-12-31, см.
etl-portfolio/scripts/generate_data.py) при STALE_DAYS=400 это гарантированно
положило бы весь пайплайн в начале февраля 2027.

Выбор сервисов: `--services postgres,clickhouse`. DAG проверяет не всё сразу,
а только то, что нужно конкретной ветке графа, — иначе неподнятый Ollama
блокирует чисто postgres'овые витрины, которым он не нужен
(см. dags/ecosystem_pipeline_dag.py).

STALE_DAYS: датасет синтетический, даты заказов заморожены в 2025 году —
свежесть здесь означает "максимальная order_date не отстаёт от текущей даты
больше чем на N дней", и со временем эта проверка честно уйдёт в WARNING,
если датасет не перегенерировать. Это ожидаемое поведение для замороженных
синтетических данных, не баг проверки.
"""
import argparse
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


CHECKS = {
    "postgres": check_postgres,
    "clickhouse": check_clickhouse,
    "ollama": check_ollama,
    "mlflow": check_mlflow,
}


def build_report(services) -> dict:
    return {name: CHECKS[name]() for name in services}


def exit_code(report: dict) -> int:
    """1 только на ERROR — WARNING не должен ронять DAG (см. докстринг)."""
    return 1 if any(r["status"] == "ERROR" for r in report.values()) else 0


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--services",
        default=",".join(CHECKS),
        help="Список сервисов через запятую: " + ", ".join(CHECKS),
    )
    args = parser.parse_args(argv)
    services = [s.strip() for s in args.services.split(",") if s.strip()]
    unknown = [s for s in services if s not in CHECKS]
    if unknown:
        parser.error(f"unknown service(s): {', '.join(unknown)}; available: {', '.join(CHECKS)}")
    return services


def main(argv=None) -> int:
    services = parse_args(argv)
    report = build_report(services)
    warnings = [name for name, r in report.items() if r["status"] == "WARNING"]
    if warnings:
        # Видно в логе задачи Airflow, но задачу не роняет
        print(f"WARNING (не блокирует пайплайн): {', '.join(warnings)}", file=sys.stderr)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return exit_code(report)


if __name__ == "__main__":
    sys.exit(main())
