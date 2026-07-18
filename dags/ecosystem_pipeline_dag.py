"""Оркестрирует полный пайплайн экосистемы поверх трёх соседних
репозиториев (etl-portfolio, product-marketing-analytics,
support-triage-llm — локальная папка называется llm-practice, но
смонтирована в контейнер под именем support-triage-llm, см.
docker-compose.yml), каждый смонтирован в контейнер как volume.

Каждая задача — BashOperator, переходящий в каталог соответствующего
репозитория и запускающий его собственный скрипт тем же способом, что и
при ручном запуске (см. README каждого репозитория), но через отдельный
venv в `/home/airflow/task-venv` (не через python самого Airflow — см. Dockerfile: Airflow
2.x жёстко требует `sqlalchemy<2.0`, а репозиториям нужен `sqlalchemy>=2.0`,
общее окружение сломало бы веб-интерфейс Airflow). Переменные подключения
(DB_HOST и т.д.) переопределены на уровне контейнера (docker-compose.yml)
на `host.docker.internal` вместо `localhost` из `.env` каждого репозитория —
`python-dotenv` не перезаписывает уже установленные переменные окружения,
поэтому конфликта нет.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

REPOS = "/opt/repos"
TASK_PYTHON = "/home/airflow/task-venv/bin/python"

default_args = {
    "owner": "nikolay-kolesnikov",
    "retries": 0,
}

with DAG(
    dag_id="ecosystem_pipeline",
    description="etl-portfolio -> product-marketing-analytics -> support-triage-llm, полный честный прогон",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["portfolio", "etl", "analytics", "llm"],
) as dag:

    etl_pipeline = BashOperator(
        task_id="etl_pipeline",
        bash_command=f"cd {REPOS}/etl-portfolio && {TASK_PYTHON} -m src.etl.pipeline",
    )

    refresh_marts = BashOperator(
        task_id="refresh_marts",
        bash_command=f"cd {REPOS}/product-marketing-analytics && {TASK_PYTHON} scripts/refresh_marts.py",
    )

    load_to_clickhouse = BashOperator(
        task_id="load_to_clickhouse",
        bash_command=f"cd {REPOS}/product-marketing-analytics && {TASK_PYTHON} clickhouse/load_to_clickhouse.py",
    )

    build_features = BashOperator(
        task_id="build_features",
        bash_command=f"cd {REPOS}/product-marketing-analytics && {TASK_PYTHON} ml/build_features.py",
    )

    train_churn_model = BashOperator(
        task_id="train_churn_model",
        bash_command=f"cd {REPOS}/product-marketing-analytics && {TASK_PYTHON} ml/train_churn_model.py",
    )

    generate_messages = BashOperator(
        task_id="generate_messages",
        bash_command=f"cd {REPOS}/support-triage-llm && {TASK_PYTHON} scripts/generate_messages.py",
    )

    # ~24 минуты на этом железе (CPU-инференс Qwen2.5-3B, 45 сообщений) —
    # см. README support-triage-llm, «Честные ограничения». Не зависание.
    run_triage = BashOperator(
        task_id="run_triage",
        bash_command=f"cd {REPOS}/support-triage-llm && {TASK_PYTHON} scripts/run_triage.py",
    )

    channel_triage_summary = BashOperator(
        task_id="channel_triage_summary",
        bash_command=f"cd {REPOS}/support-triage-llm && {TASK_PYTHON} scripts/channel_triage_summary.py",
    )

    evaluate_llm = BashOperator(
        task_id="evaluate_llm",
        bash_command=f"cd {REPOS}/support-triage-llm && {TASK_PYTHON} scripts/evaluate_llm.py",
    )

    etl_pipeline >> [refresh_marts, load_to_clickhouse, build_features, generate_messages]
    build_features >> train_churn_model
    generate_messages >> run_triage >> [channel_triage_summary, evaluate_llm]
