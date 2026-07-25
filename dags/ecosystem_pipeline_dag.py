"""Оркестрирует полный пайплайн экосистемы поверх трёх соседних
репозиториев (etl-portfolio, product-marketing-analytics,
support-triage-llm — локальная папка называется llm-practice, но
смонтирована в контейнер под именем support-triage-llm, см.
docker-compose.yml), каждый смонтирован в контейнер как volume.

Каждая задача — BashOperator, переходящий в каталог соответствующего
репозитория и запускающий его собственный скрипт тем же способом, что и
при ручном запуске (см. README каждого репозитория), но через отдельный
venv в `/home/airflow/task-venv` (не через python самого Airflow — см. Dockerfile:
Airflow 2.x жёстко требует `sqlalchemy<2.0`, а репозиториям нужен `sqlalchemy>=2.0`,
общее окружение сломало бы веб-интерфейс Airflow). Переменные подключения
(DB_HOST и т.д.) переопределены на уровне контейнера (docker-compose.yml)
на `host.docker.internal` вместо `localhost` из `.env` каждого репозитория —
`python-dotenv` не перезаписывает уже установленные переменные окружения,
поэтому конфликта нет.

Health check разбит по веткам, а не один общий корень: раньше единственный
`system_health_check` проверял все четыре сервиса сразу и был корнем всего
графа, поэтому неподнятый Ollama блокировал в том числе чисто postgres'овые
витрины, которым Ollama не нужен. Теперь каждая ветка гейтится ровно теми
сервисами, от которых зависит (см. scripts/health_check.py --services).
Проверка MLflow при этом стоит ДО дорогих задач (train_churn_model ~минуты,
run_triage ~24-51 минуты), а не после них — падать на недоступном трекинге
имеет смысл до обучения, а не после.
"""
import json
import os
import urllib.request
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

REPOS = "/opt/repos"
TASK_PYTHON = "/home/airflow/task-venv/bin/python"
HEALTH_CHECK = f"{TASK_PYTHON} /opt/airflow/scripts/health_check.py"

# n8n живёт в соседнем репозитории (n8n-business-automation), поднимается
# отдельным docker-compose и слушает на host.docker.internal:5678 — тот же
# приём, что и для ClickHouse/Ollama выше. Используем urllib из stdlib, а не
# requests: callback выполняется в собственном Python-окружении Airflow
# (не в task-venv), а его лишними зависимостями лучше не трогать — та же
# причина, по которой task-venv вообще существует (см. Dockerfile).
N8N_WEBHOOK_URL = os.environ.get(
    "N8N_WEBHOOK_URL", "http://host.docker.internal:5678/webhook/etl-failure-alert"
)
N8N_QUALITY_WEBHOOK_URL = os.environ.get(
    "N8N_QUALITY_WEBHOOK_URL", "http://host.docker.internal:5678/webhook/etl-quality-report"
)
N8N_DOCS_WEBHOOK_URL = os.environ.get(
    "N8N_DOCS_WEBHOOK_URL", "http://host.docker.internal:5678/webhook/docs-refresh"
)
N8N_DRIFT_WEBHOOK_URL = os.environ.get(
    "N8N_DRIFT_WEBHOOK_URL", "http://host.docker.internal:5678/webhook/triage-drift-check"
)
# mlflow — сервис в этом же docker-compose (docker-compose.yml), поэтому по
# имени сервиса в общей compose-сети, а не host.docker.internal
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5501")


def notify_failure(context):
    payload = {
        "dag_id": context["dag"].dag_id,
        "task_id": context["task_instance"].task_id,
        # logical_date, а не execution_date: последний устарел в Airflow 2.2,
        # даёт DeprecationWarning в 2.10 и удалён в Airflow 3
        "logical_date": str(context["logical_date"]),
        "exception": str(context.get("exception")),
        "log_url": context["task_instance"].log_url,
    }
    request = urllib.request.Request(
        N8N_WEBHOOK_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=10)
    except Exception:
        # Уведомление не должно ронять сам DAG повторно, если n8n недоступен
        pass


default_args = {
    "owner": "nikolay-kolesnikov",
    # Одна повторная попытка: почти все задачи здесь ходят по сети (Postgres,
    # ClickHouse, Ollama, MLflow, n8n) и разовый обрыв не повод ронять
    # 40-минутный прогон. Задачи идемпотентны — скрипты пересоздают свой
    # результат целиком, а не дописывают.
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    # Верхняя граница по умолчанию — чтобы зависшая сетевая задача не держала
    # слот LocalExecutor бесконечно. Долгий run_triage переопределяет её ниже.
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="ecosystem_pipeline",
    description="etl-portfolio -> product-marketing-analytics -> support-triage-llm, полный честный прогон",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # run_triage один занимает до ~51 минуты (CPU-инференс), остальной граф
    # ~10 минут; 3 часа — заведомо больше честного прогона, но конечны
    dagrun_timeout=timedelta(hours=3),
    tags=["portfolio", "etl", "analytics", "llm"],
) as dag:

    # --- Health-гейты по веткам -------------------------------------------
    # Возвращают 1 только если сервис недоступен (ERROR). WARNING — пустая
    # таблица, несвежие данные — печатается в лог, но пайплайн не блокирует
    # (см. scripts/health_check.py).

    health_check_core = BashOperator(
        task_id="health_check_core",
        bash_command=f"{HEALTH_CHECK} --services postgres,clickhouse",
    )

    # Нужен train_churn_model и run_triage/evaluate_llm — обе пишут run в MLflow
    health_check_mlflow = BashOperator(
        task_id="health_check_mlflow",
        bash_command=f"{HEALTH_CHECK} --services mlflow",
    )

    health_check_ollama = BashOperator(
        task_id="health_check_ollama",
        bash_command=f"{HEALTH_CHECK} --services ollama",
    )

    # --- Пайплайн ---------------------------------------------------------

    etl_pipeline = BashOperator(
        task_id="etl_pipeline",
        bash_command=f"cd {REPOS}/etl-portfolio && {TASK_PYTHON} -m src.etl.pipeline",
    )

    # Триггерит воркфлоу "AI Data Quality Report" в n8n-business-automation:
    # тот сам выполняет проверки (NULL/дубликаты/orphan FK/row count) и шлёт
    # отчёт в Telegram — curl тут только будит воркфлоу, логика проверок
    # живёт в n8n, не дублируется здесь.
    # Раньше здесь стоял `|| true`: задача не могла упасть никогда, и лежащий
    # n8n (или переименованный webhook) выглядел в UI зелёным, а отчёт просто
    # не приходил. `curl -sf` возвращает ненулевой код на HTTP >= 400 — этого
    # достаточно, чтобы задача честно покраснела.
    notify_quality_report = BashOperator(
        task_id="notify_quality_report",
        bash_command=f"curl -sfS --max-time 30 -X POST {N8N_QUALITY_WEBHOOK_URL}",
    )

    refresh_marts = BashOperator(
        task_id="refresh_marts",
        bash_command=f"cd {REPOS}/product-marketing-analytics && {TASK_PYTHON} scripts/refresh_marts.py",
    )

    # Триггерит "Notion Auto Documentation" — сам собирает каталог из
    # pg_catalog + data_catalog_descriptions и обновляет страницу в Notion.
    notify_docs_refresh = BashOperator(
        task_id="notify_docs_refresh",
        bash_command=f"curl -sfS --max-time 30 -X POST {N8N_DOCS_WEBHOOK_URL}",
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

    # ~24-51 минута на этом железе (CPU-инференс Qwen2.5-3B, 45 сообщений) —
    # см. README support-triage-llm, «Честные ограничения». Не зависание,
    # поэтому дефолтные 20 минут переопределены. Повтор здесь отключён:
    # ретрай часового инференса из-за сетевого сбоя дороже, чем перезапуск
    # задачи руками.
    run_triage = BashOperator(
        task_id="run_triage",
        bash_command=f"cd {REPOS}/support-triage-llm && {TASK_PYTHON} scripts/run_triage.py",
        execution_timeout=timedelta(hours=2),
        retries=0,
    )

    channel_triage_summary = BashOperator(
        task_id="channel_triage_summary",
        bash_command=f"cd {REPOS}/support-triage-llm && {TASK_PYTHON} scripts/channel_triage_summary.py",
    )

    evaluate_llm = BashOperator(
        task_id="evaluate_llm",
        bash_command=f"cd {REPOS}/support-triage-llm && {TASK_PYTHON} scripts/evaluate_llm.py",
    )

    # Считает дрейф РАСПРЕДЕЛЕНИЯ ЗАКАЗОВ (total_amount, неделя к неделе,
    # см. scripts/check_drift.py) — не то же самое, что "Data Drift Monitor"
    # в n8n (categories/confidence триажа), это её дополняет со стороны
    # etl-portfolio; пишет drift_result.json, который читает следующая задача
    check_drift = BashOperator(
        task_id="check_drift",
        bash_command=f"{TASK_PYTHON} /opt/airflow/scripts/check_drift.py",
    )

    # Триггерит "Data Drift Monitor" в n8n и передаёт ему результат
    # check_drift.py как тело запроса — сам n8n дополнительно проверяет свой
    # снапшот triage (категории/confidence/high-priority), это не дублирование.
    notify_drift_check = BashOperator(
        task_id="notify_drift_check",
        bash_command=(
            "curl -sfS --max-time 30 -X POST -H 'Content-Type: application/json' "
            f"-d @/opt/airflow/scripts/drift_result.json {N8N_DRIFT_WEBHOOK_URL}"
        ),
    )

    health_check_core >> etl_pipeline
    etl_pipeline >> [refresh_marts, load_to_clickhouse, build_features, generate_messages, notify_quality_report]
    refresh_marts >> notify_docs_refresh
    build_features >> train_churn_model
    generate_messages >> run_triage >> [channel_triage_summary, evaluate_llm]
    [channel_triage_summary, evaluate_llm] >> check_drift >> notify_drift_check

    # MLflow нужен train_churn_model и run_triage (evaluate_llm получает
    # гарантию транзитивно через run_triage), Ollama — только run_triage
    health_check_mlflow >> [train_churn_model, run_triage]
    health_check_ollama >> run_triage
