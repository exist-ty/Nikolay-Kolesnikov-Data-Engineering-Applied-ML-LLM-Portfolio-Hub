# ADR 005: Отдельный venv для задач Airflow, не общее окружение

## Context

DAG (`dags/ecosystem_pipeline_dag.py`) должен запускать скрипты трёх
соседних репозиториев (`etl-portfolio`, `product-marketing-analytics`,
`support-triage-llm`) через `BashOperator`. Изначально задачные зависимости
(`pandas`, `sqlalchemy`, `clickhouse-connect` и т.д.) ставились прямо в
Python-окружение самого Airflow.

## Decision

Отдельный venv для задач: `/home/airflow/task-venv`, полностью
изолированный от Python-окружения самого Airflow (см. `Dockerfile`). DAG
вызывает `/home/airflow/task-venv/bin/python`, а не системный `python`
образа `apache/airflow`.

## Consequences

- **Найденный конфликт, не гипотетический риск**: Airflow 2.x жёстко
  требует `sqlalchemy<2.0` (через `flask-appbuilder`/
  `marshmallow-sqlalchemy`, на которых держится веб-интерфейс), а
  `etl-portfolio` использует `from sqlalchemy import Engine` — API,
  которого в 1.4 нет. Установка задачных зависимостей с `--constraint` из
  официального constraints-файла тихо понизила `sqlalchemy` до 1.4.54;
  первый прогон `etl_pipeline` упал с `ImportError` прямо на этом.
- **Отдельный venv решает конфликт полностью**, ценой: Dockerfile теперь
  собирает venv на этапе build (`python -m venv` + `pip install -r
  requirements-tasks.txt`), что увеличивает время сборки образа и требует
  держать `requirements-tasks.txt` в синхронизации с зависимостями всех
  трёх репозиториев вручную — нет автоматической проверки, что
  `requirements-tasks.txt` не разошёлся с `requirements.txt` каждого
  репозитория (ручной риск, не устранённый, а принятый).
- **MLflow добавил тот же класс проблемы во второй раз**: клиент,
  установленный в `task-venv` без явного пина версии, подтягивал более
  новый `mlflow` (3.14.0), чем образ сервера (`v2.19.0`) — и падал на
  `/api/2.0/mlflow/logged-models`, которого нет в 2.19. Решение то же по
  духу: явный пин версии (`mlflow==2.19.0`) в `requirements-tasks.txt`,
  а не "поставится — разберёмся".

## Status

Accepted.
