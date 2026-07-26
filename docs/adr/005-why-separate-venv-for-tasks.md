# ADR 005: Отдельный venv для задач Airflow, не общее окружение

## Context

DAG (`dags/ecosystem_pipeline_dag.py`) должен запускать скрипты трёх
соседних репозиториев ([`etl-portfolio`](https://github.com/exist-ty/etl-portfolio),
[`product-marketing-analytics`](https://github.com/exist-ty/product-marketing-analytics),
[`support-triage-llm`](https://github.com/exist-ty/support-triage-llm)) через `BashOperator`. Изначально задачные зависимости
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
- **Пин оказался нужен не только mlflow.** Остальные строки долго оставались
  без версий, то есть каждая пересборка образа ставила то, что PyPI отдаёт
  сегодня — в решении, вся суть которого в контроле над версиями. Теперь
  файл разделён: `requirements-tasks.in` — намерение, `requirements-tasks.txt`
  — точные версии, которые и ставит Dockerfile. Резолвить lock нужно в
  Linux-контейнере: pip вычисляет маркеры `sys_platform` по текущей машине,
  и на Windows набор получается другой (`waitress`/`pywin32` вместо
  `gunicorn`), а образ с таким файлом не собирается.
- **Ручная синхронизация с тремя `requirements.txt` больше не безнадзорна.**
  Автоматической сверки с репозиториями по-прежнему нет (риск принят), но
  джоба `build-image` в CI собирает образ на Linux, гоняет `pip check` и
  сверяет `pip freeze` с пинами — разъехавшийся набор теперь падает в CI,
  а не в контейнере на живом прогоне.

## Status

Accepted.
