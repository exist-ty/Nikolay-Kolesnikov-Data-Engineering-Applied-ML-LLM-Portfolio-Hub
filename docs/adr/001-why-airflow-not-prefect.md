# ADR 001: Airflow (LocalExecutor), не Prefect/Dagster

## Context

Три соседних репозитория ([`etl-portfolio`](https://github.com/exist-ty/etl-portfolio),
[`product-marketing-analytics`](https://github.com/exist-ty/product-marketing-analytics),
[`support-triage-llm`](https://github.com/exist-ty/support-triage-llm)) документировали свои зависимости друг от друга как
ручные инструкции в README ("сначала прогони X, потом Y") — реальный DAG,
просто не выраженный явно. Нужен был инструмент, который превратит это в
настоящую оркестрацию: явный граф зависимостей, retry/alerting, UI для
разбора упавших прогонов.

Рассматривались Airflow, Prefect и Dagster — все три решают одну и ту же
задачу для пет-проекта такого размера, разница в основном в модели
разработки (декларативный DAG vs Python-first flow) и в том, что каждый
считает "современным" API.

## Decision

Apache Airflow 2.10, `LocalExecutor`, в Docker (не нативно на Windows —
Airflow официально поддерживает только Linux/macOS/WSL2).

Выбор сделан не потому, что Prefect/Dagster хуже — они были бы вполне
рабочим выбором для этого же пайплайна — а потому, что Airflow остаётся
наиболее вероятным инструментом, который HR/тимлид увидит в вакансии Data
Engineer, и демонстрация именно его (включая его реальные, а не
приглаженные ограничения — см. ADR 005) полезнее для портфолио, чем более
современный, но реже требуемый в вакансиях инструмент.

`LocalExecutor` (не `CeleryExecutor`/`KubernetesExecutor`) — параллелизм в
пределах одной машины достаточен: DAG запускает независимые ветки
(`refresh_marts`, `load_to_clickhouse`, `build_features`,
`generate_messages`) параллельно после `etl_pipeline`, что уже
демонстрирует параллельное исполнение без оверхеда Celery/Redis.

## Consequences

- Пришлось решать задачу, которую готовый managed-Airflow (MWAA, Cloud
  Composer) решил бы за пользователя: конфликт версий `sqlalchemy` между
  самим Airflow и задачными скриптами (см. ADR 005) — на реальном
  managed-сервисе Windows/Docker-специфичных проблем тоже меньше.
- `LocalExecutor` — потолок масштабирования: все задачи выполняются на той
  же машине, что и scheduler/webserver, а CPU-инференс Ollama внутри одной
  из задач конкурирует с самим Airflow за ограниченные ресурсы (см. README,
  раздел "Оркестрация" — честно задокументированное падение `run_triage` с
  ~24 до ~51 минуты при совместном запуске).
- Переход на `CeleryExecutor`/`KubernetesExecutor` понадобился бы при
  реальной многозадачности (несколько параллельных DAG-прогонов,
  распределение по разным машинам) — для пет-проекта с одним DAG и одним
  прогоном за раз это не нужно, добавлять было бы преждевременной
  сложностью.

## Status

Accepted.
