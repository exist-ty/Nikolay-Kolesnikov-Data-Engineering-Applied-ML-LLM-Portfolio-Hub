# Архитектура системы

```mermaid
graph TD
    Z(["🔄 Airflow DAG: ecosystem_pipeline<br/>(dags/, этот репозиторий)"]):::orchestration --> ETL

    ETL["📦 etl-portfolio<br/>ETL → PostgreSQL staging"]:::etl --> ANALYTICS
    ETL --> LLM
    ETL -.-> N8N

    ANALYTICS["📊 product-marketing-analytics<br/>SQL-витрины · ClickHouse · Churn ML"]:::analytics -.-> N8N

    LLM["💬 support-triage-llm<br/>Гибридный RAG-поиск → Triage"]:::llm -.-> N8N

    N8N["⚡ n8n-business-automation<br/>Алерты · Дайджест · Self-Service SQL-бот"]:::n8n --> OUT["Telegram / Email / Notion"]:::n8n

    classDef orchestration fill:#2a2140,stroke:#9085e9,color:#e8e6ff,stroke-width:2px
    classDef etl fill:#123a5e,stroke:#3987e5,color:#dbeafe,stroke-width:2px
    classDef analytics fill:#0f3d24,stroke:#1baf7a,color:#d4f5e6,stroke-width:2px
    classDef llm fill:#4a1f3d,stroke:#e87ba4,color:#fbe4ee,stroke-width:2px
    classDef n8n fill:#5c3a10,stroke:#eda100,color:#fdecc8,stroke-width:2px
```

Сплошные стрелки — пайплайн-поток данных (Airflow DAG вызывает эти
репозитории по очереди), пунктирные — "читается напрямую": n8n не встроен
в DAG как ещё один шаг, а читает staging `etl-portfolio` (Data Quality
Report, Notion-документация) и витрины `product-marketing-analytics`
напрямую (`mart_channel_economics`/`mart_customer_ltv`/`mart_cohort_retention` —
`GRANT SELECT` выдан именно на них для Self-Service SQL-бота, см.
`sql/readonly_role_selfservice.sql` в `n8n-business-automation`) по
событию, а не как шаг пайплайна.

## Компоненты экосистемы

- **[etl-portfolio](https://github.com/exist-ty/etl-portfolio)** — надёжный
  ETL-пайплайн, превращающий "грязные" сырые данные интернет-магазина в
  проиндексированный staging-слой PostgreSQL, готовый к аналитике и ML.
- **[product-marketing-analytics](https://github.com/exist-ty/product-marketing-analytics)** —
  SQL-витрины юнит-экономики (CAC/CPL/ROMI, LTV, retention) с материализованными
  версиями, модель прогнозирования оттока клиентов, ClickHouse OLAP-слой с
  честным бенчмарком против Postgres и A/B-тестирование с power-анализом
  поверх общего staging-слоя.
- **[support-triage-llm](https://github.com/exist-ty/support-triage-llm)** —
  автоматическая классификация и приоритизация обращений в поддержку локальной
  LLM с гибридным (векторный + полнотекстовый, RRF) RAG-поиском и
  количественной оценкой качества.
- **[n8n-business-automation](https://github.com/exist-ty/n8n-business-automation)** —
  event-driven автоматизация вокруг DAG: алерты об ошибках/дрейфе данных,
  еженедельный AI-дайджест, Notion-документация, Self-Service Analytics Bot
  в Telegram.
- **Этот репозиторий** — помимо документации, `docker-compose.yml` +
  `dags/ecosystem_pipeline_dag.py`: Airflow (LocalExecutor) оркестрирует
  основной пайплайн трёх репозиториев выше (etl-portfolio,
  product-marketing-analytics, support-triage-llm) одним DAG, а
  n8n-business-automation не встроен в DAG как ещё один узел — он вызывается
  из этого же DAG через webhook на ключевых точках (событие, а не шаг
  пайплайна), подробнее — `docs/business-automation.md`.

См. также `docs/orchestration.md` (как DAG вызывает эти репозитории) и
`docs/adr/` (архитектурные решения с обоснованием и последствиями).
