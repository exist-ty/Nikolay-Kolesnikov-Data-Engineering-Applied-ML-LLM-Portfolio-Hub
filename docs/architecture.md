# Архитектура системы

```mermaid
graph TD
    Z["Airflow DAG: ecosystem_pipeline<br/>(dags/, этот репозиторий)"] --> A
    A["Сырые данные (CSV)<br/>клиенты · заказы · товары · маркетинг"] --> B["ETL-пайплайн (etl-portfolio)<br/>extract → transform → load"]
    B --> C[("PostgreSQL: staging-слой<br/>stg_customers / stg_orders / stg_products")]

    C --> D["Аналитика (product-marketing-analytics)<br/>SQL-витрины: CAC/CPL/ROMI, LTV, Cohort Retention"]
    C --> E["ML: Feature Engineering<br/>customer-level датасет + churn-признак"]
    C --> F["Синтетические обращения клиентов<br/>(support-triage-llm)"]
    C --> M["ClickHouse: order_events<br/>MergeTree + AggregatingMergeTree rollup"]

    D --> G["Metabase / Jupyter<br/>дашборды и отчёты"]
    M --> N["Честный бенчмарк vs Postgres VIEW<br/>compare_engines.py"]

    E --> H["Churn Prediction<br/>Logistic Regression + Random Forest"]
    H --> I["Метрики: ROC-AUC, PR-AUC<br/>Feature Importance"]

    F --> J[("pgvector + tsvector: kb_documents<br/>VECTOR(384) + HNSW · search_tsv + GIN")]
    J --> K["Гибридный поиск (RRF)<br/>векторный + полнотекстовый → генерация (Qwen2.5-3B-Instruct, Ollama)"]
    K --> L["Triage Results + LLM Evaluation<br/>F1, Confusion Matrix"]

    C --> O["n8n (n8n-business-automation)<br/>алерты · дайджест · Self-Service SQL-бот"]
    L --> O
    O --> P["Telegram / Email / Notion"]
```

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
