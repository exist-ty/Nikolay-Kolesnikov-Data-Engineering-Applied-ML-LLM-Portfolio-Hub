# Архитектура системы

```mermaid
graph TD
    Z["🔄 Airflow DAG: ecosystem_pipeline<br/>(dags/, этот репозиторий)"]:::orchestration --> A["📥 Сырые данные (CSV)<br/>клиенты · заказы · товары · маркетинг"]:::etl

    subgraph ETL["📦 etl-portfolio"]
        A --> B["extract → transform → load"]:::etl
        B --> C[("PostgreSQL: staging-слой<br/>stg_customers / stg_orders / stg_products")]:::etl
    end

    subgraph ANALYTICS["📊 product-marketing-analytics"]
        C --> D["SQL-витрины<br/>CAC/CPL/ROMI, LTV, Cohort Retention"]:::analytics
        D --> G["Metabase / Jupyter<br/>дашборды и отчёты"]:::analytics
        C --> M[("ClickHouse: order_events<br/>MergeTree + AggregatingMergeTree rollup")]:::analytics
        M --> N["Честный бенчмарк vs Postgres VIEW<br/>compare_engines.py"]:::analytics
        C --> E["ML: Feature Engineering<br/>customer-level датасет + churn-признак"]:::analytics
        E --> H["Churn Prediction<br/>Logistic Regression + Random Forest"]:::analytics
        H --> I["Метрики: ROC-AUC, PR-AUC<br/>Feature Importance"]:::analytics
    end

    subgraph LLM["💬 support-triage-llm"]
        C --> F["Синтетические обращения клиентов"]:::llm
        F --> J[("pgvector + tsvector: kb_documents<br/>VECTOR(384) + HNSW · search_tsv + GIN")]:::llm
        J --> K["Гибридный поиск (RRF)<br/>векторный + полнотекстовый → генерация (Qwen2.5-3B-Instruct, Ollama)"]:::llm
        K --> L["Triage Results + LLM Evaluation<br/>F1, Confusion Matrix"]:::llm
    end

    subgraph N8N["⚡ n8n-business-automation"]
        C --> O["n8n workflows<br/>алерты · дайджест · Self-Service SQL-бот"]:::n8n
        D --> O
        L --> O
        O --> P["Telegram / Email / Notion"]:::n8n
    end

    classDef orchestration fill:#2a2140,stroke:#9085e9,color:#e8e6ff,stroke-width:2px
    classDef etl fill:#123a5e,stroke:#3987e5,color:#dbeafe,stroke-width:2px
    classDef analytics fill:#0f3d24,stroke:#1baf7a,color:#d4f5e6,stroke-width:2px
    classDef llm fill:#4a1f3d,stroke:#e87ba4,color:#fbe4ee,stroke-width:2px
    classDef n8n fill:#5c3a10,stroke:#eda100,color:#fdecc8,stroke-width:2px

    style ETL fill:#0d1a29,stroke:#3987e5,stroke-width:1px,color:#3987e5
    style ANALYTICS fill:#0a1f14,stroke:#1baf7a,stroke-width:1px,color:#1baf7a
    style LLM fill:#2a1220,stroke:#e87ba4,stroke-width:1px,color:#e87ba4
    style N8N fill:#2e1d09,stroke:#eda100,stroke-width:1px,color:#eda100
```

Каждая рамка — отдельный репозиторий; `PostgreSQL staging` в
`etl-portfolio` — общая точка входа, из которой читают все остальные три. n8n
дополнительно читает витрины `product-marketing-analytics` напрямую
(`mart_channel_economics`/`mart_customer_ltv`/`mart_cohort_retention` —
`GRANT SELECT` выдан именно на них для Self-Service SQL-бота, см.
`sql/readonly_role_selfservice.sql` в `n8n-business-automation`), не только
staging-слой `etl-portfolio`.

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
