# Быстрый старт

Три репозитория рассчитаны на совместное расположение в соседних директориях
(взаимные относительные ссылки на общую базу `etl_portfolio`):

```bash
git clone https://github.com/exist-ty/etl-portfolio.git
git clone https://github.com/exist-ty/product-marketing-analytics.git
git clone https://github.com/exist-ty/support-triage-llm.git
```

Для каждого репозитория — стандартный production-ready цикл:

```bash
python -m venv .venv && .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env        # указать реальные креды, .env — вне git
pytest                       # проверить, что окружение готово
```

1. **[etl-portfolio](https://github.com/exist-ty/etl-portfolio)** — поднять PostgreSQL, применить `sql/schema.sql`,
   сгенерировать и загрузить данные (`python -m src.etl.pipeline`).
2. **[product-marketing-analytics](https://github.com/exist-ty/product-marketing-analytics)** — применить `sql/marts.sql`, поднять
   Metabase одной командой (`docker compose up -d`), обучить модель оттока
   (`python ml/train_churn_model.py`).
3. **[support-triage-llm](https://github.com/exist-ty/support-triage-llm)** — поднять `pgvector`-контейнер и Ollama
   (`docker compose up -d`), применить `sql/triage_schema.sql`, прогнать
   пайплайн триажа (`python scripts/run_triage.py`).
4. **Этот репозиторий (оркестрация)** — см. [`docs/orchestration.md`](orchestration.md).

Для самого хаба `.env` заполняется чуть иначе: `AIRFLOW_FERNET_KEY` и три
пароля к базам обязательны и не имеют значений по умолчанию — `docker
compose` откажется стартовать, пока они пустые. Это сознательно: пустой
Fernet-ключ означает нешифрованные Connections, которые перестают читаться
после пересоздания контейнера, а пустой пароль раньше подставлялся молча и
падение приходило уже из середины пайплайна.

```bash
cp .env.example .env
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# вставить вывод в AIRFLOW_FERNET_KEY, заполнить *_PASSWORD теми же
# значениями, что в .env соседних репозиториев
docker compose up -d          # Airflow UI: http://localhost:8080 (admin)
pytest                        # юнит-тесты health_check и check_drift
```

Секреты нигде не хардкожены — только через `.env`, чувствительные файлы
исключены `.gitignore`; каждый сервис контейнеризирован и поднимается
одной командой Docker Compose.
