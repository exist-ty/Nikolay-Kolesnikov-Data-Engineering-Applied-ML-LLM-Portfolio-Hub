FROM apache/airflow:2.10.4-python3.11

USER airflow
COPY requirements-tasks.txt /requirements-tasks.txt

# Отдельный venv для тасков, не для самого Airflow. SQLAlchemy>=2.0 нужен
# скриптам трёх репозиториев (`from sqlalchemy import Engine` — API, которого
# нет в 1.4), но сам Airflow 2.x жёстко требует sqlalchemy<2.0 через
# flask-appbuilder/marshmallow-sqlalchemy (веб-интерфейс на этом и держится).
# Установка задачных зависимостей прямо в окружение Airflow ломает его
# собственный веб-сервер — проверено на практике, не гипотетический риск.
RUN python -m venv /home/airflow/task-venv && \
    /home/airflow/task-venv/bin/pip install --no-cache-dir -r /requirements-tasks.txt
