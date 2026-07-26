"""Структурные тесты DAG: импортируемость плюс инварианты, которые уже
ломались в этом репозитории.

Airflow — тяжёлая зависимость и в requirements-tasks.in её нет (task-venv
намеренно отделён от окружения самого Airflow, см. ADR 005). Локально без
Airflow эти тесты пропускаются, в CI джоба validate-dag ставит его и гоняет.
"""
from datetime import timedelta

import pytest

pytest.importorskip("airflow", reason="Airflow не установлен — тесты DAG идут только в CI")

from airflow.models import DagBag  # noqa: E402

DAG_ID = "ecosystem_pipeline"


@pytest.fixture(scope="module")
def dagbag():
    bag = DagBag(dag_folder="dags", include_examples=False)
    assert not bag.import_errors, bag.import_errors
    return bag


@pytest.fixture(scope="module")
def dag(dagbag):
    assert DAG_ID in dagbag.dags, f"{DAG_ID} not found"
    return dagbag.dags[DAG_ID]


def test_dag_imports_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_task_count_matches_readme(dag):
    # README заявляет «DAG на 17 задач» — тест держит цифру честной
    assert len(dag.tasks) == 17


def test_no_task_swallows_its_exit_code(dag):
    """`|| true` делает задачу неспособной упасть: лежащий n8n выглядит в UI
    зелёным, а отчёт не приходит. Тихие сбои так и появляются."""
    offenders = [
        t.task_id for t in dag.tasks
        if "|| true" in getattr(t, "bash_command", "")
    ]
    assert offenders == [], f"задачи маскируют код возврата: {offenders}"


def test_every_task_has_execution_timeout(dag):
    missing = [t.task_id for t in dag.tasks if t.execution_timeout is None]
    assert missing == [], f"задачи без execution_timeout: {missing}"


def test_long_running_triage_has_room_but_is_bounded(dag):
    run_triage = dag.get_task("run_triage")
    # честный прогон — до ~51 минуты, см. README support-triage-llm
    assert run_triage.execution_timeout > timedelta(hours=1)
    assert run_triage.execution_timeout <= timedelta(hours=3)
    assert dag.dagrun_timeout is not None


def test_health_checks_gate_only_their_own_branch(dag):
    """Один общий health-гейт на все сервисы связывал несвязанное: неподнятый
    Ollama блокировал postgres'овые витрины."""
    core_downstream = dag.get_task("health_check_core").get_flat_relative_ids(upstream=False)
    ollama_downstream = dag.get_task("health_check_ollama").get_flat_relative_ids(upstream=False)

    assert "refresh_marts" in core_downstream
    assert "refresh_marts" not in ollama_downstream, "витрины не зависят от Ollama"
    assert "run_triage" in ollama_downstream


def test_data_contracts_gate_marts_but_not_quality_report(dag):
    """data_contracts (Soda Core) должен стоять МЕЖДУ etl_pipeline и
    витринами — fail-fast до их построения, а не после. notify_quality_report
    (постфактум-отчёт n8n) — независимая ветка, contracts её не гейтит."""
    contracts_downstream = dag.get_task("data_contracts").get_flat_relative_ids(upstream=False)
    etl_downstream = dag.get_task("etl_pipeline").get_flat_relative_ids(upstream=False)

    assert {"refresh_marts", "load_to_clickhouse", "build_features", "generate_messages"} <= contracts_downstream
    assert "data_contracts" in etl_downstream
    assert "notify_quality_report" not in contracts_downstream


def test_mlflow_is_checked_before_expensive_training(dag):
    """Проверка трекинга имеет смысл до обучения, а не после — раньше два
    curl-а к /health стояли ПОСЛЕ train_churn_model и run_triage."""
    mlflow_downstream = dag.get_task("health_check_mlflow").get_flat_relative_ids(upstream=False)
    assert {"train_churn_model", "run_triage", "evaluate_llm"} <= mlflow_downstream


def test_no_deprecated_execution_date_in_source():
    """context["execution_date"] удалён в Airflow 3 — ловим до апгрейда."""
    from pathlib import Path

    source = Path("dags/ecosystem_pipeline_dag.py").read_text(encoding="utf-8")
    assert '"execution_date"' not in source
    assert '"logical_date"' in source
