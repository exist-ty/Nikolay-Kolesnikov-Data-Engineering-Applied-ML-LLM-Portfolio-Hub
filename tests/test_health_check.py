"""Тесты на scripts/health_check.py — код выхода и разбор --services,
без живых Postgres/ClickHouse/Ollama/MLflow.

Главное, что здесь закрыто: WARNING не роняет задачу. Health check стоит
корнем ветки с ретраями, и раньше «таблица пустая» или «данные несвежие»
возвращали 1 наравне с «сервис недоступен», блокируя весь граф.
"""
import pytest

from scripts import health_check
from scripts.health_check import exit_code, parse_args


def test_all_ok_exits_zero():
    assert exit_code({"postgres": {"status": "OK"}, "mlflow": {"status": "OK"}}) == 0


def test_warning_does_not_fail_the_task():
    report = {
        "postgres": {"status": "WARNING", "reason": "max_order_date older than 400 days"},
        "clickhouse": {"status": "OK"},
    }
    assert exit_code(report) == 0


def test_error_fails_the_task():
    report = {"postgres": {"status": "OK"}, "ollama": {"status": "ERROR", "error": "connection refused"}}
    assert exit_code(report) == 1


def test_error_wins_over_warning():
    report = {"postgres": {"status": "WARNING"}, "clickhouse": {"status": "ERROR"}}
    assert exit_code(report) == 1


def test_parse_args_defaults_to_all_services():
    assert parse_args([]) == list(health_check.CHECKS)


def test_parse_args_selects_subset():
    assert parse_args(["--services", "postgres,clickhouse"]) == ["postgres", "clickhouse"]


def test_parse_args_tolerates_spaces():
    assert parse_args(["--services", " mlflow , ollama "]) == ["mlflow", "ollama"]


def test_parse_args_rejects_unknown_service():
    with pytest.raises(SystemExit):
        parse_args(["--services", "postgres,redis"])


def test_build_report_only_runs_requested_checks(monkeypatch):
    called = []

    def fake(name):
        def _check():
            called.append(name)
            return {"status": "OK"}
        return _check

    monkeypatch.setitem(health_check.CHECKS, "postgres", fake("postgres"))
    monkeypatch.setitem(health_check.CHECKS, "ollama", fake("ollama"))

    report = health_check.build_report(["postgres"])
    assert called == ["postgres"], "ollama не должен проверяться, если его не просили"
    assert set(report) == {"postgres"}


def test_main_returns_zero_on_warning(monkeypatch, capsys):
    monkeypatch.setitem(
        health_check.CHECKS, "postgres", lambda: {"status": "WARNING", "reason": "one or more tables are empty"}
    )
    assert health_check.main(["--services", "postgres"]) == 0
    assert "WARNING" in capsys.readouterr().err
