"""Тесты на scripts/check_drift.py — чистая функция compare_windows(),
без живого Postgres.

Отдельно проверяется сериализуемость результата: живой прогон DAG уже ловил
`TypeError: Object of type bool is not JSON serializable` (drift_detected был
numpy.bool_ — см. docs/orchestration.md). Баг нашёлся руками в прогоне; здесь
он закрыт тестом.
"""
import json
import math

import pytest

from scripts.check_drift import MIN_EFFECT_SIZE, SIGMA_THRESHOLD, compare_windows


def test_identical_windows_no_drift():
    window = [100.0, 120.0, 140.0, 90.0, 110.0]
    result = compare_windows(window, list(window))
    assert result["drift_detected"] is False
    assert result["z_score"] == 0.0
    assert result["effect_size"] == 0.0


def test_large_shift_is_detected():
    previous = [100.0 + i for i in range(40)]
    recent = [400.0 + i for i in range(40)]
    result = compare_windows(recent, previous)
    assert result["drift_detected"] is True
    assert result["z_score"] > SIGMA_THRESHOLD
    assert result["effect_size"] > MIN_EFFECT_SIZE


def test_z_score_uses_standard_error_not_raw_std():
    """Регрессия на исходный баг: знаменатель — стандартная ошибка разности
    средних, а не std наблюдений. Со старой формулой (деление на prev_std)
    сдвиг на половину std давал бы |z| ~ 0.5 и не срабатывал никогда;
    с правильной — |z| растёт как sqrt(n) и на 60 точках уверенно проходит
    порог."""
    previous = [100.0, 105.0, 95.0, 110.0, 90.0] * 12   # std ~7.2, n=60
    recent = [104.0, 109.0, 99.0, 114.0, 94.0] * 12     # тот же разброс, +4
    result = compare_windows(recent, previous)

    naive_z = (result["recent_mean"] - result["previous_mean"]) / 7.24
    assert abs(naive_z) < SIGMA_THRESHOLD, "сдвиг мал в масштабе std наблюдений"
    assert abs(result["z_score"]) > SIGMA_THRESHOLD, "но значим в масштабе SE"


def test_significant_but_tiny_effect_is_not_drift():
    """Значимость без размера эффекта — не дрейф. На большом n статистически
    значимым становится сдвиг, который бизнесу безразличен."""
    previous = [100.0, 101.0, 99.0, 100.5, 99.5] * 400
    recent = [100.1, 101.1, 99.1, 100.6, 99.6] * 400
    result = compare_windows(recent, previous)
    assert abs(result["z_score"]) > SIGMA_THRESHOLD
    assert abs(result["effect_size"]) < MIN_EFFECT_SIZE
    assert result["drift_detected"] is False


@pytest.mark.parametrize(
    "recent, previous",
    [([], []), ([100.0], [100.0, 110.0]), ([100.0, 110.0], [100.0])],
)
def test_not_enough_data(recent, previous):
    result = compare_windows(recent, previous)
    assert result["drift_detected"] is False
    assert "not enough data" in result["reason"]


def test_zero_variance_windows_do_not_divide_by_zero():
    result = compare_windows([100.0] * 5, [100.0] * 5)
    assert result["drift_detected"] is False
    assert result["z_score"] is None
    assert result["effect_size"] is None


def test_result_is_json_serializable():
    """DAG отдаёт этот JSON как тело POST в n8n (задача notify_drift_check) —
    numpy-типы здесь ломают весь прогон."""
    result = compare_windows([1.0, 2.0, 3.0, 4.0], [9.0, 8.0, 7.0, 6.0], window_end_date="2025-12-31")
    encoded = json.dumps(result)
    assert isinstance(result["drift_detected"], bool)
    assert type(result["drift_detected"]).__module__ == "builtins"
    assert json.loads(encoded)["window_end_date"] == "2025-12-31"


def test_effect_size_matches_cohens_d_definition():
    recent, previous = [10.0, 12.0, 14.0, 16.0], [4.0, 6.0, 8.0, 10.0]
    result = compare_windows(recent, previous)
    n = len(recent)
    var_r = sum((x - 13.0) ** 2 for x in recent) / (n - 1)
    var_p = sum((x - 7.0) ** 2 for x in previous) / (n - 1)
    pooled = math.sqrt(((n - 1) * var_r + (n - 1) * var_p) / (2 * n - 2))
    assert result["effect_size"] == pytest.approx(6.0 / pooled, rel=1e-3)
