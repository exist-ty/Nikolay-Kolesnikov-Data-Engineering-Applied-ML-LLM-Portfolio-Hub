"""Data drift check: сравнивает total_amount заказов за последнюю неделю
данных с предыдущей неделей. "Последняя неделя" считается от MAX(order_date)
в stg_orders, а не от today() — датасет синтетический и заморожен в 2025 году
(см. etl-portfolio/scripts/generate_data.py).

Метрика — двухвыборочный z-тест разности средних (Welch) ПЛЮС размер эффекта
(Cohen's d), и срабатывает только когда выполнены оба условия. Почему так:

* Знаменатель z — стандартная ошибка РАЗНОСТИ СРЕДНИХ `sqrt(s1²/n1 + s2²/n2)`,
  а не std самих заказов. Делить разность средних на std наблюдений — ошибка
  масштаба в ~sqrt(n) раз (при n≈50 это ×7): недельный сдвиг среднего на
  треть распределения давал бы z≈0.05 и проверка не срабатывала бы никогда.
* Только значимости мало: на больших n статистически значимым становится
  экономически бессмысленный сдвиг. Поэтому второе условие — Cohen's d выше
  MIN_EFFECT_SIZE (сдвиг не меньше 0.2 объединённого std). Та же логика
  "significance != effect size", что и в power-анализе A/B-теста
  (product-marketing-analytics/ab_test/).

Полноценный KS-тест сознательно не используется: на недельном срезе в
несколько десятков заказов он шумит сильнее, чем сравнение средних.

Результат пишется в drift_result.json рядом со скриптом — его же читает
DAG (задача notify_drift_check в ecosystem_pipeline_dag.py) и передаёт как
тело POST-запроса в n8n webhook triage-drift-check."""
import json
import math
import os
from datetime import timedelta
from pathlib import Path

import numpy as np
from sqlalchemy import create_engine, text

RESULT_PATH = Path(__file__).parent / "drift_result.json"
# Порог значимости: |z| > 2 ≈ p < 0.05 для двусторонней проверки
SIGMA_THRESHOLD = 2.0
# Порог практической значимости: сдвиг меньше 0.2 объединённого std считаем
# шумом, даже если он статистически значим (условная граница "small effect"
# по Коэну)
MIN_EFFECT_SIZE = 0.2


def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', '')}"
        f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'etl_portfolio')}"
    )


def compare_windows(recent: list, previous: list, window_end_date=None) -> dict:
    """Чистая функция сравнения двух окон — вынесена из работы с БД, чтобы
    покрываться юнит-тестами без живого Postgres (см. tests/test_check_drift.py)."""
    base = {"window_end_date": str(window_end_date) if window_end_date is not None else None}

    if len(recent) < 2 or len(previous) < 2:
        return {
            **base,
            "drift_detected": False,
            "reason": f"not enough data for a weekly comparison (recent={len(recent)}, previous={len(previous)})",
        }

    recent_arr = np.array(recent, dtype=float)
    previous_arr = np.array(previous, dtype=float)
    n1, n2 = len(recent_arr), len(previous_arr)
    recent_mean, prev_mean = recent_arr.mean(), previous_arr.mean()
    # ddof=1 — выборочная дисперсия: оба окна это выборка, а не генеральная
    # совокупность
    recent_var, prev_var = recent_arr.var(ddof=1), previous_arr.var(ddof=1)

    # Стандартная ошибка РАЗНОСТИ СРЕДНИХ (Welch — не требует равенства
    # дисперсий, окна разного размера и разной волатильности)
    standard_error = math.sqrt(recent_var / n1 + prev_var / n2)
    # Объединённый std — знаменатель Cohen's d
    pooled_std = math.sqrt(((n1 - 1) * recent_var + (n2 - 1) * prev_var) / (n1 + n2 - 2))

    mean_diff = float(recent_mean - prev_mean)
    z_score = None if standard_error == 0 else mean_diff / standard_error
    effect_size = None if pooled_std == 0 else mean_diff / pooled_std

    # Оба условия обязательны: значимость без размера эффекта — шум на большом
    # n, размер эффекта без значимости — случайность на малом
    drift_detected = bool(
        z_score is not None
        and effect_size is not None
        and abs(z_score) > SIGMA_THRESHOLD
        and abs(effect_size) > MIN_EFFECT_SIZE
    )

    return {
        **base,
        # bool()/float() обязательны: numpy.bool_ и numpy.float64 не
        # сериализуются json.dumps (реальный баг, пойманный живым прогоном
        # DAG — см. docs/orchestration.md)
        "drift_detected": drift_detected,
        "z_score": round(z_score, 3) if z_score is not None else None,
        "effect_size": round(effect_size, 3) if effect_size is not None else None,
        "sigma_threshold": SIGMA_THRESHOLD,
        "min_effect_size": MIN_EFFECT_SIZE,
        "mean_diff": round(mean_diff, 2),
        "recent_mean": round(float(recent_mean), 2),
        "recent_n": n1,
        "previous_mean": round(float(prev_mean), 2),
        "previous_n": n2,
    }


def check_drift() -> dict:
    engine = get_engine()
    with engine.connect() as conn:
        max_order_date = conn.execute(text("SELECT max(order_date) FROM stg_orders")).scalar()
        if max_order_date is None:
            return {"drift_detected": False, "reason": "stg_orders is empty", "window_end_date": None}

        recent = conn.execute(
            text("SELECT total_amount FROM stg_orders WHERE order_date > :start AND order_date <= :end"),
            {"start": max_order_date - timedelta(days=7), "end": max_order_date},
        ).scalars().all()
        previous = conn.execute(
            text("SELECT total_amount FROM stg_orders WHERE order_date > :start AND order_date <= :end"),
            {"start": max_order_date - timedelta(days=14), "end": max_order_date - timedelta(days=7)},
        ).scalars().all()

    return compare_windows(recent, previous, window_end_date=max_order_date)


def main() -> None:
    result = check_drift()
    print(json.dumps(result, indent=2, ensure_ascii=False))
    RESULT_PATH.write_text(json.dumps(result))


if __name__ == "__main__":
    main()
