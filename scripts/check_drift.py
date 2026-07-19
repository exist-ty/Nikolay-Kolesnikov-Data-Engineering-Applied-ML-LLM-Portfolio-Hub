"""Data drift check: сравнивает total_amount заказов за последнюю неделю
данных с предыдущей неделей (простое сравнение среднего/std, не полноценный
KS-test — на паре тысяч заказов недельный срез даёт всего десятки точек,
полноценный тест на таком n просто шумит). "Последняя неделя" считается от
MAX(order_date) в stg_orders, а не от today() — датасет синтетический и
заморожен в 2025 году (см. etl-portfolio/scripts/generate_data.py).

Результат пишется в drift_result.json рядом со скриптом — его же читает
DAG (задача notify_drift_check в ecosystem_pipeline_dag.py) и передаёт как
тело POST-запроса в n8n webhook triage-drift-check."""
import json
import os
from datetime import timedelta
from pathlib import Path

import numpy as np
from sqlalchemy import create_engine, text

RESULT_PATH = Path(__file__).parent / "drift_result.json"
SIGMA_THRESHOLD = 2.0


def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', '')}"
        f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'etl_portfolio')}"
    )


def check_drift() -> dict:
    engine = get_engine()
    with engine.connect() as conn:
        max_order_date = conn.execute(text("SELECT max(order_date) FROM stg_orders")).scalar()
        if max_order_date is None:
            return {"drift_detected": False, "reason": "stg_orders is empty"}

        recent = conn.execute(
            text("SELECT total_amount FROM stg_orders WHERE order_date > :start AND order_date <= :end"),
            {"start": max_order_date - timedelta(days=7), "end": max_order_date},
        ).scalars().all()
        previous = conn.execute(
            text("SELECT total_amount FROM stg_orders WHERE order_date > :start AND order_date <= :end"),
            {"start": max_order_date - timedelta(days=14), "end": max_order_date - timedelta(days=7)},
        ).scalars().all()

    if len(recent) < 2 or len(previous) < 2:
        return {
            "drift_detected": False,
            "reason": f"not enough data for a weekly comparison (recent={len(recent)}, previous={len(previous)})",
            "window_end_date": str(max_order_date),
        }

    recent_arr = np.array(recent, dtype=float)
    previous_arr = np.array(previous, dtype=float)
    prev_mean, prev_std = previous_arr.mean(), previous_arr.std(ddof=1)
    recent_mean = recent_arr.mean()

    z_score = None if prev_std == 0 else float((recent_mean - prev_mean) / prev_std)
    # numpy.bool_ (из сравнения numpy.float64) не сериализуется json.dumps —
    # явный bool() в отличие от numpy.float64, который де-факто подкласс float
    drift_detected = bool(z_score is not None and abs(z_score) > SIGMA_THRESHOLD)

    return {
        "drift_detected": drift_detected,
        "z_score": round(z_score, 3) if z_score is not None else None,
        "sigma_threshold": SIGMA_THRESHOLD,
        "recent_mean": round(float(recent_mean), 2),
        "recent_n": len(recent),
        "previous_mean": round(float(prev_mean), 2),
        "previous_n": len(previous),
        "window_end_date": str(max_order_date),
    }


def main() -> None:
    result = check_drift()
    print(json.dumps(result, indent=2, ensure_ascii=False))
    RESULT_PATH.write_text(json.dumps(result))


if __name__ == "__main__":
    main()
