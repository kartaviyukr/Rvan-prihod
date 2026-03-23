"""Тесты детекции прихода"""
import numpy as np
import pandas as pd
import pytest

from pipeline.arrival_detection import ArrivalDetector
from config import Config


@pytest.fixture
def detector():
    return ArrivalDetector()


def test_detect_no_arrival(detector):
    """Нет прихода при стабильных остатках"""
    dates = pd.date_range('2025-03-01', periods=5, freq='B')
    df = pd.DataFrame({
        'date_n': dates,
        'Пульс (остатки пульса)': [100, 100, 100, 100, 100],
    })
    events = detector.detect_in_window(df, 'Пульс (остатки пульса)', dates[0], dates[-1])
    assert events == []


def test_detect_arrival_large_jump(detector):
    """Приход при росте >= DELTA_ARRIVAL"""
    dates = pd.date_range('2025-03-01', periods=5, freq='B')
    df = pd.DataFrame({
        'date_n': dates,
        'Пульс (остатки пульса)': [200, 200, 200, 500, 500],  # +300 на день 4
    })
    events = detector.detect_in_window(df, 'Пульс (остатки пульса)', dates[0], dates[-1])
    assert len(events) == 1
    assert events[0][1] == 300.0  # дельта


def test_detect_no_arrival_small_jump(detector):
    """Нет прихода при росте < DELTA_ARRIVAL"""
    dates = pd.date_range('2025-03-01', periods=3, freq='B')
    df = pd.DataFrame({
        'date_n': dates,
        'Пульс (остатки пульса)': [200, 250, 250],  # +50, меньше 100
    })
    events = detector.detect_in_window(df, 'Пульс (остатки пульса)', dates[0], dates[-1])
    assert events == []


def test_detect_for_all_competitors(detector):
    """Детекция по всем конкурентам"""
    dates = pd.date_range('2025-03-01', periods=5, freq='B')
    df = pd.DataFrame({
        'date_n': dates,
        'Пульс (остатки пульса)': [200, 200, 500, 500, 500],
        'Катрен (остатки катрена)': [100, 100, 100, 100, 100],
        'Протек (остатки протека)': [100, 100, 100, 400, 400],
        'Фармкомплект (остатки фармкомплекта)': [50, 50, 50, 50, 50],
    })
    results = detector.detect_for_all_competitors(df, dates[0], dates[-1])
    assert Config.COL_PULS in results  # Пульс: +300
    assert Config.COL_PROTEK in results  # Протек: +300
    assert Config.COL_KATREN not in results  # Катрен стабилен
    assert Config.COL_FK not in results  # ФК стабилен


def test_detect_from_numpy_store(detector):
    """Детекция из предподготовленного numpy-хранилища"""
    dates = np.array(pd.date_range('2025-03-01', periods=5, freq='B'))
    store = {
        'dates': dates,
        'gk': np.array([0, 0, 0, 0, 0], dtype=float),
        'sumc': np.array([500, 500, 800, 800, 800], dtype=float),
        'comps': {
            Config.COL_PULS: np.array([200, 200, 500, 500, 500], dtype=float),
            Config.COL_KATREN: np.array([100, 100, 100, 100, 100], dtype=float),
            Config.COL_PROTEK: np.array([100, 100, 100, 100, 100], dtype=float),
            Config.COL_FK: np.array([100, 100, 100, 100, 100], dtype=float),
        },
    }
    result = detector.detect_from_numpy_store(
        store, pd.Timestamp('2025-03-01'), None, pd.Timestamp('2025-03-07'),
    )
    assert result['arrival_flag'] is True
    assert result['arrival_events_cnt'] >= 1


def test_empty_result(detector):
    result = detector._empty_result()
    assert result['arrival_flag'] is False
    assert result['arrival_events_cnt'] == 0
