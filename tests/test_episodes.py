"""Тесты расчёта эпизодов"""
import numpy as np
import pandas as pd
import pytest

from pipeline.defectura_detection import DefecturaDetector
from pipeline.data_preparation import DataPreparator
from config import Config


@pytest.fixture
def long_defectura_df():
    """DataFrame с длинным эпизодом дефектуры"""
    dates = pd.date_range('2025-01-02', periods=50, freq='B')
    rows = []
    for i, d in enumerate(dates):
        # КАГ 300: первые 10 дней ГК > 0, потом 0
        gk = 500 if i < 10 else 0
        rows.append({
            'Дата': d, 'Код КАГ': '300', 'Имя КАГ': 'Длинная дефектура',
            'ГК (остатки гранд капитала)': gk,
            'Пульс (остатки пульса)': 200,
            'Катрен (остатки катрена)': 150,
            'Протек (остатки протека)': 100,
            'Фармкомплект (остатки фармкомплекта)': 80,
        })
    return pd.DataFrame(rows)


def test_episode_start_date(long_defectura_df):
    """Дата начала эпизода должна быть после последнего ГК > 0"""
    detector = DefecturaDetector()
    detector._eo_map = {}

    prep = DataPreparator()
    df, work_date = prep.validate_and_prepare(long_defectura_df)
    eligible = detector.filter_eligible_kags(df, work_date)
    kag_store = prep.build_kag_store(df, eligible)
    episodes = detector.find_defectura_episodes(kag_store, eligible, work_date, lookback_days=365)

    assert len(episodes) >= 1
    ep = episodes[episodes[Config.COL_KAG] == '300'].iloc[0]
    # Эпизод начинается на 11-й рабочий день (индекс 10)
    dates = pd.date_range('2025-01-02', periods=50, freq='B')
    assert ep['defect_start_date'] == dates[10].normalize()


def test_active_episode_no_end_date(long_defectura_df):
    """Активный эпизод не имеет даты окончания"""
    detector = DefecturaDetector()
    detector._eo_map = {}

    prep = DataPreparator()
    df, work_date = prep.validate_and_prepare(long_defectura_df)
    eligible = detector.filter_eligible_kags(df, work_date)
    kag_store = prep.build_kag_store(df, eligible)
    episodes = detector.find_defectura_episodes(kag_store, eligible, work_date, lookback_days=365)

    ep = episodes[episodes[Config.COL_KAG] == '300'].iloc[0]
    assert ep['is_finished'] is False
    assert pd.isna(ep['defect_end_date'])


def test_finished_episode():
    """Завершившийся эпизод имеет дату окончания"""
    dates = pd.date_range('2025-02-01', periods=20, freq='B')
    rows = []
    for i, d in enumerate(dates):
        # ГК: 500 первые 5, потом 0 на 5 дней, потом снова 500
        if i < 5 or i >= 10:
            gk = 500
        else:
            gk = 0
        rows.append({
            'Дата': d, 'Код КАГ': '400',
            'ГК (остатки гранд капитала)': gk,
            'Пульс (остатки пульса)': 200,
            'Катрен (остатки катрена)': 150,
            'Протек (остатки протека)': 100,
            'Фармкомплект (остатки фармкомплекта)': 80,
        })
    df = pd.DataFrame(rows)

    detector = DefecturaDetector()
    detector._eo_map = {}
    prep = DataPreparator()
    prepared, work_date = prep.validate_and_prepare(df)
    eligible = detector.filter_eligible_kags(prepared, work_date)
    kag_store = prep.build_kag_store(prepared, eligible)
    episodes = detector.find_defectura_episodes(kag_store, eligible, work_date, lookback_days=365)

    finished = episodes[episodes['is_finished'] == True]
    assert len(finished) >= 1
    assert pd.notna(finished.iloc[0]['defect_end_date'])
