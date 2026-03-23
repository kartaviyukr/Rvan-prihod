"""Тесты предобработки данных"""
import pandas as pd
import numpy as np
import pytest

from pipeline.preprocess import (
    base_action,
    collapse_kag_daily_smart,
    zero_small_stocks_conditional,
    drop_weekends_and_holidays,
    fix_competitor_drop_to_zero_anomalies,
)
from config import Config


def test_base_action_filters_active(sample_df):
    sample_df.loc[0, 'Активный КАГ'] = 'Нет'
    result = base_action(sample_df)
    assert len(result) < len(sample_df)
    assert (result['Активный КАГ'] == 'Да').all()


def test_base_action_filters_dates(sample_df):
    sample_df.loc[0, 'Дата'] = pd.Timestamp('2020-01-01')
    result = base_action(sample_df)
    assert result['Дата'].min() > pd.Timestamp('2025-01-01')


def test_collapse_kag_daily_smart_reduces_rows():
    """Проверяет, что дубликаты (КАГ, Дата) сворачиваются"""
    rows = []
    d = pd.Timestamp('2025-03-03')
    for _ in range(3):
        rows.append({
            'Дата': d, 'Код КАГ': '100',
            'ГК (остатки гранд капитала)': 10,
            'Пульс (остатки пульса)': 50,
            'Катрен (остатки катрена)': 40,
            'Протек (остатки протека)': 30,
            'Фармкомплект (остатки фармкомплекта)': 20,
            'Цена пульса': 100.0, 'Цена катрена': 90.0,
            'Цена протека': 80.0, 'Цена фармкомплекта': 70.0,
        })
    df = pd.DataFrame(rows)
    result = collapse_kag_daily_smart(df, show_progress=False)
    assert len(result) == 1


def test_collapse_gk_is_sum():
    """ГК должен суммироваться при свертке"""
    rows = [
        {'Дата': '2025-03-03', 'Код КАГ': '100', 'ГК (остатки гранд капитала)': 10,
         'Пульс (остатки пульса)': 50, 'Катрен (остатки катрена)': 0,
         'Протек (остатки протека)': 0, 'Фармкомплект (остатки фармкомплекта)': 0,
         'Цена пульса': 0, 'Цена катрена': 0, 'Цена протека': 0, 'Цена фармкомплекта': 0},
        {'Дата': '2025-03-03', 'Код КАГ': '100', 'ГК (остатки гранд капитала)': 20,
         'Пульс (остатки пульса)': 50, 'Катрен (остатки катрена)': 0,
         'Протек (остатки протека)': 0, 'Фармкомплект (остатки фармкомплекта)': 0,
         'Цена пульса': 0, 'Цена катрена': 0, 'Цена протека': 0, 'Цена фармкомплекта': 0},
    ]
    result = collapse_kag_daily_smart(pd.DataFrame(rows), show_progress=False)
    assert result['ГК (остатки гранд капитала)'].iloc[0] == 30


def test_zero_small_stocks():
    """Обнуляет остатки <= 10 только если max > 100"""
    df = pd.DataFrame({
        'Код КАГ': ['100', '100', '200', '200'],
        'ГК (остатки гранд капитала)': [5, 200, 5, 50],
        'Пульс (остатки пульса)': [5, 200, 5, 50],
        'Катрен (остатки катрена)': [0, 0, 0, 0],
        'Протек (остатки протека)': [0, 0, 0, 0],
        'Фармкомплект (остатки фармкомплекта)': [0, 0, 0, 0],
    })
    result = zero_small_stocks_conditional(df, verbose=False)
    # КАГ 100: max=200 > 100 → значение 5 обнуляется
    assert result.loc[result['Код КАГ'] == '100', 'ГК (остатки гранд капитала)'].iloc[0] == 0
    # КАГ 200: max=50 < 100 → значение 5 остаётся
    assert result.loc[result['Код КАГ'] == '200', 'ГК (остатки гранд капитала)'].iloc[0] == 5


def test_drop_weekends():
    """Удаляет субботу и воскресенье"""
    dates = pd.date_range('2025-03-01', periods=7)  # Сб, Вс, Пн, Вт, Ср, Чт, Пт
    df = pd.DataFrame({'Дата': dates, 'val': range(7)})
    result = drop_weekends_and_holidays(df, holidays=[], verbose=False)
    assert len(result) == 5  # Только рабочие дни


def test_drop_holidays():
    """Удаляет праздничные дни"""
    dates = pd.to_datetime(['2025-03-03', '2025-01-01', '2025-03-05'])
    df = pd.DataFrame({'Дата': dates, 'val': [1, 2, 3]})
    result = drop_weekends_and_holidays(df, holidays=['2025-01-01'], verbose=False)
    assert len(result) == 2
    assert '2025-01-01' not in result['Дата'].dt.strftime('%Y-%m-%d').values


def test_fix_anomalies_single_zero_gap():
    """Интерполирует 1-дневный провал в 0"""
    dates = pd.date_range('2025-03-03', periods=5, freq='B')
    df = pd.DataFrame({
        'Код КАГ': ['100'] * 5,
        'Дата': dates,
        'Пульс (остатки пульса)': [200, 200, 0, 200, 200],
        'Катрен (остатки катрена)': [100, 100, 100, 100, 100],
        'Протек (остатки протека)': [100, 100, 100, 100, 100],
        'Фармкомплект (остатки фармкомплекта)': [100, 100, 100, 100, 100],
    })
    result = fix_competitor_drop_to_zero_anomalies(df, verbose=False)
    # Пульс на день 3 (индекс 2) должен быть интерполирован (не 0)
    assert result['Пульс (остатки пульса)'].iloc[2] > 0
