"""Общие фикстуры для тестов"""
import numpy as np
import pandas as pd
import pytest

from config import Config


@pytest.fixture
def config():
    return Config


@pytest.fixture
def sample_df():
    """Базовый DataFrame с данными по 2 КАГ за 5 дней"""
    dates = pd.date_range('2025-03-01', periods=5, freq='B')  # Рабочие дни
    rows = []
    for kag in ['100', '200']:
        for i, d in enumerate(dates):
            rows.append({
                'Дата': d,
                'Код КАГ': kag,
                'Имя КАГ': f'Товар {kag}',
                'Активный КАГ': 'Да',
                'ГК (остатки гранд капитала)': 50 + i * 10 if kag == '100' else 0,
                'Пульс (остатки пульса)': 100 + i * 20,
                'Катрен (остатки катрена)': 80 + i * 15,
                'Протек (остатки протека)': 60 + i * 10,
                'Фармкомплект (остатки фармкомплекта)': 40 + i * 5,
                'Цена пульса': 150.0,
                'Цена катрена': 140.0,
                'Цена протека': 130.0,
                'Цена фармкомплекта': 120.0,
            })
    return pd.DataFrame(rows)


@pytest.fixture
def prepared_df(sample_df):
    """DataFrame после validate_and_prepare"""
    from pipeline.data_preparation import DataPreparator
    prep = DataPreparator()
    df, work_date = prep.validate_and_prepare(sample_df)
    return df, work_date


@pytest.fixture
def defectura_df():
    """DataFrame где КАГ 200 в дефектуре (ГК=0), конкуренты растут"""
    dates = pd.date_range('2025-03-01', periods=10, freq='B')
    rows = []
    for i, d in enumerate(dates):
        # КАГ 100: нормальный, ГК > 0
        rows.append({
            'Дата': d, 'Код КАГ': '100', 'Имя КАГ': 'Нормальный',
            'ГК (остатки гранд капитала)': 500,
            'Пульс (остатки пульса)': 200,
            'Катрен (остатки катрена)': 150,
            'Протек (остатки протека)': 100,
            'Фармкомплект (остатки фармкомплекта)': 80,
        })
        # КАГ 200: дефектура, ГК=0, конкуренты получают приход на день 5
        puls = 200 if i < 5 else 200 + (i - 4) * 150
        rows.append({
            'Дата': d, 'Код КАГ': '200', 'Имя КАГ': 'Дефектурный',
            'ГК (остатки гранд капитала)': 100 if i < 2 else 0,
            'Пульс (остатки пульса)': puls,
            'Катрен (остатки катрена)': 150,
            'Протек (остатки протека)': 100,
            'Фармкомплект (остатки фармкомплекта)': 80,
        })
    return pd.DataFrame(rows)
