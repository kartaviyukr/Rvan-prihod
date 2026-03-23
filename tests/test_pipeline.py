"""Интеграционные тесты пайплайна"""
import pandas as pd
import numpy as np
import pytest

from pipeline.data_preparation import DataPreparator
from pipeline.defectura_detection import DefecturaDetector
from pipeline.arrival_detection import ArrivalDetector
from config import Config


def test_full_analysis_flow(defectura_df):
    """Полный цикл: подготовка → фильтрация → детекция дефектуры → детекция прихода"""
    # Подготовка
    prep = DataPreparator()
    df, work_date = prep.validate_and_prepare(defectura_df)
    assert 'date_n' in df.columns
    assert 'sum_competitors' in df.columns

    # Фильтрация
    detector = DefecturaDetector()
    detector._eo_map = {}
    eligible = detector.filter_eligible_kags(df, work_date)
    assert len(eligible) >= 1

    # Хранилище
    kag_store = prep.build_kag_store(df, eligible)
    assert len(kag_store) > 0
    for kag, store in kag_store.items():
        assert 'dates' in store
        assert 'gk' in store
        assert 'sumc' in store
        assert 'comps' in store

    # Дефектура на последней точке
    defects = detector.find_last_point_defects(df, eligible)
    assert isinstance(defects, pd.DataFrame)

    # Эпизоды
    episodes = detector.find_defectura_episodes(kag_store, eligible, work_date)
    assert isinstance(episodes, pd.DataFrame)

    # Детекция прихода
    arrival = ArrivalDetector()
    for kag in eligible:
        st = kag_store.get(kag)
        if st is None:
            continue
        result = arrival.detect_from_numpy_store(st, df['date_n'].min(), None, work_date)
        assert 'arrival_flag' in result
        assert 'arrival_events_cnt' in result


def test_data_preparator_validates_columns():
    """Ошибка при отсутствии обязательных колонок"""
    df = pd.DataFrame({'foo': [1, 2, 3]})
    prep = DataPreparator()
    with pytest.raises(KeyError, match="Отсутствуют колонки"):
        prep.validate_and_prepare(df)


def test_data_preparator_deduplicates(sample_df):
    """Дедупликация по (КАГ, Дата)"""
    # Дублируем строку
    dup = pd.concat([sample_df, sample_df.iloc[:1]], ignore_index=True)
    prep = DataPreparator()
    df, _ = prep.validate_and_prepare(dup)
    # Не должно быть дубликатов
    assert df.duplicated(subset=[Config.COL_KAG, 'date_n']).sum() == 0


def test_kag_store_structure(defectura_df):
    """Структура kag_store корректна"""
    prep = DataPreparator()
    df, _ = prep.validate_and_prepare(defectura_df)
    store = prep.build_kag_store(df, ['100', '200'])

    for kag in ['100', '200']:
        assert kag in store
        s = store[kag]
        assert s['dates'].dtype.kind == 'M'  # datetime
        assert s['gk'].dtype == float
        assert s['sumc'].dtype == float
        for comp_col in Config.COMP_COLS:
            assert comp_col in s['comps']
            assert s['comps'][comp_col].dtype == float
