"""Тесты детекции дефектуры"""
import numpy as np
import pandas as pd
import pytest

from pipeline.defectura_detection import DefecturaDetector
from pipeline.data_preparation import DataPreparator
from config import Config


@pytest.fixture
def detector():
    d = DefecturaDetector()
    # Подставляем пустую карту ЭО для тестов (порог = 0, т.е. дефектура при ГК == 0)
    d._eo_map = {}
    return d


def test_threshold_without_eo(detector):
    """Без ЭО порог = 0"""
    assert detector._get_threshold('12345') == 0.0


def test_threshold_with_small_eo(detector):
    """ЭО < MIN_EO_FOR_PCT → порог = 0"""
    detector._eo_map = {'100': 3.0}
    assert detector._get_threshold('100') == 0.0


def test_threshold_with_large_eo(detector):
    """ЭО >= MIN_EO_FOR_PCT → порог = DEFECT_EO_PCT * ЭО"""
    detector._eo_map = {'100': 100.0}
    expected = Config.DEFECT_EO_PCT * 100.0
    assert detector._get_threshold('100') == expected


def test_filter_eligible_kags(detector, defectura_df):
    prep = DataPreparator()
    df, work_date = prep.validate_and_prepare(defectura_df)
    eligible = detector.filter_eligible_kags(df, work_date)
    assert len(eligible) > 0
    # Оба КАГ имеют ГК > 0 когда-либо
    assert '100' in eligible
    assert '200' in eligible


def test_find_last_point_defects(detector, defectura_df):
    prep = DataPreparator()
    df, work_date = prep.validate_and_prepare(defectura_df)
    eligible = detector.filter_eligible_kags(df, work_date)
    defects = detector.find_last_point_defects(df, eligible)
    # КАГ 200 в дефектуре (ГК=0 на последнюю дату)
    assert '200' in defects[Config.COL_KAG].values
    # КАГ 100 НЕ в дефектуре
    assert '100' not in defects[Config.COL_KAG].values


def test_find_episodes(detector, defectura_df):
    prep = DataPreparator()
    df, work_date = prep.validate_and_prepare(defectura_df)
    eligible = detector.filter_eligible_kags(df, work_date)
    kag_store = prep.build_kag_store(df, eligible)
    episodes = detector.find_defectura_episodes(kag_store, eligible, work_date)
    # Должен быть хотя бы 1 эпизод для КАГ 200
    assert len(episodes) > 0
    assert '200' in episodes[Config.COL_KAG].values
