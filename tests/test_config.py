"""Тесты конфигурации"""
from config import Config, CategoryConfig


def test_config_columns_defined():
    assert Config.COL_GK
    assert Config.COL_KAG
    assert len(Config.COMP_COLS) == 4
    assert len(Config.PRICE_COLS) == 4


def test_config_comp_pretty_matches():
    for col in Config.COMP_COLS:
        assert col in Config.COMP_PRETTY


def test_config_thresholds():
    assert 0 < Config.DEFECT_EO_PCT < 1
    assert Config.MIN_EO_FOR_PCT > 0
    assert Config.DELTA_ARRIVAL > 0
    assert 0 < Config.MIN_PCT_FROM_YESTERDAY < 1


def test_config_required_columns():
    required = Config.get_required_columns()
    assert Config.COL_DATE in required
    assert Config.COL_KAG in required
    assert Config.COL_GK in required
    for c in Config.COMP_COLS:
        assert c in required


def test_category_config_boundaries():
    assert CategoryConfig.categorize_by_days_ago(0) != CategoryConfig.categorize_by_days_ago(1)
    assert CategoryConfig.categorize_by_days_ago(1) == CategoryConfig.categorize_by_days_ago(40)
    assert CategoryConfig.categorize_by_days_ago(41) == CategoryConfig.categorize_by_days_ago(90)
    assert CategoryConfig.categorize_by_days_ago(91) == CategoryConfig.categorize_by_days_ago(200)


def test_russian_holidays_not_empty():
    assert len(Config.RUSSIAN_HOLIDAYS) > 10
