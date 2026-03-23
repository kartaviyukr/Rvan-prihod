"""
Проверка пропущенных дат в данных
"""
import os
import glob
from datetime import date, timedelta, time, datetime

import pandas as pd


def check_missing(
    parquet_path: str = None,
    days_back: int = 15,
):
    """
    Проверяет отсутствующие даты в данных за последние N дней.

    Returns:
        (min_missing, max_missing) или (None, None) если все даты есть
    """
    if parquet_path is None:
        from config import Config
        parquet_path = str(Config.RAW_DIR)

    if not os.path.exists(parquet_path):
        return '2025-01-01', '2025-12-29'

    now = datetime.now()
    if now.time() > time(11, 30):
        actual_date = date.today() - timedelta(1)
    else:
        actual_date = date.today() - timedelta(2)
    start_date = actual_date - timedelta(days=days_back)

    range_dates = pd.date_range(start_date, actual_date)

    df = pd.read_parquet(parquet_path)
    df["Дата"] = pd.to_datetime(df["Дата"]).dt.normalize()

    existing_dates = set(df["Дата"])
    missing_dates = pd.DatetimeIndex([d for d in range_dates if d not in existing_dates])

    print(f"Отсутствующие даты: {len(missing_dates)} шт.")

    if len(missing_dates) > 0:
        min_missing = missing_dates.min()
        max_missing = missing_dates.max()
        print(f"Диапазон загрузки: {min_missing} -> {max_missing}")
        return min_missing, max_missing

    print("Отсутствующих дат нет")
    return None, None


def checkup(road: str) -> pd.DataFrame:
    """Загружает последний parquet из директории"""
    parquet = glob.glob(os.path.join(road, '*parquet'))
    parquet.sort(key=os.path.getmtime)
    return pd.read_parquet(parquet[-1])
