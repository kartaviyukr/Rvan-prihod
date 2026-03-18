from datetime import date, timedelta, time, datetime
import pandas as pd
import glob
import os


def check_missing(
    parquet_path: str = r"C:\Проекты\Project_etl_power_bi\data\raw",
    days_back: int = 15,
):
    """
    Проверяет отсутствующие даты в витрине за последние N дней.

    Возвращает:
        min_missing — Дата, с которйо начнётся выгрузка
        max_missing  — Дата, до которой бдет выполняться выгрузка
    """
    if not os.path.exists(parquet_path):
        return '2025-01-01', '2025-12-29'
    now = datetime.now()
    #диапазон дат для проверки. По умолчанию смотрим на период в 15 дней
    if now.time() > time(11, 30):
        actual_date = date.today() - timedelta(1)
    else:
        actual_date = date.today() - timedelta(2)
    start_date = actual_date - timedelta(days=days_back)

    range_dates = pd.date_range(start_date, actual_date)

    print("Диапазон дат для проверки:")
    print(range_dates)
    print()

    # Смотрим в главный паркет. Есть ли там эти даты? 
    df = pd.read_parquet(parquet_path)

    df["Дата"] = pd.to_datetime(df["Дата"]).dt.normalize()

    # поиск отсутствующих дат
    existing_dates = set(df["Дата"])
    missing_dates = pd.DatetimeIndex([d for d in range_dates if d not in existing_dates])

    print(f"Отсутствующие даты ({len(missing_dates)} шт.):")
    print(missing_dates)
    print()

    if len(missing_dates) > 0:
        min_missing = missing_dates.min()
        max_missing = missing_dates.max()
        print(f"Диапазон загрузки с {min_missing} до {max_missing}")
    else:
        min_missing = None
        max_missing = None
        print("Отсутствующих дат нет")

    return min_missing, max_missing

def checkup(road: str):
    parquet = glob.glob(os.path.join(road, '*parquet'))
    parquet.sort(key=os.path.getmtime)
    last = parquet[-1]
    df = pd.read_parquet(last)
    return df