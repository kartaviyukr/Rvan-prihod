"""
Загрузка данных из SQL Server в Parquet
"""
import os
import sys
import time
import threading
import itertools
from datetime import date

import pandas as pd
import pyodbc

from config import Config


class _Spinner:
    """Анимация ожидания SQL-запроса"""

    def __init__(self, message="Выполняется SQL-запрос... "):
        self._cycle = itertools.cycle(["|", "/", "-", "\\"])
        self._running = False
        self._thread = None
        self._message = message

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._animate, daemon=True)
        self._thread.start()

    def _animate(self):
        while self._running:
            sys.stdout.write(f"\r{self._message}{next(self._cycle)}")
            sys.stdout.flush()
            time.sleep(0.2)

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join()
        sys.stdout.write("\r")
        sys.stdout.flush()


def _connect():
    return pyodbc.connect(
        f"DRIVER={Config.DB_SOURCE_DRIVER};"
        f"SERVER={Config.DB_SOURCE_SERVER};"
        f"DATABASE={Config.DB_SOURCE_DATABASE};"
        f"UID={Config.DB_SOURCE_UID};"
        f"PWD={Config.DB_SOURCE_PWD};"
        "TrustServerCertificate=yes;",
        autocommit=True,
    )


def export_ai_stock_to_parquet(
    start_date,
    end_date,
    *,
    output_dir: str = None,
    maxdop: int = 15,
) -> pd.DataFrame:
    """
    Выгружает данные из SQL за период [start_date, end_date] включительно.
    Сохраняет в один parquet-файл.
    """
    if output_dir is None:
        output_dir = str(Config.INTERIM_DIR)

    end_dt_excl = end_date + pd.Timedelta(days=1)
    start_sql = start_date.strftime("%Y-%m-%d")
    end_sql = end_dt_excl.strftime("%Y-%m-%d")

    print(f"Выгрузка периода: {start_date} -> {end_date} (включительно)")

    conn = None
    try:
        conn = _connect()
        print("Соединение с БД установлено")

        sql = f"""
        SELECT *
        FROM [{Config.DB_SOURCE_SCHEMA}].[{Config.DB_SOURCE_VIEW}] WITH (NOLOCK)
        WHERE [Дата] >= '{start_sql}'
          AND [Дата] < '{end_sql}'
        OPTION (MAXDOP {int(maxdop)})
        """

        spinner = _Spinner()
        spinner.start()
        t0 = time.time()

        df = pd.read_sql(sql, conn)

        spinner.stop()
        elapsed = time.time() - t0

        print(f"Получено строк: {len(df):,}  ({elapsed / 60:.2f} мин)")

        if len(df) == 0:
            print("Данных нет.")
            return pd.DataFrame()

        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, f"ai_stock_{date.today()}.parquet")

        df.to_parquet(out_path, index=False)
        print(f"Сохранено: {out_path}")

        return df

    finally:
        if conn:
            conn.close()


def _normalize_parquet(df, date_col="Дата", kag_col="Код КАГ"):
    """Нормализация типов для parquet: дата -> datetime, КАГ -> Int64"""
    df = df.copy()
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
    if kag_col in df.columns:
        s = df[kag_col].astype("string").str.strip().str.replace(r"\.0$", "", regex=True)
        df[kag_col] = pd.to_numeric(s, errors="coerce").astype("Int64")
    return df


def union_all_parquet(
    df_new: pd.DataFrame,
    *,
    parquet_path: str = None,
) -> None:
    """Объединяет новый DataFrame с существующим raw parquet"""
    if parquet_path is None:
        parquet_path = str(Config.RAW_PARQUET)

    df_new = _normalize_parquet(df_new)

    if os.path.exists(parquet_path):
        df_old = _normalize_parquet(pd.read_parquet(parquet_path))
        print(f"Размер исходного df = {df_old.shape}")
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        print(f"Parquet не найден, создаю новый: {parquet_path}")
        df_all = df_new

    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    df_all.to_parquet(parquet_path, index=False)
    print(f"Размер итогового df = {df_all.shape}")


def union_all_clean_parquet(
    df_new: pd.DataFrame,
    *,
    parquet_path: str = None,
) -> None:
    """Объединяет с существующим clean parquet, с дедупликацией по датам"""
    if parquet_path is None:
        parquet_path = str(Config.CLEAN_PARQUET)

    date_col, kag_col = "Дата", "Код КАГ"
    df_new = _normalize_parquet(df_new, date_col, kag_col)

    if os.path.exists(parquet_path):
        df_old = _normalize_parquet(pd.read_parquet(parquet_path), date_col, kag_col)
        print(f"Размер исходного df = {df_old.shape}")

        # Фильтруем дубликаты дат
        if date_col in df_new.columns and date_col in df_old.columns:
            existing_dates = set(df_old[date_col].dropna().unique())
            new_dates = set(df_new[date_col].dropna().unique())
            duplicates = new_dates & existing_dates

            if duplicates:
                print(f"Найдено {len(duplicates)} дат, которые уже есть")
                df_new = df_new[~df_new[date_col].isin(duplicates)]
                if df_new.empty:
                    print("Все данные уже присутствуют")
                    return

        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        print(f"Parquet не найден, создаю новый: {parquet_path}")
        df_all = df_new

    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    df_all.to_parquet(parquet_path, index=False)
    print(f"Размер итогового df = {df_all.shape}")
