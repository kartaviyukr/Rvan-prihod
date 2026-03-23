"""
Загрузка результатов в SQL Server
"""
import os
import warnings
import urllib.parse

import pandas as pd
import pyodbc  # noqa
from sqlalchemy import create_engine, text
from tqdm.auto import tqdm

from config import Config

warnings.filterwarnings("ignore")

# Таблицы
TABLE_XLSX = "AI_cut_data_ragged_arrival"
TABLE_PARQUET = "AI_full_data_ragged_arrival"

CHUNK_ROWS = 100_000


def _get_engine():
    odbc_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={Config.DB_DEST_SERVER};"
        f"DATABASE={Config.DB_DEST_DATABASE};"
        "Trusted_Connection=yes;"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )
    conn_str = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_str)
    return create_engine(conn_str, fast_executemany=True)


def _table_exists(engine, table_name: str) -> bool:
    with engine.connect() as conn:
        result = conn.execute(text(
            f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'"
        ))
        return result.scalar() > 0


def test_connection() -> bool:
    try:
        engine = _get_engine()
        with engine.connect() as conn:
            pd.read_sql("SELECT 1 AS ok", conn)
        print("Подключение к БД OK")
        return True
    except Exception as e:
        print(f"Ошибка подключения: {e}")
        return False


def load_xlsx_to_sql(xlsx_path: str = None, table_name: str = TABLE_XLSX):
    """Загрузка Excel в SQL Server (полная перезапись)."""
    if xlsx_path is None:
        xlsx_path = str(Config.OUT_DIR / "final_merged_table.xlsx")

    engine = _get_engine()
    df = pd.read_excel(xlsx_path)
    print(f"Загрузка Excel в {table_name}: {len(df)} строк")

    exists = _table_exists(engine, table_name)
    if exists:
        with engine.connect() as conn:
            conn.execute(text(f"DELETE FROM [dbo].[{table_name}]"))
            conn.commit()

    # Типизация
    bigint_cols = [
        "Код КАГ", "Длительность дефектуры, дней",
        "Количество приходов после дефектуры у конкурентов (всего)",
        "Кол-во конкурентов с приходами", "Общий объём прихода после дефектуры",
        "Объём прихода Пульс (сумма)", "Объём прихода Катрен (сумма)",
        "Объём прихода Протек (сумма)", "Объём прихода Фармкомплект (сумма)",
        "Остаток Пульс (вчера)", "Остаток Катрен (вчера)",
        "Остаток Протек (вчера)", "Остаток Фармкомплект (вчера)",
        "Остаток ФК Гранд Капитал (последняя дата)",
    ]
    for col in bigint_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["Рейтинг Внешний"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    nvarchar_cols = [
        "Имя КАГ", "Статус",
        "Дата входа в дефектуру ФК Гранд Капитал",
        "Дата окончания дефектуры ФК Гранд Капитал",
        "Конкуренты с приходами",
        "Приходы Пульс (дата-объём)", "Приходы Катрен (дата-объём)",
        "Приходы Протек (дата-объём)", "Приходы Фармкомплект (дата-объём)",
    ]
    for col in nvarchar_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("nan", None)

    df.replace([float("inf"), float("-inf")], pd.NA, inplace=True)

    df.to_sql(table_name, engine, schema="dbo", if_exists="append" if exists else "replace", index=False)
    print(f"Загружено {len(df)} строк в {table_name}")


def _get_existing_dates(engine, table_name: str) -> set:
    try:
        with engine.connect() as conn:
            df = pd.read_sql(f"SELECT DISTINCT CAST([Дата] AS DATE) as dt FROM [dbo].[{table_name}]", conn)
        return set(pd.to_datetime(df["dt"]).dt.date)
    except Exception:
        return set()


def _get_parquet_dates(parquet_path: str) -> set:
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(parquet_path)
    dates = set()
    for batch in pf.iter_batches(batch_size=500_000, columns=["Дата"]):
        df = batch.to_pandas()
        df["Дата"] = pd.to_datetime(df["Дата"], errors="coerce")
        dates.update(df["Дата"].dt.date.dropna().unique())
    return dates


def load_parquet_to_sql_incremental(parquet_path: str = None, table_name: str = TABLE_PARQUET):
    """Инкрементальная загрузка Parquet в SQL Server по датам."""
    import pyarrow.parquet as pq

    if parquet_path is None:
        parquet_path = str(Config.CLEAN_PARQUET)

    engine = _get_engine()
    exists = _table_exists(engine, table_name)

    existing_dates = _get_existing_dates(engine, table_name) if exists else set()
    parquet_dates = _get_parquet_dates(parquet_path)
    new_dates = parquet_dates - existing_dates

    if not new_dates:
        print(f"Все даты уже загружены в {table_name}")
        return

    print(f"Новых дат: {len(new_dates)} ({min(new_dates)} - {max(new_dates)})")

    bigint_cols = ["Код КАГ", "ГК (остатки гранд капитала)", "weekday"]
    float_cols = [
        "Пульс (остатки пульса)", "Катрен (остатки катрена)",
        "Протек (остатки протека)", "Фармкомплект (остатки фармкомплекта)",
        "Цена пульса", "Цена катрена", "Цена протека", "Цена фармкомплекта",
    ]
    nvarchar_cols = ["Имя КАГ", "Код товара", "Наименование у нас"]

    pf = pq.ParquetFile(parquet_path)
    loaded_rows = 0
    first_insert = not exists

    for batch in tqdm(pf.iter_batches(batch_size=CHUNK_ROWS), desc=f"Загрузка {table_name}"):
        df = batch.to_pandas()
        if "Дата" in df.columns:
            df["Дата"] = pd.to_datetime(df["Дата"], errors="coerce").dt.floor("s")

        df["_date"] = df["Дата"].dt.date
        df_new = df[df["_date"].isin(new_dates)].drop(columns=["_date"]).copy()
        if df_new.empty:
            continue

        for col in bigint_cols:
            if col in df_new.columns:
                df_new[col] = pd.to_numeric(df_new[col], errors="coerce").astype("Int64")
        for col in float_cols:
            if col in df_new.columns:
                df_new[col] = pd.to_numeric(df_new[col], errors="coerce")
        for col in nvarchar_cols:
            if col in df_new.columns:
                df_new[col] = df_new[col].astype(str).replace("nan", None)

        df_new.replace([float("inf"), float("-inf")], pd.NA, inplace=True)

        df_new.to_sql(
            table_name, engine, schema="dbo",
            if_exists="replace" if first_insert else "append",
            index=False,
        )
        first_insert = False
        loaded_rows += len(df_new)

    print(f"Загружено {loaded_rows:,} строк в {table_name}")


if __name__ == "__main__":
    if not test_connection():
        exit(1)

    xlsx_path = str(Config.OUT_DIR / "final_merged_table.xlsx")
    if os.path.exists(xlsx_path):
        load_xlsx_to_sql(xlsx_path)

    parquet_path = str(Config.CLEAN_PARQUET)
    if os.path.exists(parquet_path):
        load_parquet_to_sql_incremental(parquet_path)
