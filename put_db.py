import os
import warnings
import urllib.parse

import pandas as pd
import pyodbc  # noqa
from sqlalchemy import create_engine, text
from tqdm.auto import tqdm

warnings.filterwarnings("ignore")

# ==============================
# ⚙️ НАСТРОЙКИ
# ==============================

server = r"hq-sql06\dwh"
database = "DWH_Analysis_Results"

# Файл 1: Excel (полная перезапись)
XLSX_DIR = r"C:\Проекты\Project_etl_power_bi\data\result"
XLSX_FILE = "final_merged_table.xlsx"
XLSX_PATH = os.path.join(XLSX_DIR, XLSX_FILE)
TABLE_1 = "AI_cut_data_ragged_arrival"

# Файл 2: Parquet (инкрементальная загрузка по датам)
PARQUET_DIR = r"C:\Проекты\Project_etl_power_bi\data\preproc_parquet"
PARQUET_FILE = "big_data_clean.parquet"
PARQUET_PATH = os.path.join(PARQUET_DIR, PARQUET_FILE)
TABLE_2 = "AI_full_data_ragged_arrival"

CHUNK_ROWS = 100_000

# ==============================
# 🔌 ПОДКЛЮЧЕНИЕ
# ==============================

odbc_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database};"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
)

conn_str = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_str)
engine = create_engine(conn_str, fast_executemany=True)

# ==============================
# ПРОВЕРКА ПОДКЛЮЧЕНИЯ
# ==============================

def test_connection():
    try:
        with engine.connect() as conn:
            _ = pd.read_sql("SELECT 1 AS ok", conn)
        print("✅ Подключение OK")
        return True
    except Exception as e:
        print("❌ Ошибка подключения:", e)
        return False


# ==============================
# ЗАГРУЗКА XLSX → SQL (ПОЛНАЯ ПЕРЕЗАПИСЬ)
# ==============================

def load_xlsx_to_sql(xlsx_path: str, table_name: str):
    print(f"\n📊 Загрузка Excel: {xlsx_path}")
    print(f"   Режим: ПОЛНАЯ ПЕРЕЗАПИСЬ")
    
    df = pd.read_excel(xlsx_path)
    print(f"   Строк в файле: {len(df)}")
    
    # Проверка существования и очистка таблицы
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'
        """))
        count = result.scalar()
        table_exists = count > 0
        print(f"   🔎 Проверка таблицы: найдено {count} совпадений")
        
        if table_exists:
            conn.execute(text(f"DELETE FROM [dbo].[{table_name}]"))
            conn.commit()
            print(f"   🗑️ Таблица {table_name} очищена")
        else:
            print(f"   📝 Таблица {table_name} будет создана")
    
    # BIGINT колонки
    bigint_cols = [
        "Код КАГ",
        "Длительность дефектуры, дней",
        "Количество приходов после дефектуры у конкурентов (всего)",
        "Кол-во конкурентов с приходами",
        "Общий объём прихода после дефектуры",
        "Объём прихода Пульс (сумма)",
        "Объём прихода Катрен (сумма)",
        "Объём прихода Протек (сумма)",
        "Объём прихода Фармкомплект (сумма)",
        "Остаток Пульс (вчера)",
        "Остаток Катрен (вчера)",
        "Остаток Протек (вчера)",
        "Остаток Фармкомплект (вчера)",
        "Остаток ФК Гранд Капитал (последняя дата)"
    ]
    
    # FLOAT колонки
    float_cols = ["Рейтинг Внешний"]
    
    # NVARCHAR(255) колонки
    nvarchar_255_cols = [
        "Имя КАГ",
        "Статус",
        "Дата входа в дефектуру ФК Гранд Капитал",
        "Дата окончания дефектуры ФК Гранд Капитал",
        "Конкуренты с приходами"
    ]
    
    # NVARCHAR(MAX) колонки
    nvarchar_long_cols = [
        "Приходы Пульс (дата-объём)",
        "Приходы Катрен (дата-объём)",
        "Приходы Протек (дата-объём)",
        "Приходы Фармкомплект (дата-объём)"
    ]
    
    # Преобразование типов
    for col in bigint_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    for col in nvarchar_255_cols + nvarchar_long_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("nan", None)
    
    # Защита от inf
    df.replace([float("inf"), float("-inf")], pd.NA, inplace=True)
    
    df.to_sql(
        table_name,
        engine,
        schema="dbo",
        if_exists="append" if table_exists else "replace",
        index=False
    )
    
    print(f"✅ Готово! Загружено {len(df)} строк в {table_name}")


# ==============================
# ПОЛУЧЕНИЕ СУЩЕСТВУЮЩИХ ДАТ ИЗ БД
# ==============================

def get_existing_dates(table_name: str) -> set:
    query = f"SELECT DISTINCT CAST([Дата] AS DATE) as dt FROM [dbo].[{table_name}]"
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        dates = set(pd.to_datetime(df["dt"]).dt.date)
        return dates
    except Exception as e:
        print(f"   ⚠️ Не удалось получить даты из БД: {e}")
        return set()


# ==============================
# ПОЛУЧЕНИЕ ДАТ ИЗ PARQUET
# ==============================

def get_parquet_dates(parquet_path: str) -> set:
    import pyarrow.parquet as pq
    
    pf = pq.ParquetFile(parquet_path)
    dates = set()
    
    for batch in pf.iter_batches(batch_size=500_000, columns=["Дата"]):
        df = batch.to_pandas()
        df["Дата"] = pd.to_datetime(df["Дата"], errors="coerce")
        dates.update(df["Дата"].dt.date.dropna().unique())
    
    return dates


# ==============================
# ЗАГРУЗКА PARQUET → SQL (ИНКРЕМЕНТАЛЬНАЯ ПО ДАТАМ)
# ==============================

def load_parquet_to_sql_incremental(parquet_path: str, table_name: str, chunk_rows: int = 100_000):
    import pyarrow.parquet as pq
    
    print(f"\n📦 Загрузка Parquet: {parquet_path}")
    print(f"   Режим: ИНКРЕМЕНТАЛЬНАЯ (по датам)")
    
    # Проверяем существование таблицы
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'
        """))
        count = result.scalar()
        table_exists = count > 0
        print(f"   🔎 Проверка таблицы: найдено {count} совпадений")
    
    # Получаем существующие даты из БД
    print("   🔍 Проверка существующих дат в БД...")
    if table_exists:
        existing_dates = get_existing_dates(table_name)
    else:
        existing_dates = set()
        print(f"   📝 Таблица {table_name} будет создана")
    print(f"   📅 Дат в БД: {len(existing_dates)}")
    
    # Получаем даты из parquet
    print("   🔍 Сканирование дат в Parquet...")
    parquet_dates = get_parquet_dates(parquet_path)
    print(f"   📅 Дат в Parquet: {len(parquet_dates)}")
    
    # Определяем новые даты
    new_dates = parquet_dates - existing_dates
    
    if not new_dates:
        print("   ✅ Все даты уже загружены. Нечего добавлять.")
        return
    
    print(f"   🆕 Новых дат для загрузки: {len(new_dates)}")
    print(f"   📆 Диапазон: {min(new_dates)} — {max(new_dates)}")
    
    # BIGINT колонки
    bigint_cols = [
        "Код КАГ",
        "ГК (остатки гранд капитала)",
        "weekday"
    ]
    
    # FLOAT колонки
    float_cols = [
        "Пульс (остатки пульса)",
        "Катрен (остатки катрена)",
        "Протек (остатки протека)",
        "Фармкомплект (остатки фармкомплекта)",
        "Цена пульса",
        "Цена катрена",
        "Цена протека",
        "Цена фармкомплекта"
    ]
    
    # NVARCHAR колонки
    nvarchar_cols = [
        "Имя КАГ",
        "Код товара",
        "Наименование у нас"
    ]
    
    pf = pq.ParquetFile(parquet_path)
    total_rows = pf.metadata.num_rows
    
    loaded_rows = 0
    skipped_rows = 0
    first_insert = not table_exists  # Первая вставка создаст таблицу
    
    pbar = tqdm(total=total_rows, desc=f"Обработка {table_name}", unit="строк")
    
    for batch in pf.iter_batches(batch_size=chunk_rows):
        df = batch.to_pandas()
        
        # DATETIME2(0)
        if "Дата" in df.columns:
            df["Дата"] = pd.to_datetime(df["Дата"], errors="coerce").dt.floor("s")
        
        # Фильтруем только новые даты
        df["_date"] = df["Дата"].dt.date
        df_new = df[df["_date"].isin(new_dates)].drop(columns=["_date"]).copy()
        df = df.drop(columns=["_date"])
        
        skipped_rows += len(df) - len(df_new)
        
        if len(df_new) == 0:
            pbar.update(len(df))
            continue
        
        # BIGINT
        for col in bigint_cols:
            if col in df_new.columns:
                df_new[col] = pd.to_numeric(df_new[col], errors="coerce").astype("Int64")
        
        # FLOAT
        for col in float_cols:
            if col in df_new.columns:
                df_new[col] = pd.to_numeric(df_new[col], errors="coerce")
        
        # NVARCHAR
        for col in nvarchar_cols:
            if col in df_new.columns:
                df_new[col] = df_new[col].astype(str).replace("nan", None)
        
        # Защита от inf
        df_new.replace([float("inf"), float("-inf")], pd.NA, inplace=True)
        
        df_new.to_sql(
            table_name,
            engine,
            schema="dbo",
            if_exists="replace" if first_insert else "append",
            index=False
        )
        first_insert = False  # После первой вставки используем append
        
        loaded_rows += len(df_new)
        pbar.update(len(df))
    
    pbar.close()
    print(f"✅ Готово!")
    print(f"   📥 Загружено строк: {loaded_rows:,}")
    print(f"   ⏭️ Пропущено строк (уже есть): {skipped_rows:,}")


# ==============================
# MAIN
# ==============================

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 ЗАГРУЗКА ДАННЫХ В SQL SERVER")
    print("=" * 60)
    
    if not test_connection():
        exit(1)
    
    # Загрузка файла 1: Excel → AI_cut_data_ragged_arrival (ПОЛНАЯ ПЕРЕЗАПИСЬ)
    if os.path.exists(XLSX_PATH):
        load_xlsx_to_sql(XLSX_PATH, TABLE_1)
    else:
        print(f"⚠️ Файл не найден: {XLSX_PATH}")
    
    # Загрузка файла 2: Parquet → AI_full_data_ragged_arrival (ИНКРЕМЕНТАЛЬНАЯ)
    if os.path.exists(PARQUET_PATH):
        load_parquet_to_sql_incremental(PARQUET_PATH, TABLE_2, CHUNK_ROWS)
    else:
        print(f"⚠️ Файл не найден: {PARQUET_PATH}")
    
    print("\n" + "=" * 60)
    print("🎉 ЗАГРУЗКА ЗАВЕРШЕНА")
    print("=" * 60)