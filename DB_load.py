import pyodbc
import pandas as pd
import time
import os
import threading
import itertools
import sys
from datetime import date

# 🔐 Подключение
uid = "ii_user"
pwd = "Vjoi970N"
driver = "{ODBC Driver 18 for SQL Server}"
server = "192.168.200.196,59197"
database = "dwh_price"

SCHEMA_NAME = "dbo"
VIEW_NAME = "AI_stock_farm_market_table"

# 📁 Папка для выходного parquet
OUTPUT_DIR = "C:\Проекты\Project_etl_power_bi\data\interim"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# Анимация "запрос выполняется..."
class Spinner:
    def __init__(self, message="⏳ Выполняется SQL-запрос... "):
        self.spinner = itertools.cycle(["|", "/", "-", "\\"])
        self.running = False
        self.thread = None
        self.message = message

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self.animate, daemon=True)
        self.thread.start()

    def animate(self):
        while self.running:
            sys.stdout.write(f"\r{self.message}{next(self.spinner)}")
            sys.stdout.flush()
            time.sleep(0.2)

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()
        sys.stdout.write("\r")  
        sys.stdout.flush()


def export_ai_stock_to_parquet(
    start_date: str,
    end_date: str,
    *,
    output_dir: str = OUTPUT_DIR,
    schema_name: str = SCHEMA_NAME,
    view_name: str = VIEW_NAME,
    maxdop: int = 15,
) -> pd.DataFrame | None:
    """
    Выгружает данные из dbo.AI_stock_farm_market за период [start_date, end_date] (end_date включительно)
    и сохраняет в ОДИН parquet-файл.

    Аргументы:
      start_date: 'YYYY-MM-DD'
      end_date:   'YYYY-MM-DD' (включительно)
      output_dir: папка для сохранения parquet

    Возвращает:
      Путь к сохранённому parquet-файлу.
    """

    # делаем верхнюю границу эксклюзивной: end_date + 1 day
    end_dt_excl = end_date + pd.Timedelta(days=1)
    # чисто для визуала
    start_sql = start_date.strftime("%Y-%m-%d")
    end_sql = end_dt_excl.strftime("%Y-%m-%d")

    print(f"📆 Выгрузка периода: {start_date} → {end_date} (включительно)")
    print(f"   SQL-фильтр: [Дата] >= '{start_sql}' AND [Дата] < '{end_sql}'")
    print()
    
    conn = None
    try:
        #Настройки
        conn = pyodbc.connect(
            f"DRIVER={driver};SERVER={server};DATABASE={database};"
            f"UID={uid};PWD={pwd};TrustServerCertificate=yes;",
            autocommit=True,
        )
        print("✅ Соединение с БД установлено\n")

        #Сам запрос в базу
        #Настрйока максдоп - это максимальное количество паралельных потоков, которое может выполнять функция
        sql = f"""
        SELECT
            *
        FROM [{schema_name}].[{view_name}] WITH (NOLOCK)
        WHERE [Дата] >= '{start_sql}'
          AND [Дата] < '{end_sql}'
          OPTION (MAXDOP {int(maxdop)})
        """
        #ВИзуал
        spinner = Spinner()
        spinner.start()
        t0 = time.time()

        df = pd.read_sql(sql, conn)

        spinner.stop()
        dt = time.time() - t0

        rows = len(df)
        print(f"📥 Получено строк: {rows:,}")
        print(f"⏱ SQL-время: {dt/60:.2f} мин\n")

        if rows == 0:
            print("⚠️ Данных нет. Файл не создаю.")
            return ""

        os.makedirs(output_dir, exist_ok=True)
        out_name = f"ai_stock_{date.today()}.parquet"
        out_path = os.path.join(output_dir, out_name)

        t1 = time.time()
        df.to_parquet(out_path, index=False)
        dt_save = time.time() - t1

        print(f"💾 Сохранено в parquet: {out_path}")
        print(f"⏱ Запись parquet: {dt_save/60:.2f} мин")
        print("✅ ГОТОВО")

        return df

    except Exception as e:
        print("\n❌ Ошибка при выгрузке:")
        print(e)
        raise

    finally:
        if conn:
            conn.close()
            print("\n🔚 Соединение с БД закрыто")

import os
import pandas as pd

def union_all_parquet(
    df_new: pd.DataFrame,
    *,
    parquet_path: str = r"C:\Проекты\Project_etl_power_bi\data\raw\big_data.parquet",
    date_col: str = "Дата",
    kag_col: str = "Код КАГ",
) -> None:
    try:
        df_new = df_new.copy()

        # --- Дата -> datetime64[ns] (дата без времени) ---
        if date_col in df_new.columns:
            df_new[date_col] = pd.to_datetime(df_new[date_col], errors="coerce").dt.normalize()

        # --- Код КАГ -> Int64 (nullable int) ---
        if kag_col in df_new.columns:
            # приводим к строке, чистим, затем в число
            s = df_new[kag_col]
            s = s.astype("string").str.strip()
            # если бывают "20532.0" — можно оставить только целую часть
            s = s.str.replace(r"\.0$", "", regex=True)
            df_new[kag_col] = pd.to_numeric(s, errors="coerce").astype("Int64")

        if os.path.exists(parquet_path):
            df_old = pd.read_parquet(parquet_path).copy()
            print("Размер исходного df =", df_old.shape)

            # нормализация старого df теми же правилами
            if date_col in df_old.columns:
                df_old[date_col] = pd.to_datetime(df_old[date_col], errors="coerce").dt.normalize()

            if kag_col in df_old.columns:
                s = df_old[kag_col].astype("string").str.strip()
                s = s.str.replace(r"\.0$", "", regex=True)
                df_old[kag_col] = pd.to_numeric(s, errors="coerce").astype("Int64")

            df_all = pd.concat([df_old, df_new], ignore_index=True)
        else:
            print("Parquet не найден, создаю новый:", parquet_path)
            df_all = df_new

        # диагностика мусора
        if kag_col in df_all.columns:
            bad_kag = df_all[kag_col].isna().sum()
            if bad_kag:
                print(f"⚠️ Внимание: {bad_kag} строк(и) с некорректным '{kag_col}' → стали <NA>")

        if date_col in df_all.columns:
            bad_dates = df_all[date_col].isna().sum()
            if bad_dates:
                print(f"⚠️ Внимание: {bad_dates} строк(и) с некорректной '{date_col}' → стали NaT")

        df_all.to_parquet(parquet_path, index=False)
        print("Размер итогового df =", df_all.shape)

    except Exception as e:
        print("❌ Произошла ошибка в union_all_parquet:")
        print(e)
        raise

def union_all_clean_parquet(
    df_new: pd.DataFrame,
    *,
    parquet_path: str = r"C:\Проекты\Project_etl_power_bi\data\preproc_parquet\big_data_clean.parquet",
    date_col: str = "Дата",
    kag_col: str = "Код КАГ",
) -> None:
    try:
        df_new = df_new.copy()

        # --- Дата -> datetime64[ns] (дата без времени) ---
        if date_col in df_new.columns:
            df_new[date_col] = pd.to_datetime(df_new[date_col], errors="coerce").dt.normalize()

        # --- Код КАГ -> Int64 (nullable int) ---
        if kag_col in df_new.columns:
            s = df_new[kag_col].astype("string").str.strip()
            s = s.str.replace(r"\.0$", "", regex=True)
            df_new[kag_col] = pd.to_numeric(s, errors="coerce").astype("Int64")

        if os.path.exists(parquet_path):
            df_old = pd.read_parquet(parquet_path).copy()
            print("Размер исходного df =", df_old.shape)

            # нормализация старого df
            if date_col in df_old.columns:
                df_old[date_col] = pd.to_datetime(df_old[date_col], errors="coerce").dt.normalize()

            if kag_col in df_old.columns:
                s = df_old[kag_col].astype("string").str.strip()
                s = s.str.replace(r"\.0$", "", regex=True)
                df_old[kag_col] = pd.to_numeric(s, errors="coerce").astype("Int64")

            # --- ПРОВЕРКА НА ДУБЛИКАТЫ ДАТ ---
            if date_col in df_new.columns and date_col in df_old.columns:
                existing_dates = df_old[date_col].dropna().unique()
                new_dates = df_new[date_col].dropna().unique()
                
                # Даты, которые уже есть
                duplicate_dates = set(new_dates) & set(existing_dates)
                
                if duplicate_dates:
                    print(f"⚠️ Найдено {len(duplicate_dates)} дат(ы), которые уже есть в данных:")
                    print(f"   {sorted(duplicate_dates)[:5]}...")  # показываем первые 5
                    
                    # Фильтруем df_new, оставляя только новые даты
                    df_new = df_new[~df_new[date_col].isin(duplicate_dates)]
                    print(f"✓ После фильтрации осталось {len(df_new)} строк для добавления")
                    
                    if df_new.empty:
                        print("ℹ️ Все данные уже присутствуют, добавление не требуется")
                        return
                else:
                    print(f"✓ Все {len(new_dates)} дат(ы) новые, добавляем")

            df_all = pd.concat([df_old, df_new], ignore_index=True)
        else:
            print("Parquet не найден, создаю новый:", parquet_path)
            df_all = df_new

        # диагностика мусора
        if kag_col in df_all.columns:
            bad_kag = df_all[kag_col].isna().sum()
            if bad_kag:
                print(f"⚠️ Внимание: {bad_kag} строк(и) с некорректным '{kag_col}' → стали <NA>")

        if date_col in df_all.columns:
            bad_dates = df_all[date_col].isna().sum()
            if bad_dates:
                print(f"⚠️ Внимание: {bad_dates} строк(и) с некорректной '{date_col}' → стали NaT")

        df_all.to_parquet(parquet_path, index=False)
        print("Размер итогового df =", df_all.shape)

    except Exception as e:
        print("❌ Произошла ошибка в union_all_parquet:")
        print(e)
        raise



