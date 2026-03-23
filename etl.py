"""
ETL-процесс: загрузка, предобработка и сохранение данных
"""
import sys
import logging
import datetime as dt
from datetime import timedelta

import pandas as pd

from config import Config
from pipeline.check_dates import check_missing, checkup
from pipeline.db_loader import export_ai_stock_to_parquet, union_all_parquet, union_all_clean_parquet
from pipeline.preprocess import (
    base_action,
    collapse_kag_daily_smart,
    zero_small_stocks_conditional,
    drop_weekends_and_holidays,
    fix_competitor_drop_to_zero_anomalies,
)

# Режим работы
USE_MANUAL_DATES = True
MANUAL_START_DATE = '2025-01-01'
MANUAL_END_DATE = '2026-03-19'
FORCE_RELOAD_LAST_DAYS = 0

# Настройка логирования
Config.LOG_DIR.mkdir(parents=True, exist_ok=True)
Config.PREPROC_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Config.LOG_DIR / 'etl_process.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)


def determine_date_range():
    """Определяет диапазон дат для загрузки."""
    today = dt.datetime.now().date()
    logging.info(f"Сегодня: {today}, режим: {'РУЧНОЙ' if USE_MANUAL_DATES else 'АВТОМАТИЧЕСКИЙ'}")

    if USE_MANUAL_DATES:
        min_date = pd.to_datetime(MANUAL_START_DATE)
        max_date = pd.to_datetime(MANUAL_END_DATE)
        logging.info(f"Ручной диапазон: {min_date.date()} - {max_date.date()}")

        if max_date < min_date:
            raise ValueError("Конечная дата меньше начальной")

        try:
            existing_df = checkup(str(Config.INTERIM_DIR))
            if not existing_df.empty and 'Дата' in existing_df.columns:
                existing_min = existing_df['Дата'].min().date()
                existing_max = existing_df['Дата'].max().date()
                if min_date.date() >= existing_min and max_date.date() <= existing_max:
                    logging.info("Данные уже есть в системе")
                    return None, None, True
        except Exception:
            pass

        return min_date, max_date, False

    # Автоматический режим
    try:
        min_missing, max_missing = check_missing()
        if min_missing is None and max_missing is None:
            if FORCE_RELOAD_LAST_DAYS > 0:
                force_start = today - timedelta(days=FORCE_RELOAD_LAST_DAYS)
                return pd.to_datetime(force_start), pd.to_datetime(today - timedelta(days=1)), False
            return None, None, True
        return min_missing, max_missing, False
    except Exception:
        start_year = max(2025, today.year)
        return pd.to_datetime(f'{start_year}-01-01'), pd.to_datetime(today - timedelta(days=1)), False


def main():
    """Основной ETL-процесс."""
    start_time = dt.datetime.now()
    logging.info("=" * 60)
    logging.info("ЗАПУСК ETL ПРОЦЕССА")

    try:
        # Блок 1: Загрузка данных
        min_date, max_date, data_complete = determine_date_range()

        if data_complete:
            logging.info("Загрузка не требуется — используем существующие данные")
            df = checkup(str(Config.INTERIM_DIR))
        else:
            logging.info(f"Загрузка данных: {min_date.date()} - {max_date.date()}")
            df = export_ai_stock_to_parquet(min_date, max_date)
            logging.info(f"Загружено: {len(df):,} строк")
            union_all_parquet(df)

            if not USE_MANUAL_DATES:
                min_check, max_check = check_missing()
                if min_check is not None:
                    logging.warning(f"Остались пропущенные даты: {min_check} - {max_check}")

            df = checkup(str(Config.INTERIM_DIR))

        initial_rows = len(df)
        logging.info(f"Начальное количество строк: {initial_rows:,}")

        # Валидация колонок
        required_cols = Config.get_required_columns()
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Отсутствуют колонки: {missing_cols}")

        # Блок 2: Предобработка
        logging.info("Предобработка данных")

        df = base_action(df)
        logging.info(f"После базовых преобразований: {len(df):,}")

        df = collapse_kag_daily_smart(df, comp_cols=Config.COMP_COLS, price_cols=Config.PRICE_COLS, show_progress=True)
        logging.info(f"После свертки: {len(df):,}")

        df = zero_small_stocks_conditional(df)
        logging.info(f"После очистки малых остатков: {len(df):,}")

        df = drop_weekends_and_holidays(df, verbose=True)
        logging.info(f"После удаления выходных/праздников: {len(df):,}")

        df = fix_competitor_drop_to_zero_anomalies(df)
        final_rows = len(df)
        logging.info(f"После исправления аномалий: {final_rows:,}")

        # Блок 3: Сохранение
        union_all_clean_parquet(df)
        logging.info("Данные сохранены")

        duration = (dt.datetime.now() - start_time).total_seconds()
        logging.info(f"ETL завершён за {duration:.1f} сек. Строк: {initial_rows:,} -> {final_rows:,}")
        return 0

    except Exception as e:
        duration = (dt.datetime.now() - start_time).total_seconds()
        logging.error(f"Ошибка ETL после {duration:.1f} сек: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
