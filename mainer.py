from check_dates import check_missing, checkup
from DB_load import export_ai_stock_to_parquet, union_all_parquet, union_all_clean_parquet
from preprocess import (
    base_action, 
    collapse_kag_daily_smart, 
    zero_small_stocks_conditional, 
    drop_weekends_and_holidays, 
    drop_inactive_by_last_months_stock, 
    fix_competitor_drop_to_zero_anomalies
)
import pandas as pd
import logging
import sys
import datetime as dt
from datetime import timedelta
import os

# =============================================================================
# ⚙️  НАСТРОЙКИ ЗАГРУЗКИ ДАННЫХ - ИЗМЕНИТЕ ЗДЕСЬ!
# =============================================================================

# 🎯 ВЫБЕРИТЕ РЕЖИМ РАБОТЫ:
USE_MANUAL_DATES = True  # True = вручную задаете даты | False = автоматически

# 📅 РУЧНОЙ ДИАПАЗОН ДАТ (используется если USE_MANUAL_DATES = True):
MANUAL_START_DATE = '2025-01-01'  # ← ИЗМЕНИТЕ НА НУЖНУЮ ДАТУ
MANUAL_END_DATE = '2026-03-17'    # ← ИЗМЕНИТЕ НА НУЖНУЮ ДАТУ

# 🔄 РЕЖИМ ОБНОВЛЕНИЯ (используется если USE_MANUAL_DATES = False):
FORCE_RELOAD_LAST_DAYS = 0  # Сколько последних дней пересоберить принудительно (0 = только пропущенные)

# =============================================================================

# Настройка путей
PATH_TO_PARQUET_DIR = r'C:\Проекты\Project_etl_power_bi\data\interim'
PATH_TO_LOGS = r'C:\Проекты\Project_etl_power_bi\logs'
PATH_TO_OUTPUT = r'C:\Проекты\Project_etl_power_bi\data\preproc_parquet'

# Создаем директории если их нет
os.makedirs(PATH_TO_LOGS, exist_ok=True)
os.makedirs(PATH_TO_OUTPUT, exist_ok=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(PATH_TO_LOGS, 'etl_process.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Конфигурация колонок
comp_cols = [
    "Пульс (остатки пульса)",
    "Катрен (остатки катрена)",
    "Протек (остатки протека)",
    "Фармкомплект (остатки фармкомплекта)",
]

price_cols = [
    "Цена пульса",
    "Цена катрена",
    "Цена протека",
    "Цена фармкомплекта",
]


def analyze_removed_dates(df_before, df_after, date_col='Дата'):
    """Анализирует какие даты были удалены"""
    
    dates_before = set(df_before[date_col].dt.date.unique())
    dates_after = set(df_after[date_col].dt.date.unique())
    removed_dates = dates_before - dates_after
    
    if not removed_dates:
        logging.info("   ⚠️ ВНИМАНИЕ: Ни одна дата не была удалена!")
        return
    
    # Сортируем удаленные даты
    removed_sorted = sorted(list(removed_dates))
    
    # Определяем типы удаленных дат
    weekends = []
    holidays_2025 = []
    holidays_2026 = []
    
    # Список праздников для проверки
    russian_holidays = {
        # 2025
        '2024-12-30', '2024-12-31', '2025-01-01', '2025-01-02', '2025-01-03', 
        '2025-01-04', '2025-01-05', '2025-01-06', '2025-01-07', '2025-01-08', '2025-01-09',
        '2025-02-22', '2025-02-23', '2025-02-24', '2025-03-07', '2025-03-08', '2025-03-10',
        '2025-04-30', '2025-05-01', '2025-05-02', '2025-05-08', '2025-05-09',
        '2025-06-11', '2025-06-12', '2025-06-13', '2025-11-03', '2025-11-04',
        # 2026
        '2025-12-30', '2025-12-31', '2026-01-01', '2026-01-02', '2026-01-03',
        '2026-01-04', '2026-01-05', '2026-01-06', '2026-01-07', '2026-01-08', '2026-01-09',
        '2026-02-21', '2026-02-23', '2026-03-07', '2026-03-09', '2026-04-30', 
        '2026-05-01', '2026-05-04', '2026-05-08', '2026-05-11', '2026-06-11', 
        '2026-06-12', '2026-11-03', '2026-11-04'
    }
    
    for date in removed_sorted:
        weekday = date.weekday()  # 0=понедельник, 6=воскресенье
        date_str = date.strftime('%Y-%m-%d')
        
        if weekday in [5, 6]:  # Суббота или воскресенье
            weekends.append(date_str)
        elif date_str in russian_holidays:
            if date.year == 2025:
                holidays_2025.append(date_str)
            else:
                holidays_2026.append(date_str)
    
    # Логирование результатов
    logging.info(f"   📅 Всего удалено дат: {len(removed_dates)}")
    
    if weekends:
        logging.info(f"   🛌 Выходные дни ({len(weekends)}): {', '.join(weekends[:10])}")
        if len(weekends) > 10:
            logging.info(f"       ... и еще {len(weekends) - 10} выходных")
    
    if holidays_2025:
        logging.info(f"   🎉 Праздники 2025 ({len(holidays_2025)}): {', '.join(holidays_2025)}")
    
    if holidays_2026:
        logging.info(f"   🎉 Праздники 2026 ({len(holidays_2026)}): {', '.join(holidays_2026)}")
    
    # Проверка на неожиданные даты
    total_categorized = len(weekends) + len(holidays_2025) + len(holidays_2026)
    if total_categorized != len(removed_dates):
        uncategorized = len(removed_dates) - total_categorized
        logging.warning(f"   ⚠️ Неопознанных дат: {uncategorized}")


def determine_date_range():
    """Определяет диапазон дат для загрузки на основе настроек"""
    
    today = dt.datetime.now().date()
    logging.info(f"🗓️ Сегодня: {today}")
    logging.info(f"⚙️ Режим: {'РУЧНОЙ' if USE_MANUAL_DATES else 'АВТОМАТИЧЕСКИЙ'}")
    
    if USE_MANUAL_DATES:
        # ===========================================
        # 🎯 РУЧНОЙ РЕЖИМ - ИСПОЛЬЗУЕМ ЗАДАННЫЕ ДАТЫ
        # ===========================================
        try:
            min_date = pd.to_datetime(MANUAL_START_DATE)
            max_date = pd.to_datetime(MANUAL_END_DATE)
            
            logging.info(f"📅 РУЧНОЙ ДИАПАЗОН: {min_date.date()} - {max_date.date()}")
            
            # Валидация
            if max_date < min_date:
                logging.error("❌ ОШИБКА: Конечная дата меньше начальной!")
                raise ValueError("Конечная дата меньше начальной")
            
            if max_date.date() > today:
                logging.warning(f"⚠️ ВНИМАНИЕ: Конечная дата {max_date.date()} в будущем (сегодня {today})")
            
            # Проверяем, есть ли уже эти данные
            try:
                existing_df = checkup(PATH_TO_PARQUET_DIR)
                if not existing_df.empty and 'Дата' in existing_df.columns:
                    existing_min = existing_df['Дата'].min().date()
                    existing_max = existing_df['Дата'].max().date()
                    logging.info(f"📊 Существующие данные: {existing_min} - {existing_max}")
                    
                    if min_date.date() >= existing_min and max_date.date() <= existing_max:
                        logging.info("✅ Запрашиваемые данные уже есть в системе")
                        return None, None, True
                    else:
                        logging.info("🔄 Требуется загрузка новых данных")
            except Exception as e:
                logging.info(f"📂 Существующих данных нет: {e}")
            
            return min_date, max_date, False
            
        except Exception as e:
            logging.error(f"❌ ОШИБКА в ручных датах: {e}")
            logging.error(f"💡 Проверьте формат дат в настройках:")
            logging.error(f"   MANUAL_START_DATE = '{MANUAL_START_DATE}'")
            logging.error(f"   MANUAL_END_DATE = '{MANUAL_END_DATE}'")
            raise
    
    else:
        # ===========================================
        # 🤖 АВТОМАТИЧЕСКИЙ РЕЖИМ
        # ===========================================
        try:
            # Пытаемся определить отсутствующие даты
            min_missing, max_missing = check_missing()
            
            if min_missing is None and max_missing is None:
                
                # Проверяем принудительное обновление
                if FORCE_RELOAD_LAST_DAYS > 0:
                    force_start = today - timedelta(days=FORCE_RELOAD_LAST_DAYS)
                    force_end = today - timedelta(days=1)  # До вчера
                    
                    logging.info(f"🔄 Принудительное обновление последних {FORCE_RELOAD_LAST_DAYS} дней")
                    logging.info(f"📅 Диапазон принудительного обновления: {force_start} - {force_end}")
                    
                    return pd.to_datetime(force_start), pd.to_datetime(force_end), False
                else:
                    # Все данные есть
                    logging.info("✅ Все данные присутствуют в системе")
                    return None, None, True
            else:
                # Есть пропущенные даты
                logging.info(f"❌ Найдены пропущенные даты: {min_missing} - {max_missing}")
                return min_missing, max_missing, False
                
        except Exception as e:
            logging.warning(f"⚠️ Ошибка в check_missing(): {e}")
            
            # Фолбэк: загружаем данные с начала текущего года
            start_year = max(2025, today.year)
            fallback_min = pd.to_datetime(f'{start_year}-01-01')
            fallback_max = pd.to_datetime(today - timedelta(days=1))
            
            logging.info(f"🔄 Фолбэк: {fallback_min.date()} - {fallback_max.date()}")
            return fallback_min, fallback_max, False


def main():
    """Основной ETL процесс обработки данных"""
    start_time = dt.datetime.now()
    logging.info("=" * 80)
    logging.info("ЗАПУСК ETL ПРОЦЕССА")
    logging.info("=" * 80)
    
    # Показываем текущие настройки
    logging.info("⚙️ ТЕКУЩИЕ НАСТРОЙКИ:")
    if USE_MANUAL_DATES:
        logging.info(f"   Режим: РУЧНОЙ")
        logging.info(f"   Диапазон: {MANUAL_START_DATE} - {MANUAL_END_DATE}")
    else:
        logging.info(f"   Режим: АВТОМАТИЧЕСКИЙ")
        logging.info(f"   Принудительное обновление: {FORCE_RELOAD_LAST_DAYS} дней")
    logging.info("-" * 80)
    
    try:
        # ======================== БЛОК 1: ЗАГРУЗКА ДАННЫХ ========================
        logging.info("БЛОК 1: Проверка и загрузка данных")
        logging.info("-" * 80)
        
        # Определяем диапазон дат
        min_date, max_date, data_complete = determine_date_range()
        
        # Загружаем или обновляем данные
        if data_complete:
            logging.info("✅ Загрузка не требуется - используем существующие данные")
            df = checkup(PATH_TO_PARQUET_DIR)
            logging.info(f"📊 Загружено строк из существующего датасета: {len(df):,}")
            if 'Дата' in df.columns:
                logging.info(f"📅 Максимальная дата в данных: {df['Дата'].max()}")
        else:
            logging.info(f"🔄 Загрузка данных: {min_date.date()} - {max_date.date()}")
            logging.info("📥 Загрузка из БД...")
            
            try:
                df = export_ai_stock_to_parquet(min_date, max_date)
                logging.info(f"✅ Загружено новых строк: {len(df):,}")
                
                logging.info("🔗 Объединение с существующими данными...")
                union_all_parquet(df)
                
                # Повторная проверка (только в автоматическом режиме)
                if not USE_MANUAL_DATES:
                    logging.info("🔍 Повторная проверка отсутствующих дат")
                    min_date_check, max_date_check = check_missing()
                    if min_date_check is None and max_date_check is None:
                        logging.info("✅ После загрузки все даты присутствуют")
                    else:
                        logging.warning(f"⚠️ Еще остались пропущенные даты: {min_date_check} - {max_date_check}")
                
                # Загружаем полный датасет
                df = checkup(PATH_TO_PARQUET_DIR)
                logging.info(f"📊 Итого строк в полном датасете: {len(df):,}")
                
            except Exception as e:
                logging.error(f"❌ Ошибка при загрузке данных: {e}")
                # Пытаемся загрузить существующие данные
                try:
                    df = checkup(PATH_TO_PARQUET_DIR)
                    logging.info(f"🔄 Используем существующие данные: {len(df):,} строк")
                except Exception as e2:
                    logging.error(f"❌ Не удалось загрузить даже существующие данные: {e2}")
                    raise
        
        initial_rows = len(df)
        logging.info(f"🎯 Начальное количество строк: {initial_rows:,}")
        
        # ======================== ВАЛИДАЦИЯ ДАННЫХ ========================
        logging.info("\n📋 ВАЛИДАЦИЯ ЗАГРУЖЕННЫХ ДАННЫХ")
        logging.info("-" * 80)
        
        # Проверяем основные колонки
        required_cols = ['Дата', 'Код КАГ', 'ГК (остатки гранд капитала)'] + comp_cols
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            logging.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: Отсутствуют обязательные колонки: {missing_cols}")
            raise ValueError(f"Отсутствуют колонки: {missing_cols}")
        else:
            logging.info("✅ Все обязательные колонки присутствуют")
        
        # Проверяем диапазон дат
        if 'Дата' in df.columns and not df.empty:
            df_min_date = df['Дата'].min()
            df_max_date = df['Дата'].max()
            # Безопасное извлечение даты (может быть уже date или datetime)
            min_date_str = df_min_date.date() if hasattr(df_min_date, 'date') else df_min_date
            max_date_str = df_max_date.date() if hasattr(df_max_date, 'date') else df_max_date
            logging.info(f"📅 Диапазон дат в данных: {min_date_str} - {max_date_str}")
        
        # ======================== БЛОК 2: ПРЕДОБРАБОТКА ========================
        logging.info("\nБЛОК 2: Предобработка данных")
        logging.info("-" * 80)
        
        # Шаг 1: Базовые преобразования
        logging.info("📝 Шаг 1/5: Применение базовых преобразований")
        try:
            df = base_action(df)
            logging.info(f"✅ После базовых преобразований: {len(df):,} строк")
        except Exception as e:
            logging.error(f"❌ Ошибка в base_action: {e}")
            raise
        
        # Шаг 2: Свертка данных по конкурентам
        logging.info("🔄 Шаг 2/5: Свертка данных по конкурентам и ценам")
        try:
            df = collapse_kag_daily_smart(
                df, 
                comp_cols=comp_cols, 
                price_cols=price_cols, 
                show_progress=True
            )
            logging.info(f"✅ После свертки: {len(df):,} строк")
        except Exception as e:
            logging.error(f"❌ Ошибка в collapse_kag_daily_smart: {e}")
            raise
        
        # Шаг 3: Очистка малых остатков
        logging.info("🧹 Шаг 3/5: Очистка малых остатков (threshold=10, max_gt=100)")
        try:
            df = zero_small_stocks_conditional(df)
            logging.info(f"✅ После очистки малых остатков: {len(df):,} строк")
        except Exception as e:
            logging.error(f"❌ Ошибка в zero_small_stocks_conditional: {e}")
            raise
        
        # Шаг 4: Удаление выходных и праздников (С ДЕТАЛЬНЫМ ЛОГИРОВАНИЕМ)
        logging.info("🗓️ Шаг 4/5: Удаление выходных дней и праздников")
        
        try:
            # Сохраняем состояние ДО удаления для анализа
            df_before_holidays = df.copy()
            rows_before = len(df)
            unique_dates_before = df['Дата'].nunique()
            min_date_before = df['Дата'].min()
            max_date_before = df['Дата'].max()
            
            # Безопасное извлечение даты
            min_before_str = min_date_before.date() if hasattr(min_date_before, 'date') else min_date_before
            max_before_str = max_date_before.date() if hasattr(max_date_before, 'date') else max_date_before
            
            logging.info(f"   📊 До удаления: {rows_before:,} строк, {unique_dates_before} уникальных дат")
            logging.info(f"   📅 Диапазон дат: {min_before_str} - {max_before_str}")
            
            # Выполняем удаление
            df = drop_weekends_and_holidays(df, verbose=True)
            
            # Анализируем результат
            rows_after = len(df)
            unique_dates_after = df['Дата'].nunique()
            
            logging.info(f"   📊 После удаления: {rows_after:,} строк, {unique_dates_after} уникальных дат")
            logging.info(f"   ➖ Удалено строк: {rows_before - rows_after:,}")
            logging.info(f"   ➖ Удалено дат: {unique_dates_before - unique_dates_after}")
            
            # ДЕТАЛЬНЫЙ АНАЛИЗ УДАЛЕННЫХ ДАТ
            logging.info("   🔍 Детальный анализ удаленных дат:")
            analyze_removed_dates(df_before_holidays, df, 'Дата')
            
            # Проверяем наличие выходных в оставшихся данных
            if len(df) > 0:
                remaining_weekends = df[df['Дата'].dt.weekday.isin([5, 6])]
                if len(remaining_weekends) > 0:
                    logging.warning(f"   ⚠️ ВНИМАНИЕ: Остались выходные дни ({len(remaining_weekends)} строк)!")
                    unique_weekend_dates = remaining_weekends['Дата'].dt.date.unique()
                    logging.warning(f"   📅 Даты выходных: {', '.join(map(str, unique_weekend_dates[:5]))}")
                else:
                    logging.info("   ✅ Выходные дни полностью удалены")
            
            logging.info("   " + "=" * 50)
            
        except Exception as e:
            logging.error(f"❌ Ошибка в drop_weekends_and_holidays: {e}")
            raise
        
        # Шаг 5: Исправление аномалий конкурентов
        logging.info("🔧 Шаг 5/5: Исправление аномалий падения до нуля у конкурентов")
        try:
            df = fix_competitor_drop_to_zero_anomalies(df)
            final_rows = len(df)
            logging.info(f"✅ После исправления аномалий: {final_rows:,} строк")
        except Exception as e:
            logging.error(f"❌ Ошибка в fix_competitor_drop_to_zero_anomalies: {e}")
            raise
        
        # ======================== БЛОК 3: СОХРАНЕНИЕ ========================
        logging.info("\nБЛОК 3: Сохранение результатов")
        logging.info("-" * 80)
        
        try:
            logging.info("💾 Сохранение очищенных данных...")
            union_all_clean_parquet(df)
            logging.info("✅ Данные успешно сохранены")
        except Exception as e:
            logging.error(f"❌ Ошибка при сохранении: {e}")
            raise
        
        # ======================== ИТОГОВАЯ СТАТИСТИКА ========================
        end_time = dt.datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logging.info("\n" + "=" * 80)
        logging.info("📊 ИТОГОВАЯ СТАТИСТИКА")
        logging.info("=" * 80)
        logging.info(f"⏰ Время начала:           {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"⏰ Время окончания:        {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"⏱️ Время выполнения:       {duration:.2f} сек ({duration/60:.2f} мин)")
        logging.info(f"📈 Начальное кол-во строк: {initial_rows:,}")
        logging.info(f"📉 Конечное кол-во строк:  {final_rows:,}")
        logging.info(f"➖ Удалено строк:          {initial_rows - final_rows:,} ({((initial_rows - final_rows) / initial_rows * 100):.2f}%)")
        
        if len(df) > 0 and 'Дата' in df.columns:
            min_date_final = df['Дата'].min()
            max_date_final = df['Дата'].max()
            # Безопасное извлечение даты
            min_final_str = min_date_final.date() if hasattr(min_date_final, 'date') else min_date_final
            max_final_str = max_date_final.date() if hasattr(max_date_final, 'date') else max_date_final
            logging.info(f"📅 Диапазон дат:           {min_final_str} - {max_final_str}")
            logging.info(f"📆 Уникальных дат:         {df['Дата'].nunique():,}")
        
        # ФИНАЛЬНАЯ ПРОВЕРКА НА ВЫХОДНЫЕ/ПРАЗДНИКИ
        logging.info("\n📋 ФИНАЛЬНАЯ ПРОВЕРКА:")
        if len(df) > 0:
            final_weekends = df[df['Дата'].dt.weekday.isin([5, 6])]
            if len(final_weekends) == 0:
                logging.info("✅ Выходные дни: полностью удалены")
            else:
                logging.error(f"❌ Выходные дни: остались {len(final_weekends)} строк!")
            
            # Проверяем некоторые известные праздники
            test_holidays = ['2025-01-01', '2025-01-02', '2025-01-03', '2025-05-01', '2025-05-09', '2026-01-01']
            remaining_holidays = []
            for holiday in test_holidays:
                try:
                    if pd.to_datetime(holiday) in df['Дата'].values:
                        remaining_holidays.append(holiday)
                except:
                    pass
            
            if not remaining_holidays:
                logging.info("✅ Праздничные дни: успешно удалены (проверены основные)")
            else:
                logging.error(f"❌ Праздничные дни: остались {remaining_holidays}")
        
        # ======================== ДОПОЛНИТЕЛЬНЫЕ ПРОВЕРКИ ========================
        logging.info("\n📊 ДОПОЛНИТЕЛЬНЫЕ ПРОВЕРКИ:")
        logging.info("-" * 80)
        
        if len(df) > 0:
            # 1. Проверка остатков ГК
            gk_col = "ГК (остатки гранд капитала)"
            if gk_col in df.columns:
                try:
                    gk_data = pd.to_numeric(df[gk_col], errors='coerce').fillna(0)
                    
                    total_records = len(gk_data)
                    zero_gk = (gk_data == 0).sum()
                    positive_gk = (gk_data > 0).sum()
                    negative_gk = (gk_data < 0).sum()
                    
                    zero_pct = (zero_gk / total_records) * 100 if total_records > 0 else 0
                    positive_pct = (positive_gk / total_records) * 100 if total_records > 0 else 0
                    
                    logging.info(f"🏭 АНАЛИЗ ОСТАТКОВ ГК:")
                    logging.info(f"   📦 Всего записей: {total_records:,}")
                    logging.info(f"   🔴 ГК = 0: {zero_gk:,} ({zero_pct:.1f}%)")
                    logging.info(f"   🟢 ГК > 0: {positive_gk:,} ({positive_pct:.1f}%)")
                    
                    if negative_gk > 0:
                        logging.warning(f"   ⚠️ ГК < 0: {negative_gk:,} записей (возможная ошибка данных)")
                    
                    if positive_gk > 0:
                        positive_data = gk_data[gk_data > 0]
                        logging.info(f"   📈 Медиана ГК>0: {positive_data.median():.0f}")
                        logging.info(f"   📈 Максимум ГК: {positive_data.max():.0f}")
                    
                    # Выводы
                    if zero_pct > 50:
                        logging.info(f"   💡 Больше половины записей с ГК=0 (дефектура)")
                    elif zero_pct < 10:
                        logging.info(f"   💡 Мало записей с ГК=0 (хорошая доступность)")
                    else:
                        logging.info(f"   💡 Умеренное количество записей с ГК=0")
                
                except Exception as e:
                    logging.error(f"   ❌ Ошибка анализа ГК: {e}")
            else:
                logging.warning(f"   ⚠️ Колонка '{gk_col}' не найдена")
            
            # 2. График динамики конкурентов
            logging.info(f"\n📈 ГРАФИК ДИНАМИКИ КОНКУРЕНТОВ:")
            try:
                # Создаем график
                import matplotlib.pyplot as plt
                import matplotlib.dates as mdates
                
                # Сортируем данные по дате
                df_sorted = df.sort_values('Дата').copy()
                
                # Берем последние 20 дат
                unique_dates = df_sorted['Дата'].dt.normalize().unique()
                last_20_dates = sorted(unique_dates)[-20:]
                
                if len(last_20_dates) < 5:
                    logging.warning(f"   ⚠️ Недостаточно дат для графика ({len(last_20_dates)} дат)")
                else:
                    # Фильтруем данные по последним 20 датам
                    df_chart = df_sorted[df_sorted['Дата'].dt.normalize().isin(last_20_dates)].copy()
                    
                    if len(df_chart) > 0:
                        # Агрегируем по дате (сумма остатков по всем КАГ)
                        daily_totals = df_chart.groupby(df_chart['Дата'].dt.normalize())[comp_cols].sum()
                        
                        # Создаем график
                        plt.figure(figsize=(14, 8))
                        
                        # Цвета для конкурентов
                        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']
                        
                        for i, col in enumerate(comp_cols):
                            if col in daily_totals.columns:
                                # Красивое название конкурента
                                pretty_name = col.replace('(остатки ', '').replace(')', '').replace('Фармкомплект', 'ФармК')
                                
                                plt.plot(
                                    daily_totals.index, 
                                    daily_totals[col], 
                                    marker='o', 
                                    linewidth=2, 
                                    markersize=4,
                                    color=colors[i % len(colors)],
                                    label=pretty_name
                                )
                        
                        # Настройки графика
                        plt.title('Динамика остатков конкурентов за последние 20 дат', 
                                 fontsize=16, fontweight='bold', pad=20)
                        plt.xlabel('Дата', fontsize=12)
                        plt.ylabel('Общие остатки (шт)', fontsize=12)
                        
                        # Форматирование оси X
                        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
                        plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(last_20_dates)//8)))
                        
                        # Поворот меток дат
                        plt.xticks(rotation=45)
                        
                        # Легенда и сетка
                        plt.legend(loc='upper right', frameon=True, fancybox=True, shadow=True)
                        plt.grid(True, alpha=0.3, linestyle='--')
                        
                        # Настройки макета
                        plt.tight_layout()
                        
                        # Сохранение графика
                        chart_path = os.path.join(PATH_TO_OUTPUT, 'competitors_dynamics_last_20_days.png')
                        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
                        plt.close()  # Закрываем фигуру чтобы освободить память
                        
                        logging.info(f"   ✅ График сохранен: {chart_path}")
                        logging.info(f"   📅 Период: {last_20_dates[0].date()} - {last_20_dates[-1].date()}")
                        logging.info(f"   📊 Дат на графике: {len(last_20_dates)}")
                        
                        # Краткая статистика по последним данным
                        latest_data = daily_totals.iloc[-1]
                        logging.info(f"   📈 Остатки на последнюю дату:")
                        for col in comp_cols:
                            if col in latest_data.index:
                                pretty_name = col.replace('(остатки ', '').replace(')', '').replace('Фармкомплект', 'ФармК')
                                value = latest_data[col]
                                logging.info(f"      • {pretty_name}: {value:,.0f} шт")
                    
                    else:
                        logging.warning(f"   ⚠️ Нет данных для построения графика")
            
            except ImportError:
                logging.warning(f"   ⚠️ Matplotlib не установлен, график не создан")
                logging.info(f"      Для установки: pip install matplotlib")
            
            except Exception as e:
                logging.error(f"   ❌ Ошибка создания графика: {e}")
        
        logging.info("=" * 80)
        
        # Сохраняем метаданные о выполнении
        metadata = {
            'timestamp': end_time,
            'start_time': start_time,
            'duration_sec': duration,
            'initial_rows': initial_rows,
            'final_rows': final_rows,
            'rows_removed': initial_rows - final_rows,
            'min_date': df['Дата'].min() if len(df) > 0 else None,
            'max_date': df['Дата'].max() if len(df) > 0 else None,
            'unique_dates': df['Дата'].nunique() if len(df) > 0 else 0,
            'weekends_remaining': len(df[df['Дата'].dt.weekday.isin([5, 6])]) if len(df) > 0 else 0,
            'manual_mode': USE_MANUAL_DATES,
            'manual_start': MANUAL_START_DATE if USE_MANUAL_DATES else None,
            'manual_end': MANUAL_END_DATE if USE_MANUAL_DATES else None,
            'status': 'SUCCESS'
        }
        
        metadata_path = os.path.join(PATH_TO_LOGS, 'etl_metadata.csv')
        pd.DataFrame([metadata]).to_csv(
            metadata_path,
            mode='a',
            index=False,
            header=not os.path.exists(metadata_path)
        )
        
        logging.info("\n✅ ETL ПРОЦЕСС УСПЕШНО ЗАВЕРШЕН")
        logging.info("=" * 80)
        
        return 0  # Успешное завершение
        
    except Exception as e:
        # Обработка ошибок
        end_time = dt.datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logging.error("\n" + "=" * 80)
        logging.error("❌ ОШИБКА В ETL ПРОЦЕССЕ")
        logging.error("=" * 80)
        logging.error(f"🔥 Тип ошибки: {type(e).__name__}")
        logging.error(f"💬 Сообщение: {str(e)}")
        logging.error(f"⏱️ Время выполнения до ошибки: {duration:.2f} сек")
        logging.error("=" * 80, exc_info=True)
        
        # Сохраняем информацию об ошибке
        error_metadata = {
            'timestamp': end_time,
            'start_time': start_time,
            'duration_sec': duration,
            'manual_mode': USE_MANUAL_DATES,
            'manual_start': MANUAL_START_DATE if USE_MANUAL_DATES else None,
            'manual_end': MANUAL_END_DATE if USE_MANUAL_DATES else None,
            'status': 'FAILED',
            'error_type': type(e).__name__,
            'error_message': str(e)
        }
        
        metadata_path = os.path.join(PATH_TO_LOGS, 'etl_metadata.csv')
        try:
            pd.DataFrame([error_metadata]).to_csv(
                metadata_path,
                mode='a',
                index=False,
                header=not os.path.exists(metadata_path)
            )
        except:
            logging.error("❌ Не удалось сохранить метаданные ошибки")
        
        return 1  # Код ошибки


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)