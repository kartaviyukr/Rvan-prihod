"""
Автоматическая рассылка отчёта через Outlook
Запуск: ежедневно в 13:00 через Task Scheduler
"""

import os
import pandas as pd
from datetime import datetime, timedelta

# ==============================
# ⚙️ НАСТРОЙКИ
# ==============================

FILE_PATH = r"C:\Проекты\Project_etl_power_bi\data\result\final_merged_table.xlsx"

# Получатели отчёта
RECIPIENTS = [
    # "lipkina@grand-capital.pro",
    "DOKhokhlov@datauniverse.ru",
    # 'KNPlakhtyukov@grand-capital.pro',
    # 'TIVlasova@grand-capital.pro',
    # 'NAvdeev@grand-capital.pro',
    # 'ESavenkova@grand-capital.pro',
    # 'EVasileva@grand-capital.pro',
    # 'AADemidov@grand-capital.pro'
]
# 
# Получатели уведомлений об ошибках
ERROR_RECIPIENTS = [
    "DOKhokhlov@datauniverse.ru",
    "SIMukovoz@datauniverse.ru"
]

SUBJECT = "Отчёт: Рваный приход (дефектура)"

BODY = """Добрый день, коллеги!

Благодарим за обратную связь по файлу.

Сейчас в работе комментарии Алексея Александровича и Константина Николеавича: 
2. Добавить информацию о последней дате заказа, потребности, счёте
4. Добавить информацию о последней дате поступления на наш склад
5. Добавить информацию о товаре в пути

Обновление статуса:
1. Добавить привязку "Прямой поставщик - Менеджер" - Сегодня тестируем добавление. Пока некорректный список. 
3. Проверить данные по продуктам СОЛГАР - Нашли ошибку в первоисточнике наших данных. Исправили. 


Во вложении новые данные по рваным приходам. 
Прошу взять в работу. Сообщение будет приходить во второй половине дня для изучения и проработки к следующему утру. 
Лист "Новые позиции" для оперативного разбора.
Лист "Текущие" — все активные дефектуры на данный момент.
Лист "Завершившиеся" — исторические эпизоды дефектуры для анализа прогресса.

*Рваными приходами мы называем события, когда на остатке у ФК Гранд Капитал нет товара, но у наших конкуретов произошло пополнение.
---
Это автоматическое сообщение.
"""

# Колонки для валидации (сумма по каждой должна быть >= 100)
# Проверяем только лист "Текущие"
VALIDATION_COLUMNS = [
    'Объём прихода Пульс (сумма)',
    'Объём прихода Катрен (сумма)',
    'Объём прихода Протек (сумма)',
    'Объём прихода Фармкомплект (сумма)',
    'Остаток Пульс (вчера)',
    'Остаток Катрен (вчера)',
    'Остаток Протек (вчера)',
    'Остаток Фармкомплект (вчера)',
]

# Исключения: колонки, которые НЕ проверяем
SKIP_VALIDATION = []

MIN_SUM_THRESHOLD = 100

# Максимальный возраст самой свежей дефектуры (дней)
MAX_DAYS_SINCE_LAST_DEFECTURA = 7


# ==============================
# ✅ ВАЛИДАЦИЯ
# ==============================

def validate_data(filepath: str) -> tuple[bool, list[str], list[str]]:
    """
    Проверяет данные перед отправкой.
    Читает лист "Текущие" (текущие дефектуры).
    
    Returns:
        (is_valid, errors, warnings) - флаг валидности, список ошибок и предупреждений
    """
    errors = []
    warnings = []
    
    # Загружаем лист "Текущие"
    try:
        df = pd.read_excel(filepath, sheet_name='Текущие')
    except Exception:
        # Фоллбэк на первый лист (старый формат файла)
        try:
            df = pd.read_excel(filepath, sheet_name=0)
            warnings.append("Лист 'Текущие' не найден, прочитан первый лист")
        except Exception as e:
            return False, [f"Не удалось прочитать файл: {e}"], []
    
    if df.empty:
        return False, ["Лист 'Текущие' пустой — нет записей"], []
    
    print(f"\n📊 Лист 'Текущие': {len(df)} записей")
    
    # ==========================================
    # ПРОВЕРКА 1: Свежесть данных
    # ==========================================
    date_col = 'Дата входа в дефектуру ФК Гранд Капитал'
    
    if date_col in df.columns:
        print(f"\n📅 Проверка свежести данных...")
        
        dates = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce')
        valid_dates = dates.dropna()
        
        if valid_dates.empty:
            warnings.append(f"Не удалось распарсить даты в колонке '{date_col}'")
        else:
            latest_date = valid_dates.max()
            today = pd.Timestamp.now().normalize()
            days_since_latest = (today - latest_date).days
            
            print(f"   Самая свежая дефектура: {latest_date.strftime('%d.%m.%Y')}")
            print(f"   Дней назад: {days_since_latest}")
            
            if days_since_latest > MAX_DAYS_SINCE_LAST_DEFECTURA:
                errors.append(
                    f"Последняя дефектура началась {days_since_latest} дней назад "
                    f"({latest_date.strftime('%d.%m.%Y')}). "
                    f"Данные не обновляются!"
                )
                print(f"   ❌ ОШИБКА: Данные устарели!")
            else:
                print(f"   ✅ Данные актуальны")
    
    # ==========================================
    # ПРОВЕРКА 2: Суммы по колонкам (только текущие)
    # ==========================================
    print(f"\n📊 Проверка колонок...")
    
    for col in VALIDATION_COLUMNS:
        if col in SKIP_VALIDATION:
            print(f"   ⏭️ {col}: пропущено (исключение)")
            continue
        
        if col not in df.columns:
            errors.append(f"Колонка '{col}' не найдена")
            continue
        
        col_sum = pd.to_numeric(df[col], errors='coerce').fillna(0).sum()
        
        if col_sum < MIN_SUM_THRESHOLD:
            errors.append(
                f"'{col}' = {col_sum:.0f} (меньше {MIN_SUM_THRESHOLD})"
            )
            print(f"   ❌ {col}: {col_sum:.0f}")
        else:
            print(f"   ✅ {col}: {col_sum:,.0f}")
    
    # ==========================================
    # ПРОВЕРКА 3: Наличие листа "Завершившиеся"
    # ==========================================
    try:
        df_fin = pd.read_excel(filepath, sheet_name='Завершившиеся')
        print(f"\n📊 Лист 'Завершившиеся': {len(df_fin)} записей ✅")
    except Exception:
        warnings.append("Лист 'Завершившиеся' не найден в файле")
    
    is_valid = len(errors) == 0
    return is_valid, errors, warnings


# ==============================
# 🚨 ОТПРАВКА ОШИБОК
# ==============================

def send_error_notification(errors: list[str], exception: str = None):
    """Отправляет уведомление об ошибке на почту разработчикам"""
    import win32com.client as win32
    
    timestamp = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    
    error_subject = f"🚨 ОШИБКА: Отчёт дефектуры не отправлен ({datetime.now().strftime('%d.%m.%Y')})"
    
    error_body = f"""Автоматическая отправка отчёта дефектуры была отменена.

Время: {timestamp}
Файл: {FILE_PATH}

{'=' * 50}
ОШИБКИ ВАЛИДАЦИИ:
{'=' * 50}
"""
    
    for err in errors:
        error_body += f"\n• {err}"
    
    if exception:
        error_body += f"""

{'=' * 50}
ИСКЛЮЧЕНИЕ:
{'=' * 50}
{exception}
"""
    
    error_body += f"""

{'=' * 50}
Необходимо проверить:
1. Корректность исходных данных
2. Работу ETL-пайплайна
3. Логи скриптов calculate_episodes.py и merge_defectura_tables.py

---
Это автоматическое сообщение системы мониторинга.
"""
    
    try:
        outlook = win32.Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)
        
        mail.To = "; ".join(ERROR_RECIPIENTS)
        mail.Subject = error_subject
        mail.Body = error_body
        mail.Importance = 2  # Высокая важность
        
        mail.Send()
        
        print(f"\n📧 Уведомление об ошибке отправлено:")
        print(f"   Получатели: {', '.join(ERROR_RECIPIENTS)}")
        return True
    except Exception as e:
        print(f"\n❌ Не удалось отправить уведомление об ошибке: {e}")
        return False


def send_warning_notification(warnings: list[str]):
    """Отправляет уведомление о предупреждениях (отчёт всё равно отправляется)"""
    import win32com.client as win32
    
    timestamp = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    
    warning_subject = f"⚠️ ПРЕДУПРЕЖДЕНИЕ: Отчёт дефектуры отправлен с замечаниями ({datetime.now().strftime('%d.%m.%Y')})"
    
    warning_body = f"""Отчёт дефектуры был отправлен, но есть предупреждения.

Время: {timestamp}
Файл: {FILE_PATH}

{'=' * 50}
ПРЕДУПРЕЖДЕНИЯ:
{'=' * 50}
"""
    
    for warn in warnings:
        warning_body += f"\n⚠️ {warn}"
    
    warning_body += f"""

{'=' * 50}
Рекомендуется проверить:
1. Актуальность исходных данных
2. Корректность работы ETL-пайплайна
3. Наличие новых дефектур в системе

---
Это автоматическое сообщение системы мониторинга.
"""
    
    try:
        outlook = win32.Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)
        
        mail.To = "; ".join(ERROR_RECIPIENTS)
        mail.Subject = warning_subject
        mail.Body = warning_body
        mail.Importance = 1  # Средняя важность
        
        mail.Send()
        
        print(f"\n📧 Уведомление о предупреждениях отправлено:")
        print(f"   Получатели: {', '.join(ERROR_RECIPIENTS)}")
        return True
    except Exception as e:
        print(f"\n❌ Не удалось отправить уведомление о предупреждениях: {e}")
        return False


# ==============================
# 📧 ОТПРАВКА ОТЧЁТА
# ==============================

def send_email():
    import shutil
    import tempfile
    import win32com.client as win32
    
    # Проверка существования файла
    if not os.path.exists(FILE_PATH):
        error_msg = f"Файл не найден: {FILE_PATH}"
        print(f"❌ {error_msg}")
        send_error_notification([error_msg])
        return False
    
    # Валидация данных
    print("\n🔍 Валидация данных...")
    is_valid, errors, warnings = validate_data(FILE_PATH)
    
    # Критические ошибки — не отправляем отчёт
    if not is_valid:
        print("\n" + "=" * 50)
        print("❌ ОТПРАВКА ОТМЕНЕНА - ОШИБКИ ВАЛИДАЦИИ:")
        print("=" * 50)
        for err in errors:
            print(f"   {err}")
        print("=" * 50)
        
        send_error_notification(errors)
        return False
    
    print("\n✅ Валидация пройдена!")
    
    # Копируем файл во временную папку с красивым именем
    date_str = datetime.now().strftime("%d.%m.%Y")
    attachment_name = f"Рваные_приходы_{date_str}.xlsx"
    tmp_dir = tempfile.mkdtemp()
    tmp_path = os.path.join(tmp_dir, attachment_name)
    shutil.copy2(FILE_PATH, tmp_path)
    
    # Отправка отчёта
    try:
        outlook = win32.Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)
        
        mail.To = "; ".join(RECIPIENTS)
        mail.Subject = SUBJECT
        mail.Body = BODY
        mail.Attachments.Add(tmp_path)
        
        mail.Send()
        
        print(f"\n✅ Email отправлен:")
        print(f"   Получатели: {', '.join(RECIPIENTS)}")
        print(f"   Вложение: {attachment_name}")
    finally:
        # Убираем временный файл
        try:
            os.remove(tmp_path)
            os.rmdir(tmp_dir)
        except OSError:
            pass
    
    # Предупреждения — отчёт отправляем, но уведомляем разработчиков
    if warnings:
        print("\n" + "=" * 50)
        print("⚠️ ЕСТЬ ПРЕДУПРЕЖДЕНИЯ:")
        print("=" * 50)
        for warn in warnings:
            print(f"   {warn}")
        print("=" * 50)
        
        send_warning_notification(warnings)
    
    return True


if __name__ == "__main__":
    print("=" * 50)
    print("📧 ОТПРАВКА ОТЧЁТА")
    print("=" * 50)
    
    try:
        success = send_email()
        exit_code = 0 if success else 1
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        send_error_notification(["Критическая ошибка выполнения скрипта"], str(e))
        exit_code = 1
    
    print("=" * 50)
    exit(exit_code)