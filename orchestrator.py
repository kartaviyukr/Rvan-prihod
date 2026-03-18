"""
🎼 ОРКЕСТРАТОР ETL-ПАЙПЛАЙНА
Главный скрипт для запуска всего процесса обработки данных дефектуры

Запуск: ежедневно через Task Scheduler
"""

import os
import sys
import shutil
import subprocess
import re
import logging
from datetime import datetime, timedelta
from pathlib import Path

# ==============================
# ⚙️ НАСТРОЙКИ
# ==============================

# Корневая директория проекта
PROJECT_ROOT = Path(r"C:\Проекты\Project_etl_power_bi")

# Директории с данными для архивации
DATA_DIRS_TO_ARCHIVE = [
    PROJECT_ROOT / "data" / "raw",
    PROJECT_ROOT / "data" / "interim",
    PROJECT_ROOT / "data" / "preproc_parquet",
]

# Директория для архивов
HISTORY_DIR = PROJECT_ROOT / "data" / "history"

# Скрипты для выполнения (в порядке запуска)
SCRIPTS_TO_RUN = [
    {
        "name": "1. Загрузка и предобработка данных",
        "path": PROJECT_ROOT / "process" / "first_block_load" / "mainer.py",
        "update_date": True,
    },
    {
        "name": "2. Расчёт последней точки дефектуры",
        "path": PROJECT_ROOT / "process" / "third_block_process" / "poin_calculator.py",
        "update_date": False,
    },
    {
        "name": "3. Расчёт эпизодов дефектуры",
        "path": PROJECT_ROOT / "process" / "third_block_process" / "episodes_calculator_2.py",
        "update_date": False,
    },
    {
        "name": "4. Формирование Excel-отчёта",
        "path": PROJECT_ROOT / "process" / "fourth_block_excel_remastered" / "excel_process.py",
        "update_date": False,
    },
    {
        "name": "5. Отправка отчёта по email",
        "path": PROJECT_ROOT / "process" / "fourth_block_excel_remastered" / "send_report_email.py",
        "update_date": False,
    },
]

# Путь к Python интерпретатору
PYTHON_EXE = r"C:\Проекты\Project_etl_power_bi\venv\Scripts\python.exe"

# Получатели уведомлений об ошибках
ERROR_RECIPIENTS = [
    "DOKhokhlov@datauniverse.ru",
    "SIMukovoz@datauniverse.ru"
]

# Логирование
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "orchestrator.log"


# ==============================
# 📝 НАСТРОЙКА ЛОГИРОВАНИЯ
# ==============================

def setup_logging():
    """Настраивает логирование"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    # Создаём форматтер
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Файловый хендлер (UTF-8)
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setFormatter(formatter)
    
    # Консольный хендлер (с защитой от Unicode ошибок)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Настраиваем логгер
    logger = logging.getLogger('orchestrator')
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


logger = setup_logging()


# ==============================
# 📦 АРХИВАЦИЯ ДАННЫХ
# ==============================

def archive_data():
    """
    Перемещает содержимое папок raw, interim, preproc_parquet в history
    с добавлением даты к именам файлов/папок
    """
    logger.info("=" * 60)
    logger.info("АРХИВАЦИЯ ДАННЫХ")
    logger.info("=" * 60)
    
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    
    date_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    total_moved = 0
    
    for source_dir in DATA_DIRS_TO_ARCHIVE:
        if not source_dir.exists():
            logger.warning(f"   Папка не существует: {source_dir}")
            continue
        
        dir_name = source_dir.name
        history_subdir = HISTORY_DIR / dir_name
        history_subdir.mkdir(parents=True, exist_ok=True)
        
        items = list(source_dir.iterdir())
        
        if not items:
            logger.info(f"   {dir_name}: пусто")
            continue
        
        logger.info(f"   {dir_name}: {len(items)} объектов")
        
        for item in items:
            try:
                if item.is_file():
                    stem = item.stem
                    suffix = item.suffix
                    new_name = f"{stem}_{date_suffix}{suffix}"
                else:
                    new_name = f"{item.name}_{date_suffix}"
                
                destination = history_subdir / new_name
                shutil.move(str(item), str(destination))
                logger.info(f"      OK: {item.name} -> {new_name}")
                total_moved += 1
                
            except Exception as e:
                logger.error(f"      ОШИБКА перемещения {item.name}: {e}")
    
    logger.info(f"   Итого перемещено: {total_moved} объектов")
    logger.info("=" * 60)
    
    return total_moved


# ==============================
# 📅 ОБНОВЛЕНИЕ ДАТЫ В СКРИПТЕ
# ==============================

def update_manual_end_date(script_path: Path):
    """
    Обновляет MANUAL_END_DATE в скрипте на вчерашнюю дату
    """
    logger.info(f"   Обновление MANUAL_END_DATE в {script_path.name}")
    
    if not script_path.exists():
        raise FileNotFoundError(f"Скрипт не найден: {script_path}")
    
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    with open(script_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    pattern = r"(MANUAL_END_DATE\s*=\s*['\"])(\d{4}-\d{2}-\d{2})(['\"])"
    
    match = re.search(pattern, content)
    if not match:
        raise ValueError(f"MANUAL_END_DATE не найден в {script_path}")
    
    old_date = match.group(2)
    new_content = re.sub(pattern, rf"\g<1>{yesterday}\g<3>", content)
    
    with open(script_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    logger.info(f"      {old_date} -> {yesterday}")
    
    return old_date, yesterday


# ==============================
# 🚀 ЗАПУСК СКРИПТА (REAL-TIME ЛОГИ)
# ==============================

def run_script(script_info: dict) -> bool:
    """
    Запускает Python скрипт с выводом логов в реальном времени
    
    Returns:
        True если успешно, False если ошибка
    """
    name = script_info["name"]
    script_path = Path(script_info["path"])
    update_date = script_info.get("update_date", False)
    
    logger.info("\n" + "-" * 60)
    logger.info(f"ЗАПУСК: {name}")
    logger.info(f"   Скрипт: {script_path}")
    logger.info("-" * 60)
    
    # Проверяем существование скрипта
    if not script_path.exists():
        logger.error(f"   ОШИБКА: Скрипт не найден: {script_path}")
        return False
    
    # Обновляем дату если нужно
    if update_date:
        try:
            old_date, new_date = update_manual_end_date(script_path)
        except Exception as e:
            logger.error(f"   ОШИБКА обновления даты: {e}")
            return False
    
    # Запускаем скрипт
    start_time = datetime.now()
    
    try:
        script_dir = script_path.parent
        
        # Переменные окружения для UTF-8
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        env['PYTHONUTF8'] = '1'
        
        # Используем Popen для real-time вывода
        process = subprocess.Popen(
            [PYTHON_EXE, "-u", str(script_path)],
            cwd=str(script_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Объединяем stderr в stdout
            text=True,
            encoding='utf-8',
            errors='replace',
            env=env,
            bufsize=1  # Line buffered
        )
        
        # Читаем и выводим логи в реальном времени
        while True:
            line = process.stdout.readline()
            if not line and process.poll() is not None:
                break
            if line:
                line = line.rstrip()
                if line:
                    # Выводим в консоль напрямую (без логгера для чистоты)
                    print(f"   | {line}")
                    # И пишем в лог-файл
                    with open(LOG_FILE, 'a', encoding='utf-8') as f:
                        f.write(f"   | {line}\n")
        
        # Получаем код возврата
        return_code = process.wait()
        duration = (datetime.now() - start_time).total_seconds()
        
        if return_code == 0:
            logger.info(f"\n   OK: Успешно завершён за {duration:.1f} сек")
            return True
        else:
            logger.error(f"\n   ОШИБКА: Код возврата: {return_code}")
            return False
            
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"   ИСКЛЮЧЕНИЕ после {duration:.1f} сек: {e}")
        return False


# ==============================
# 🚨 УВЕДОМЛЕНИЕ ОБ ОШИБКЕ
# ==============================

def send_error_notification(failed_step: str, error_details: str):
    """Отправляет уведомление об ошибке оркестратора"""
    logger.info("Отправка уведомления об ошибке...")
    
    try:
        import win32com.client as win32
    except ImportError as e:
        logger.error(f"   ОШИБКА: Модуль win32com не установлен: {e}")
        logger.error(f"   Установите: pip install pywin32")
        return False
    
    timestamp = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    
    subject = f"[ОШИБКА] Оркестратор ETL: {failed_step} ({datetime.now().strftime('%d.%m.%Y')})"
    
    body = f"""Оркестратор ETL-пайплайна остановлен из-за ошибки.

Время: {timestamp}
Этап: {failed_step}

{'=' * 50}
ДЕТАЛИ ОШИБКИ:
{'=' * 50}
{error_details}

{'=' * 50}
РЕКОМЕНДАЦИИ:
{'=' * 50}
1. Проверьте логи: {LOG_FILE}
2. Проверьте доступность БД и сетевых ресурсов
3. Убедитесь что все зависимости установлены
4. При необходимости запустите скрипты вручную

---
Это автоматическое сообщение оркестратора.
"""
    
    try:
        logger.info(f"   Подключение к Outlook...")
        outlook = win32.Dispatch("Outlook.Application")
        
        logger.info(f"   Создание письма...")
        mail = outlook.CreateItem(0)
        
        mail.To = "; ".join(ERROR_RECIPIENTS)
        mail.Subject = subject
        mail.Body = body
        mail.Importance = 2  # Высокая важность
        
        logger.info(f"   Отправка на: {', '.join(ERROR_RECIPIENTS)}")
        mail.Send()
        
        logger.info(f"   OK: Уведомление отправлено!")
        return True
        
    except Exception as e:
        logger.error(f"   ОШИБКА отправки email: {type(e).__name__}: {e}")
        return False


# ==============================
# 🎼 ГЛАВНАЯ ФУНКЦИЯ
# ==============================

def main():
    """Главная функция оркестратора"""
    
    start_time = datetime.now()
    
    print("\n" + "=" * 60)
    print("ЗАПУСК ОРКЕСТРАТОРА ETL-ПАЙПЛАЙНА")
    print("=" * 60)
    
    logger.info("=" * 60)
    logger.info("ЗАПУСК ОРКЕСТРАТОРА ETL-ПАЙПЛАЙНА")
    logger.info("=" * 60)
    logger.info(f"Время запуска: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Проект: {PROJECT_ROOT}")
    logger.info(f"Python: {PYTHON_EXE}")
    logger.info("=" * 60)
    
    failed_step = None
    error_details = ""
    
    try:
        # ========================
        # ШАГ 0: АРХИВАЦИЯ
        # ========================
        logger.info("\nШАГ 0: АРХИВАЦИЯ СТАРЫХ ДАННЫХ")
        
        try:
            archived = archive_data()
            logger.info(f"OK: Архивация завершена: {archived} объектов")
        except Exception as e:
            logger.warning(f"ПРЕДУПРЕЖДЕНИЕ: Ошибка архивации (продолжаем): {e}")
        
        # ========================
        # ШАГИ 1-5: СКРИПТЫ
        # ========================
        for i, script_info in enumerate(SCRIPTS_TO_RUN, 1):
            success = run_script(script_info)
            
            if not success:
                failed_step = script_info["name"]
                error_details = f"Скрипт {script_info['path']} завершился с ошибкой"
                
                logger.error("\n" + "=" * 60)
                logger.error(f"ПАЙПЛАЙН ОСТАНОВЛЕН НА ШАГЕ: {failed_step}")
                logger.error("=" * 60)
                
                # Отправляем уведомление
                send_error_notification(failed_step, error_details)
                
                return 1
        
        # ========================
        # УСПЕШНОЕ ЗАВЕРШЕНИЕ
        # ========================
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "=" * 60)
        logger.info("ОРКЕСТРАТОР УСПЕШНО ЗАВЕРШЁН")
        logger.info("=" * 60)
        logger.info(f"Время начала:      {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Время окончания:   {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Общее время:       {duration:.1f} сек ({duration/60:.1f} мин)")
        logger.info("=" * 60)
        
        return 0
        
    except Exception as e:
        failed_step = "Неизвестный этап"
        error_details = f"{type(e).__name__}: {str(e)}"
        
        logger.error(f"\nКРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
        send_error_notification(failed_step, error_details)
        
        return 1


# ==============================
# 🚀 ТОЧКА ВХОДА
# ==============================

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)