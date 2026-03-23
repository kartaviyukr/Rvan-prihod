"""
Оркестратор ETL-пайплайна
Запуск: ежедневно через Task Scheduler
"""
import os
import sys
import re
import shutil
import subprocess
import logging
from datetime import datetime, timedelta
from pathlib import Path

from config import Config

# Директории для архивации
DATA_DIRS_TO_ARCHIVE = [Config.RAW_DIR, Config.INTERIM_DIR, Config.PREPROC_DIR]

# Скрипты для выполнения (в порядке запуска)
SCRIPTS_TO_RUN = [
    {"name": "1. Загрузка и предобработка", "path": Config.PROJECT_ROOT / "process" / "first_block_load" / "etl.py", "update_date": True},
    {"name": "2. Расчёт последней точки", "path": Config.PROJECT_ROOT / "process" / "third_block_process" / "poin_calculator.py", "update_date": False},
    {"name": "3. Расчёт эпизодов", "path": Config.PROJECT_ROOT / "process" / "third_block_process" / "episodes_calculator_2.py", "update_date": False},
    {"name": "4. Excel-отчёт", "path": Config.PROJECT_ROOT / "process" / "fourth_block_excel_remastered" / "excel_process.py", "update_date": False},
    {"name": "5. Отправка email", "path": Config.PROJECT_ROOT / "process" / "fourth_block_excel_remastered" / "send_report_email.py", "update_date": False},
]

ERROR_RECIPIENTS = ["DOKhokhlov@datauniverse.ru", "SIMukovoz@datauniverse.ru"]
LOG_FILE = Config.LOG_DIR / "orchestrator.log"


def _setup_logging():
    Config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(LOG_FILE, encoding='utf-8')
    fh.setFormatter(formatter)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger = logging.getLogger('orchestrator')
    logger.setLevel(logging.INFO)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


logger = _setup_logging()


def archive_data() -> int:
    """Перемещает содержимое data-директорий в history."""
    Config.HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    date_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
    total = 0

    for source_dir in DATA_DIRS_TO_ARCHIVE:
        if not source_dir.exists():
            continue
        history_sub = Config.HISTORY_DIR / source_dir.name
        history_sub.mkdir(parents=True, exist_ok=True)

        for item in source_dir.iterdir():
            try:
                new_name = f"{item.stem}_{date_suffix}{item.suffix}" if item.is_file() else f"{item.name}_{date_suffix}"
                shutil.move(str(item), str(history_sub / new_name))
                total += 1
            except Exception as e:
                logger.error(f"Ошибка перемещения {item.name}: {e}")

    logger.info(f"Архивировано: {total} объектов")
    return total


def update_manual_end_date(script_path: Path):
    """Обновляет MANUAL_END_DATE в скрипте на вчерашнюю дату."""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    content = script_path.read_text(encoding='utf-8')
    pattern = r"(MANUAL_END_DATE\s*=\s*['\"])(\d{4}-\d{2}-\d{2})(['\"])"
    match = re.search(pattern, content)
    if not match:
        raise ValueError(f"MANUAL_END_DATE не найден в {script_path}")

    old_date = match.group(2)
    new_content = re.sub(pattern, rf"\g<1>{yesterday}\g<3>", content)
    script_path.write_text(new_content, encoding='utf-8')
    logger.info(f"Дата обновлена: {old_date} -> {yesterday}")


def run_script(script_info: dict) -> bool:
    """Запускает Python-скрипт с real-time логами."""
    name = script_info["name"]
    script_path = Path(script_info["path"])
    logger.info(f"ЗАПУСК: {name} ({script_path})")

    if not script_path.exists():
        logger.error(f"Скрипт не найден: {script_path}")
        return False

    if script_info.get("update_date"):
        try:
            update_manual_end_date(script_path)
        except Exception as e:
            logger.error(f"Ошибка обновления даты: {e}")
            return False

    start_time = datetime.now()
    env = os.environ.copy()
    env['PYTHONIOENCODING'] = 'utf-8'
    env['PYTHONUTF8'] = '1'

    try:
        process = subprocess.Popen(
            [Config.PYTHON_EXE, "-u", str(script_path)],
            cwd=str(script_path.parent),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding='utf-8', errors='replace',
            env=env, bufsize=1,
        )
        while True:
            line = process.stdout.readline()
            if not line and process.poll() is not None:
                break
            if line.strip():
                print(f"   | {line.rstrip()}")

        return_code = process.wait()
        duration = (datetime.now() - start_time).total_seconds()

        if return_code == 0:
            logger.info(f"OK: {name} за {duration:.1f} сек")
            return True
        logger.error(f"ОШИБКА: {name}, код {return_code}")
        return False

    except Exception as e:
        logger.error(f"Исключение в {name}: {e}")
        return False


def send_error_notification(failed_step: str, error_details: str):
    """Уведомление об ошибке (SMTP в Docker, Outlook на Windows)."""
    subject = f"[ОШИБКА] ETL: {failed_step} ({datetime.now().strftime('%d.%m.%Y')})"
    body = f"Этап: {failed_step}\n\n{error_details}\n\nЛоги: {LOG_FILE}"
    try:
        from pipeline.email_sender import _send_mail
        _send_mail(ERROR_RECIPIENTS, subject, body, importance=2)
        logger.info("Уведомление об ошибке отправлено")
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление: {e}")


def main():
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("ЗАПУСК ОРКЕСТРАТОРА")

    try:
        # Архивация
        try:
            archive_data()
        except Exception as e:
            logger.warning(f"Ошибка архивации (продолжаем): {e}")

        # Запуск скриптов
        for script_info in SCRIPTS_TO_RUN:
            if not run_script(script_info):
                failed_step = script_info["name"]
                send_error_notification(failed_step, f"Скрипт {script_info['path']} завершился с ошибкой")
                return 1

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Оркестратор завершён за {duration:.1f} сек")
        return 0

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        send_error_notification("Неизвестный этап", str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
