"""
Автоматическая рассылка отчёта через Outlook
"""
import os
import shutil
import tempfile
import pandas as pd
from datetime import datetime

from config import Config

# Получатели отчёта
RECIPIENTS = [
    "lipkina@grand-capital.pro",
    "DOKhokhlov@datauniverse.ru",
    'KNPlakhtyukov@grand-capital.pro',
    'TIVlasova@grand-capital.pro',
    'NAvdeev@grand-capital.pro',
    'ESavenkova@grand-capital.pro',
    'EVasileva@grand-capital.pro',
    'AADemidov@grand-capital.pro',
]

ERROR_RECIPIENTS = [
    "DOKhokhlov@datauniverse.ru",
    "SIMukovoz@datauniverse.ru",
]

SUBJECT = "Отчёт: Рваный приход (дефектура)"

BODY = """Добрый день, коллеги!

Благодарим за обратную связь по файлу.

Сейчас в работе комментарии Алексея Александровича и Константина Николеавича:
2. Добавить информацию о последней дате заказа, потребности, счёте - Срок от отдела Домино начало Апреля
4. Добавить информацию о последней дате поступления на наш склад - Срок от отдела Домино начало Апреля
5. Добавить информацию о товаре в пути - 19.03.26

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

VALIDATION_COLUMNS = [
    'Объём прихода Пульс (сумма)', 'Объём прихода Катрен (сумма)',
    'Объём прихода Протек (сумма)', 'Объём прихода Фармкомплект (сумма)',
    'Остаток Пульс (вчера)', 'Остаток Катрен (вчера)',
    'Остаток Протек (вчера)', 'Остаток Фармкомплект (вчера)',
]

MIN_SUM_THRESHOLD = 100
MAX_DAYS_SINCE_LAST_DEFECTURA = 7

FILE_PATH = str(Config.OUT_DIR / "final_merged_table.xlsx")


def validate_data(filepath: str) -> tuple:
    """Проверяет данные перед отправкой. Returns (is_valid, errors, warnings)."""
    errors, warnings = [], []

    try:
        df = pd.read_excel(filepath, sheet_name='Текущие')
    except Exception:
        try:
            df = pd.read_excel(filepath, sheet_name=0)
            warnings.append("Лист 'Текущие' не найден, прочитан первый лист")
        except Exception as e:
            return False, [f"Не удалось прочитать файл: {e}"], []

    if df.empty:
        return False, ["Лист 'Текущие' пустой"], []

    # Свежесть данных
    date_col = 'Дата входа в дефектуру ФК Гранд Капитал'
    if date_col in df.columns:
        dates = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce').dropna()
        if not dates.empty:
            days_since = (pd.Timestamp.now().normalize() - dates.max()).days
            if days_since > MAX_DAYS_SINCE_LAST_DEFECTURA:
                errors.append(f"Последняя дефектура {days_since} дней назад. Данные не обновляются!")

    # Суммы по колонкам
    for col in VALIDATION_COLUMNS:
        if col not in df.columns:
            errors.append(f"Колонка '{col}' не найдена")
            continue
        col_sum = pd.to_numeric(df[col], errors='coerce').fillna(0).sum()
        if col_sum < MIN_SUM_THRESHOLD:
            errors.append(f"'{col}' = {col_sum:.0f} (меньше {MIN_SUM_THRESHOLD})")

    # Наличие листа «Завершившиеся»
    try:
        pd.read_excel(filepath, sheet_name='Завершившиеся')
    except Exception:
        warnings.append("Лист 'Завершившиеся' не найден")

    return len(errors) == 0, errors, warnings


def _send_outlook_mail(recipients: list, subject: str, body: str, attachment: str = None, importance: int = 1):
    """Отправляет письмо через Outlook COM."""
    import win32com.client as win32
    outlook = win32.Dispatch("Outlook.Application")
    mail = outlook.CreateItem(0)
    mail.To = "; ".join(recipients)
    mail.Subject = subject
    mail.Body = body
    mail.Importance = importance
    if attachment:
        mail.Attachments.Add(attachment)
    mail.Send()


def send_error_notification(errors: list, exception: str = None):
    """Уведомление об ошибке разработчикам."""
    timestamp = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    body = f"Отправка отчёта отменена.\nВремя: {timestamp}\nФайл: {FILE_PATH}\n\nОшибки:\n"
    body += '\n'.join(f"- {e}" for e in errors)
    if exception:
        body += f"\n\nИсключение:\n{exception}"
    try:
        _send_outlook_mail(
            ERROR_RECIPIENTS,
            f"ОШИБКА: Отчёт дефектуры не отправлен ({datetime.now().strftime('%d.%m.%Y')})",
            body, importance=2,
        )
    except Exception as e:
        print(f"Не удалось отправить уведомление об ошибке: {e}")


def send_email():
    """Валидирует данные и отправляет отчёт."""
    if not os.path.exists(FILE_PATH):
        send_error_notification([f"Файл не найден: {FILE_PATH}"])
        return False

    is_valid, errors, warnings = validate_data(FILE_PATH)
    if not is_valid:
        print(f"Валидация не пройдена: {errors}")
        send_error_notification(errors)
        return False

    date_str = datetime.now().strftime("%d.%m.%Y")
    attachment_name = f"Рваные_приходы_{date_str}.xlsx"
    tmp_dir = tempfile.mkdtemp()
    tmp_path = os.path.join(tmp_dir, attachment_name)
    shutil.copy2(FILE_PATH, tmp_path)

    try:
        _send_outlook_mail(RECIPIENTS, SUBJECT, BODY, attachment=tmp_path)
        print(f"Email отправлен: {', '.join(RECIPIENTS)}")
    finally:
        try:
            os.remove(tmp_path)
            os.rmdir(tmp_dir)
        except OSError:
            pass

    if warnings:
        try:
            _send_outlook_mail(
                ERROR_RECIPIENTS,
                f"Предупреждение: отчёт дефектуры ({date_str})",
                "Отчёт отправлен с предупреждениями:\n" + '\n'.join(f"- {w}" for w in warnings),
            )
        except Exception:
            pass

    return True


if __name__ == "__main__":
    try:
        success = send_email()
        exit(0 if success else 1)
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        send_error_notification(["Критическая ошибка выполнения"], str(e))
        exit(1)
