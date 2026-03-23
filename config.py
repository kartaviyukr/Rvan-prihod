"""
Централизованная конфигурация ETL-пайплайна анализа дефектуры

Порог дефектуры:
    - Если ЭО >= MIN_EO_FOR_PCT  ->  дефектура при ГК < DEFECT_EO_PCT * ЭО
    - Если ЭО < MIN_EO_FOR_PCT   ->  дефектура только при ГК == 0
    - Если КАГ нет в справочнике  ->  дефектура только при ГК == 0

Все пути и параметры задаются здесь. Остальные модули импортируют Config.
"""
import os
from pathlib import Path
from typing import Dict, List


class Config:
    """Единый источник параметров проекта"""

    # === Корневой путь проекта ===
    PROJECT_ROOT = Path(
        os.environ.get("ETL_PROJECT_ROOT", r"C:\Проекты\Project_etl_power_bi")
    )

    # === Пороги анализа ===
    MIN_OBS_LAST30 = 3
    LAST30_DAYS = 30
    LOOKBACK_DEFECTS_DAYS = 90

    DEFECT_EO_PCT = 0.05        # 5% от ЭО
    MIN_EO_FOR_PCT = 5          # Минимум ЭО для процентного порога

    # Параметры детекции прихода
    DELTA_ARRIVAL = 100
    MIN_PCT_FROM_YESTERDAY = 0.10

    # Визуализация
    N_WIDE_LAST = 20

    # === Колонки датасета ===
    COL_DATE = "Дата"
    COL_KAG = "Код КАГ"
    COL_KAG_NAME = "Имя КАГ"

    COL_GK = "ГК (остатки гранд капитала)"
    COL_PULS = "Пульс (остатки пульса)"
    COL_KATREN = "Катрен (остатки катрена)"
    COL_PROTEK = "Протек (остатки протека)"
    COL_FK = "Фармкомплект (остатки фармкомплекта)"

    COMP_COLS: List[str] = [COL_PULS, COL_KATREN, COL_PROTEK, COL_FK]

    COMP_PRETTY: Dict[str, str] = {
        COL_PULS: "Пульс",
        COL_KATREN: "Катрен",
        COL_PROTEK: "Протек",
        COL_FK: "Фармкомплект",
    }

    # Колонки цен
    PRICE_COLS: List[str] = [
        "Цена пульса",
        "Цена катрена",
        "Цена протека",
        "Цена фармкомплекта",
    ]

    # === Пути к данным ===
    DATA_DIR = PROJECT_ROOT / "data"
    RAW_DIR = DATA_DIR / "raw"
    INTERIM_DIR = DATA_DIR / "interim"
    PREPROC_DIR = DATA_DIR / "preproc_parquet"
    OUT_DIR = DATA_DIR / "result"
    HISTORY_DIR = DATA_DIR / "history"
    LOG_DIR = PROJECT_ROOT / "logs"

    PLOTS_DIR = OUT_DIR / "графики_остатков"
    EO_FILE = OUT_DIR / "EO.xlsx"
    CLEAN_PARQUET = PREPROC_DIR / "big_data_clean.parquet"
    RAW_PARQUET = RAW_DIR / "big_data.parquet"

    # === База данных (источник) ===
    DB_SOURCE_DRIVER = os.environ.get("DB_SOURCE_DRIVER", "{ODBC Driver 18 for SQL Server}")
    DB_SOURCE_SERVER = os.environ.get("DB_SOURCE_SERVER", "192.168.200.196,59197")
    DB_SOURCE_DATABASE = os.environ.get("DB_SOURCE_DATABASE", "dwh_price")
    DB_SOURCE_UID = os.environ.get("DB_SOURCE_UID", "ii_user")
    DB_SOURCE_PWD = os.environ.get("DB_SOURCE_PWD", "Vjoi970N")
    DB_SOURCE_SCHEMA = "dbo"
    DB_SOURCE_VIEW = "AI_stock_farm_market_table"

    # === База данных (назначение) ===
    DB_DEST_SERVER = os.environ.get("DB_DEST_SERVER", r"hq-sql06\dwh")
    DB_DEST_DATABASE = os.environ.get("DB_DEST_DATABASE", "DWH_Analysis_Results")

    # === Python интерпретатор ===
    PYTHON_EXE = os.environ.get(
        "ETL_PYTHON_EXE",
        r"C:\Проекты\Project_etl_power_bi\venv\Scripts\python.exe",
    )

    # === Праздники (Россия 2025-2026) ===
    RUSSIAN_HOLIDAYS: List[str] = [
        # 2025
        '2024-12-30', '2024-12-31',
        '2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04',
        '2025-01-05', '2025-01-06', '2025-01-07', '2025-01-08', '2025-01-09',
        '2025-02-22', '2025-02-23', '2025-02-24',
        '2025-03-07', '2025-03-08', '2025-03-10',
        '2025-04-30', '2025-05-01', '2025-05-02',
        '2025-05-08', '2025-05-09',
        '2025-06-11', '2025-06-12', '2025-06-13',
        '2025-11-03', '2025-11-04',
        # 2026
        '2025-12-30', '2025-12-31',
        '2026-01-01', '2026-01-02', '2026-01-03', '2026-01-04',
        '2026-01-05', '2026-01-06', '2026-01-07', '2026-01-08', '2026-01-09',
        '2026-02-21', '2026-02-23',
        '2026-03-07', '2026-03-09',
        '2026-04-30', '2026-05-01', '2026-05-04',
        '2026-05-08', '2026-05-11',
        '2026-06-11', '2026-06-12',
        '2026-11-03', '2026-11-04',
    ]

    @classmethod
    def get_required_columns(cls) -> List[str]:
        return [cls.COL_DATE, cls.COL_KAG, cls.COL_GK, *cls.COMP_COLS]

    @classmethod
    def setup_directories(cls):
        cls.OUT_DIR.mkdir(parents=True, exist_ok=True)
        cls.PLOTS_DIR.mkdir(parents=True, exist_ok=True)
        cls.LOG_DIR.mkdir(parents=True, exist_ok=True)
        cls.PREPROC_DIR.mkdir(parents=True, exist_ok=True)
        cls.INTERIM_DIR.mkdir(parents=True, exist_ok=True)


class CategoryConfig:
    """Категории дефектуры по давности"""

    @staticmethod
    def categorize_by_days_ago(days_ago: int) -> str:
        if days_ago == 0:
            return "Мы вошли в дефектуру в последний день"
        if 1 <= days_ago <= 40:
            return "Мы вошли в дефектуру 1–40 дней назад"
        if 41 <= days_ago <= 90:
            return "Мы вошли в дефектуру 41–90 дней назад"
        return "Мы вошли в дефектуру более 90 дней назад"
