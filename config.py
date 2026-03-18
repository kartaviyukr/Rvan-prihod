"""
Конфигурация для анализа дефектуры товаров
"""
from pathlib import Path
from typing import Dict


class Config:
    """Централизованная конфигурация проекта"""
    
    # === Пороги и параметры анализа ===
    MIN_OBS_LAST30 = 3          # Минимум наблюдений за последние 30 дней
    LAST30_DAYS = 30
    LOOKBACK_DEFECTS_DAYS = 90   # Период поиска дефектур (дней назад)
    
    # Порог дефектуры: остаток ГК < DEFECT_EO_PCT * ЭО
    DEFECT_EO_DIVISOR = 20         # 10% от ЭО
    MIN_EO_FOR_PCT = 5          # Если ЭО < этого значения — дефектура только при ГК == 0
    
    # Параметры детекции "прихода" у конкурентов
    DELTA_ARRIVAL = 100                    # Абсолютное увеличение остатков
    MIN_PCT_FROM_YESTERDAY = 0.1          # Относительное увеличение (10%)
    
    # Визуализация
    N_WIDE_LAST = 20                       # Последних наблюдений для графиков
    
    # === Колонки датасета ===
    COL_DATE = "Дата"
    COL_KAG = "Код КАГ"
    COL_KAG_NAME = "Имя КАГ"
    
    COL_GK = "ГК (остатки гранд капитала)"
    COL_PULS = "Пульс (остатки пульса)"
    COL_KATREN = "Катрен (остатки катрена)"
    COL_PROTEK = "Протек (остатки протека)"
    COL_FK = "Фармкомплект (остатки фармкомплекта)"
    
    # Список конкурентов
    COMP_COLS = [COL_PULS, COL_KATREN, COL_PROTEK, COL_FK]
    
    # Красивые названия для отчётов
    COMP_PRETTY: Dict[str, str] = {
        COL_PULS: "Пульс",
        COL_KATREN: "Катрен",
        COL_PROTEK: "Протек",
        COL_FK: "Фармкомплект",
    }
    
    # === Пути ===
    OUT_DIR = Path(r"C:\Проекты\Project_etl_power_bi\data\result")
    PLOTS_DIR = OUT_DIR / "графики_остатков"
    EO_FILE = Path(r"C:\Проекты\Project_etl_power_bi\data\result\EO.xlsx")
    
    @classmethod
    def get_required_columns(cls) -> list:
        """Возвращает список обязательных колонок"""
        return [cls.COL_DATE, cls.COL_KAG, cls.COL_GK, *cls.COMP_COLS]
    
    @classmethod
    def setup_directories(cls):
        """Создаёт необходимые директории для экспорта"""
        cls.OUT_DIR.mkdir(exist_ok=True)
        cls.PLOTS_DIR.mkdir(exist_ok=True)


class CategoryConfig:
    """Категории дефектуры по давности"""
    
    @staticmethod
    def categorize_by_days_ago(days_ago: int) -> str:
        """
        Определяет категорию дефектуры по количеству дней
        
        Args:
            days_ago: Сколько дней назад начался дефект
            
        Returns:
            Название категории
        """
        if days_ago == 0:
            return "Мы вошли в дефектуру в последний день"
        if 1 <= days_ago <= 40:
            return "Мы вошли в дефектуру 1–40 дней назад"
        if 41 <= days_ago <= 90:
            return "Мы вошли в дефектуру 41–90 дней назад"
        return "Мы вошли в дефектуру более 90 дней назад"