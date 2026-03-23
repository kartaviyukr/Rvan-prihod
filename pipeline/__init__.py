"""
Модули ETL-пайплайна анализа дефектуры

Структура:
    extract:   db_loader, check_dates
    transform: preprocess, data_preparation
    analyze:   defectura_detection, arrival_detection, point_calculator,
               episodes_calculator, metrics
    load:      export, excel_process, email_sender, db_uploader, visualization
"""

from pipeline.db_loader import export_ai_stock_to_parquet, union_all_parquet, union_all_clean_parquet
from pipeline.check_dates import check_missing, checkup
from pipeline.preprocess import (
    base_action,
    collapse_kag_daily_smart,
    zero_small_stocks_conditional,
    drop_weekends_and_holidays,
    fix_competitor_drop_to_zero_anomalies,
)
from pipeline.data_preparation import DataPreparator
from pipeline.defectura_detection import DefecturaDetector
from pipeline.arrival_detection import ArrivalDetector
from pipeline.point_calculator import DefecturaAnalyzer
from pipeline.metrics import MetricsCalculator
from pipeline.export import ResultExporter
from pipeline.visualization import Visualizer
