"""
Модули ETL-пайплайна анализа дефектуры

Структура:
    extract:   db_loader, check_dates
    transform: preprocess, data_preparation
    analyze:   defectura_detection, arrival_detection, point_calculator,
               episodes_calculator, metrics
    load:      export, excel_process, email_sender, db_uploader, visualization

Импорт — по необходимости из конкретных модулей:
    from pipeline.db_loader import export_ai_stock_to_parquet
    from pipeline.preprocess import base_action
"""
