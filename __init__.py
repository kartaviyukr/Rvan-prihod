"""
Пакет для анализа дефектуры товаров

Основное использование:
    from defectura_analysis import analyze_last_point, analyze_episodes
    
    # Анализ последней точки
    result = analyze_last_point(df, export=True, visualize=True)
    
    # Анализ эпизодов за 90 дней
    result = analyze_episodes(df, lookback_days=90, export=True)

Расширенное использование:
    from defectura_analysis import DefecturaAnalyzer, Config
    
    # Кастомная конфигурация
    config = Config()
    config.DELTA_ARRIVAL = 150
    config.MIN_OBS_LAST30 = 25
    
    analyzer = DefecturaAnalyzer(df, config)
    result = analyzer.analyze_last_point()
    result = analyzer.analyze_episodes(lookback_days=60)
"""

from .config import Config, CategoryConfig
from .main import DefecturaAnalyzer, analyze_last_point, analyze_episodes

__version__ = '1.0.0'

__all__ = [
    'Config',
    'CategoryConfig',
    'DefecturaAnalyzer',
    'analyze_last_point',
    'analyze_episodes',
]
