"""
Скрипт запуска анализа дефектуры
Запуск: python run.py
"""
import pandas as pd
from third_block_process.main import analyze_last_point, analyze_episodes

def main():
    # Загрузите ваши данные
    print("Загрузка данных...")
    df = pd.read_excel('путь/к/вашим/данным.xlsx')
    
    # Вариант 1: Анализ последней точки
    print("\n" + "="*60)
    print("АНАЛИЗ ПОСЛЕДНЕЙ ТОЧКИ")
    print("="*60)
    
    result = analyze_last_point(
        df, 
        export=True,
        visualize=False  # True если нужны графики
    )
    
    print(f"\n✅ Найдено КАГ в дефектуре: {len(result)}")
    
    # Вариант 2: Анализ эпизодов (опционально)
    # result_eps = analyze_episodes(df, lookback_days=90, export=True)
    # print(f"\n✅ Найдено эпизодов: {len(result_eps)}")

if __name__ == '__main__':
    main()