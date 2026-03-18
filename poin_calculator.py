"""
Главный модуль для анализа дефектуры товаров

Предоставляет два основных режима анализа:
1. Последняя точка - КАГ в дефектуре на последней дате
2. Эпизоды - все периоды дефектуры за заданный период
"""
import pandas as pd
from typing import Optional, Tuple
import sys
from pathlib import Path

# Добавляем родительскую папку в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

# Импорты - теперь будут работать
from config import Config
from data_preparation import DataPreparator
from defectura_detection import DefecturaDetector
from arrival_detection import ArrivalDetector
from metrics import MetricsCalculator
from export import ResultExporter
from visualization import Visualizer


class DefecturaAnalyzer:
    """
    Основной класс для анализа дефектуры
    
    Использование:
        analyzer = DefecturaAnalyzer(df)
        result = analyzer.analyze_last_point()
        # или
        result = analyzer.analyze_episodes()
    """
    
    def __init__(self, df: pd.DataFrame, config: Config = Config):
        """
        Args:
            df: Исходный датафрейм с данными
            config: Конфигурация (опционально)
        """
        self.config = config
        
        # Инициализируем компоненты
        self.preparator = DataPreparator(config)
        self.defect_detector = DefecturaDetector(config)
        self.arrival_detector = ArrivalDetector(config)
        self.metrics_calc = MetricsCalculator(config)
        self.exporter = ResultExporter(config)
        self.visualizer = Visualizer(config)
        
        # Подготавливаем данные
        print("🔧 Подготовка данных...")
        self.data, self.work_date = self.preparator.validate_and_prepare(df)
        
        # Фильтруем подходящие КАГ
        self.eligible_kags = self.defect_detector.filter_eligible_kags(
            self.data, 
            self.work_date
        )
        
        # Кэш для результатов
        self._kag_store = None
        self._last_point_result = None
        self._episodes_result = None
    
    def analyze_last_point(
        self,
        export: bool = True,
        visualize: bool = False
    ) -> pd.DataFrame:
        """
        Анализ КАГ в дефектуре на последней дате наблюдений
        
        Args:
            export: Экспортировать результаты в Excel/Parquet
            visualize: Построить графики
            
        Returns:
            DataFrame с результатами анализа
        """
        if self._last_point_result is not None:
            print("ℹ️  Используем кэшированный результат")
            return self._last_point_result
        
        print("\n" + "="*60)
        print("РЕЖИМ: АНАЛИЗ ПОСЛЕДНЕЙ ТОЧКИ")
        print("="*60)
        
        # 1. Находим КАГ в дефектуре на последней дате
        defects_last = self.defect_detector.find_last_point_defects(
            self.data,
            self.eligible_kags
        )
        
        if defects_last.empty:
            print("⚠️  КАГ в дефектуре не найдено")
            return pd.DataFrame()
        
        # 2. Добавляем метаданные и категории
        result = self.metrics_calc.add_categories_and_metadata(
            defects_last,
            self.data,
            self.work_date
        )
        
        # 3. Детальная статистика прихода
        arrival_stats = self.metrics_calc.add_detailed_arrival_stats(
            result,
            self.data,
            self.work_date
        )

        # 4. Остатки на вчера и последнюю дату
        stock_snapshots = self.metrics_calc.add_stock_snapshots(
            result,
            self.data,
            self.work_date
        )

        # 5. Объединяем всё
        if not arrival_stats.empty and self.config.COL_KAG in arrival_stats.columns:
            result = result.merge(arrival_stats, on=self.config.COL_KAG, how='left')
        
        if not stock_snapshots.empty and self.config.COL_KAG in stock_snapshots.columns:
            result = result.merge(stock_snapshots, on=self.config.COL_KAG, how='left')
        
        # 6. Детектим первый приход (для совместимости со старой версией)
        result = self._add_first_arrival_metrics(result)
        
        # Кэшируем
        self._last_point_result = result
        
        # 7. Экспорт
        if export:
            self.exporter.export_final_table(
                result,
                'final_table_last_point',
                self.work_date
            )
        
        # 8. Визуализация
        if visualize:
            self.visualizer.plot_stocks_for_kags(
                self.data,
                result[self.config.COL_KAG].unique().tolist()
            )
        
        print(f"\n✅ Анализ завершён. Найдено КАГ: {len(result):,}")
        
        return result
    
    def analyze_episodes(
        self,
        lookback_days: int = None,
        export: bool = True,
        visualize: bool = False
    ) -> pd.DataFrame:
        """
        Анализ всех эпизодов дефектуры за период
        
        Args:
            lookback_days: Сколько дней назад искать эпизоды
            export: Экспортировать результаты
            visualize: Построить графики
            
        Returns:
            DataFrame с эпизодами дефектуры
        """
        if self._episodes_result is not None:
            print("ℹ️  Используем кэшированный результат")
            return self._episodes_result
        
        print("\n" + "="*60)
        print("РЕЖИМ: АНАЛИЗ ЭПИЗОДОВ ДЕФЕКТУРЫ")
        print("="*60)
        
        # 1. Строим numpy-массивы для быстрого доступа
        if self._kag_store is None:
            print("🔧 Подготовка numpy-массивов...")
            self._kag_store = self.preparator.build_kag_store(
                self.data,
                self.eligible_kags
            )
        
        # 2. Находим все эпизоды дефектуры
        episodes = self.defect_detector.find_defectura_episodes(
            self._kag_store,
            self.eligible_kags,
            self.work_date,
            lookback_days
        )
        
        if episodes.empty:
            print("⚠️  Эпизодов не найдено")
            return pd.DataFrame()
        
        # 3. Обогащаем метриками прихода
        episodes_with_arrival = self.metrics_calc.enrich_with_arrival_metrics(
            episodes,
            self._kag_store,
            self.work_date
        )
        
        # Фильтруем только эпизоды с приходом
        result = episodes_with_arrival[
            episodes_with_arrival['arrival_flag'] == True
        ].copy()
        
        if result.empty:
            print("⚠️  Эпизодов с приходом не найдено")
            return pd.DataFrame()
        
        # 4. Добавляем метаданные
        result = self.metrics_calc.add_categories_and_metadata(
            result,
            self.data,
            self.work_date
        )
        
        # 5. Детальная статистика - ДЛЯ ЭПИЗОДОВ используем defect_start_date
        # Создаём временный DataFrame с нужной структурой
        temp_for_stats = result.copy()
        temp_for_stats['date_n'] = pd.to_datetime(temp_for_stats['defect_start_date']).dt.normalize()
        
        arrival_stats = self.metrics_calc.add_detailed_arrival_stats(
            temp_for_stats,
            self.data,
            self.work_date
        )
        
        stock_snapshots = self.metrics_calc.add_stock_snapshots(
            result,
            self.data,
            self.work_date
        )
        
        # Объединяем данные
        # Для эпизодов нужен составной ключ КАГ + дата старта
        if not arrival_stats.empty:
            # Создаём уникальный ключ для каждого эпизода
            result['_merge_key'] = (
                result[self.config.COL_KAG].astype(str) + '|' +
                pd.to_datetime(result['defect_start_date']).dt.strftime('%Y-%m-%d')
            )
            
            temp_for_stats['_merge_key'] = (
                temp_for_stats[self.config.COL_KAG].astype(str) + '|' +
                pd.to_datetime(temp_for_stats['defect_start_date']).dt.strftime('%Y-%m-%d')
            )
            
            # Добавляем ключ в arrival_stats
            arrival_stats = arrival_stats.merge(
                temp_for_stats[[self.config.COL_KAG, '_merge_key']].drop_duplicates(),
                on=self.config.COL_KAG,
                how='left'
            )
            
            # Объединяем
            result = result.merge(
                arrival_stats,
                on='_merge_key',
                how='left',
                suffixes=('', '_arr')
            )
            
            # Убираем дубли КАГ колонок
            if f'{self.config.COL_KAG}_arr' in result.columns:
                result = result.drop(columns=[f'{self.config.COL_KAG}_arr'])
        
        if not stock_snapshots.empty:
            result = result.merge(
                stock_snapshots,
                on=self.config.COL_KAG,
                how='left',
                suffixes=('', '_stock')
            )
            
            # Убираем дубли КАГ колонок
            if f'{self.config.COL_KAG}_stock' in result.columns:
                result = result.drop(columns=[f'{self.config.COL_KAG}_stock'])
        
        # Убираем служебные колонки
        result = result.drop(columns=['_merge_key'], errors='ignore')
        
        # Убираем все дубли колонок
        dup_cols = [c for c in result.columns if c.endswith('_arr') or c.endswith('_stock')]
        for dup_col in dup_cols:
            base_col = dup_col.replace('_arr', '').replace('_stock', '')
            if base_col in result.columns:
                result[base_col] = result[dup_col].combine_first(result[base_col])
        result = result.drop(columns=dup_cols, errors='ignore')
        
        # Красивые названия конкурентов
        if 'arrival_competitor' in result.columns:
            result['Конкурент (первый приход)'] = result['arrival_competitor'].apply(
                self._prettify_competitors
            )
        
        # Переименовываем для совместимости
        result = result.rename(columns={
            'defect_start_date': 'Дата входа в дефектуру ГК',
            'defect_end_date': 'Дата выхода из дефектуры ГК',
            'arrival_first_date': 'Дата прихода у конкурента',
        })
        
        # Кэшируем
        self._episodes_result = result
        
        # 6. Экспорт
        if export:
            self.exporter.export_final_table(
                result,
                'final_table_episodes',
                self.work_date
            )
        
        # 7. Визуализация
        if visualize:
            self.visualizer.plot_stocks_for_kags(
                self.data,
                result[self.config.COL_KAG].unique().tolist()
            )
        
        print(f"\n✅ Анализ завершён. Найдено эпизодов с приходом: {len(result):,}")
        
        return result
        
    def _add_first_arrival_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
            """Добавляет метрики первого прихода (для режима последней точки)"""
            first_dates = []
            first_comps = []
            
            for _, row in df.iterrows():
                arrivals = {}
                for comp_col in self.config.COMP_COLS:
                    pretty = self.config.COMP_PRETTY.get(comp_col, comp_col)
                    events_str = row.get(f'Приходы {pretty} (дата-объём)', '0')
                    
                    # Защита от NaN/float
                    if pd.isna(events_str):
                        events_str = '0'
                    else:
                        events_str = str(events_str).strip()
                    
                    if events_str and events_str != '0' and events_str.lower() != 'nan':
                        parts = events_str.split(';')
                        if parts:
                            first_part = parts[0].strip()
                            if ' - ' in first_part:
                                date_str = first_part.split(' - ')[0]
                                try:
                                    date = pd.to_datetime(date_str, format='%d.%m.%y')
                                    arrivals[comp_col] = date
                                except:
                                    pass
                
                if arrivals:
                    min_date = min(arrivals.values())
                    comps = [c for c, d in arrivals.items() if d == min_date]
                    first_dates.append(min_date)
                    first_comps.append('; '.join([
                        self.config.COMP_PRETTY.get(c, c) for c in comps
                    ]))
                else:
                    first_dates.append(pd.NaT)
                    first_comps.append(None)
            
            df['Дата прихода у конкурента'] = first_dates
            df['Конкурент (первый приход)'] = first_comps
            
            if 'Дата входа в дефектуру ГК' in df.columns:
                df['Лаг реакции, дней'] = (
                    pd.to_datetime(df['Дата прихода у конкурента']) -
                    pd.to_datetime(df['Дата входа в дефектуру ГК'])
                ).dt.days
            
            return df
    
    def _prettify_competitors(self, x) -> str:
        """Преобразует названия конкурентов в красивый формат"""
        if pd.isna(x) or x is None or str(x).strip() == '':
            return '0'
        
        parts = [p.strip() for p in str(x).split(';')]
        return '; '.join([
            self.config.COMP_PRETTY.get(p, p) for p in parts if p
        ]) or '0'
    
    def export_wide_tables(self):
        """Экспортирует wide-таблицы с историей остатков"""
        print("\n" + "="*60)
        print("ЭКСПОРТ WIDE-ТАБЛИЦ")
        print("="*60)
        
        if self._last_point_result is not None:
            kags = self._last_point_result[self.config.COL_KAG].unique()
            data_subset = self.data[self.data[self.config.COL_KAG].isin(kags)]
            
            self.exporter.export_wide_table(
                data_subset,
                self.config.COL_KAG,
                self.config.COL_GK,
                'wide_gk_last_point'
            )
            
            self.exporter.export_wide_table(
                data_subset,
                self.config.COL_KAG,
                'sum_competitors',
                'wide_competitors_last_point'
            )
        
        if self._episodes_result is not None:
            kags = self._episodes_result[self.config.COL_KAG].unique()
            data_subset = self.data[self.data[self.config.COL_KAG].isin(kags)]
            
            self.exporter.export_wide_table(
                data_subset,
                self.config.COL_KAG,
                'sum_competitors',
                'wide_competitors_episodes'
            )
        
        print("="*60 + "\n")


def analyze_last_point(
    df: pd.DataFrame,
    export: bool = True,
    visualize: bool = False,
    config: Config = Config
) -> pd.DataFrame:
    """Быстрый анализ последней точки"""
    analyzer = DefecturaAnalyzer(df, config)
    return analyzer.analyze_last_point(export=export, visualize=visualize)


def analyze_episodes(
    df: pd.DataFrame,
    lookback_days: int = None,
    export: bool = True,
    visualize: bool = False,
    config: Config = Config
) -> pd.DataFrame:
    """Быстрый анализ эпизодов"""
    analyzer = DefecturaAnalyzer(df, config)
    return analyzer.analyze_episodes(
        lookback_days=lookback_days,
        export=export,
        visualize=visualize
    )


# ЗАПУСК НАПРЯМУЮ
if __name__ == '__main__':
    
    # ============ ИЗМЕНИТЕ ПУТЬ К ВАШИМ ДАННЫМ ============
    DATA_FILE = r'C:\Проекты\Project_etl_power_bi\data\preproc_parquet\big_data_clean.parquet'
    # =======================================================
    
    print("="*60)
    print("АНАЛИЗ ДЕФЕКТУРЫ")
    print("="*60)
    
    try:
        print(f"\n📂 Загрузка: {DATA_FILE}")
        
        # Автоопределение формата
        if DATA_FILE.endswith('.parquet'):
            df = pd.read_parquet(DATA_FILE)
        elif DATA_FILE.endswith('.csv'):
            df = pd.read_csv(DATA_FILE)
        elif DATA_FILE.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(DATA_FILE)
        else:
            raise ValueError(f"Неподдерживаемый формат: {DATA_FILE}")
        
        print(f"✅ Загружено: {len(df):,} строк\n")
        
        # ============================================================
        # РЕЖИМ 1: АКТИВНЫЕ ДЕФЕКТУРЫ (СЕЙЧАС)
        # ============================================================
        print("\n" + "="*60)
        print("РЕЖИМ 1: ТЕКУЩИЕ ДЕФЕКТУРЫ (последняя точка)")
        print("="*60)
        
        result_last = analyze_last_point(df, export=True, visualize=False)
        
        print(f"\n✅ РЕЖИМ 1 ГОТОВ!")
        print(f"   КАГ в дефектуре сейчас: {len(result_last):,}")
        print(f"   📁 final_table_last_point.xlsx")
        
        # ============================================================
        # РЕЖИМ 2: ВСЕ ЭПИЗОДЫ ДЕФЕКТУРЫ (активные + завершившиеся)
        # ============================================================
        print("\n" + "="*60)
        print("РЕЖИМ 2: ЭПИЗОДЫ ДЕФЕКТУРЫ (за 90 дней)")
        print("="*60)
        
        result_episodes = analyze_episodes(
            df, 
            lookback_days=90,  # Ищем эпизоды за последние 90 дней
            export=True, 
            visualize=False
        )
        
        print(f"\n✅ РЕЖИМ 2 ГОТОВ!")
        print(f"   Эпизодов с приходом: {len(result_episodes):,}")
        if len(result_episodes) > 0:
            active = result_episodes[result_episodes['Статус дефектуры'] == 'Активная']
            finished = result_episodes[result_episodes['Статус дефектуры'] == 'Закончившаяся']
            print(f"   - Активных: {len(active):,}")
            print(f"   - Завершившихся: {len(finished):,}")
        print(f"   📁 final_table_episodes.xlsx")
        
        # ============================================================
        # ИТОГО
        # ============================================================
        print("\n" + "="*60)
        print("✅ ВСЁ ГОТОВО!")
        print("="*60)
        print(f"\n📁 Результаты в: {Config.OUT_DIR}")
        print(f"\n1️⃣  final_table_last_point.xlsx")
        print(f"    Текущие дефектуры: {len(result_last):,} КАГ")
        print(f"\n2️⃣  final_table_episodes.xlsx")
        print(f"    Эпизоды дефектуры: {len(result_episodes):,} случаев")
        
        # ============================================================
        # АНАЛИЗ ПЕРЕСЕЧЕНИЙ
        # ============================================================
        if len(result_last) > 0 and len(result_episodes) > 0:
            print("\n" + "="*60)
            print("АНАЛИЗ ПЕРЕСЕЧЕНИЙ")
            print("="*60)
            
            # КАГ из обоих файлов
            kags_last = set(result_last[Config.COL_KAG].astype(str).unique())
            kags_episodes = set(result_episodes[Config.COL_KAG].astype(str).unique())
            
            # Пересечение
            intersection = kags_last & kags_episodes
            only_in_last = kags_last - kags_episodes
            only_in_episodes = kags_episodes - kags_last
            
            print(f"\n📊 Общая статистика:")
            print(f"   КАГ в last_point: {len(kags_last):,}")
            print(f"   КАГ в episodes: {len(kags_episodes):,}")
            print(f"   ├─ Пересечение: {len(intersection):,} КАГ")
            print(f"   ├─ Только в last_point: {len(only_in_last):,} КАГ")
            print(f"   └─ Только в episodes: {len(only_in_episodes):,} КАГ")
            
            # Детальный анализ
            if len(intersection) > 0:
                print(f"\n✅ Пересечение ({len(intersection)} КАГ):")
                print(f"   Это КАГ, которые:")
                print(f"   - Сейчас в дефектуре (last_point)")
                print(f"   - И имели эпизод(ы) дефектуры за 90 дней (episodes)")
                
                # Показываем примеры
                sample = list(intersection)[:5]
                if sample:
                    print(f"\n   Примеры КАГ:")
                    for kag in sample:
                        kag_name = result_last[result_last[Config.COL_KAG].astype(str) == kag][Config.COL_KAG_NAME].iloc[0] if Config.COL_KAG_NAME in result_last.columns else ''
                        episodes_count = len(result_episodes[result_episodes[Config.COL_KAG].astype(str) == kag])
                        print(f"      {kag} ({kag_name}) - эпизодов: {episodes_count}")
            
            if len(only_in_last) > 0:
                print(f"\n⚠️  Только в last_point ({len(only_in_last)} КАГ):")
                print(f"   Это КАГ, которые:")
                print(f"   - Сейчас в дефектуре")
                print(f"   - Но НЕ имели приходов у конкурентов за последние 90 дней")
                print(f"   - Возможные причины:")
                print(f"     • Дефектура началась давно (>90 дней)")
                print(f"     • Приходы были слишком маленькие (не прошли фильтр)")
                print(f"     • У конкурентов не было приходов")
                
                # Показываем примеры
                sample = list(only_in_last)[:5]
                if sample:
                    print(f"\n   Примеры КАГ:")
                    for kag in sample:
                        kag_data = result_last[result_last[Config.COL_KAG].astype(str) == kag].iloc[0]
                        kag_name = kag_data.get(Config.COL_KAG_NAME, '')
                        last_date = kag_data.get('Последняя дата КАГ', '')
                        print(f"      {kag} ({kag_name}) - последняя дата: {last_date}")
            
            if len(only_in_episodes) > 0:
                print(f"\n🔄 Только в episodes ({len(only_in_episodes)} КАГ):")
                print(f"   Это КАГ, которые:")
                print(f"   - Имели эпизод(ы) дефектуры с приходом за 90 дней")
                print(f"   - Но СЕЙЧАС не в дефектуре (ГК восстановлен)")
                
                # Анализируем - сколько из них закончившиеся
                only_ep_df = result_episodes[result_episodes[Config.COL_KAG].astype(str).isin(only_in_episodes)]
                if 'Статус дефектуры' in only_ep_df.columns:
                    finished = only_ep_df[only_ep_df['Статус дефектуры'] == 'Закончившаяся']
                    active = only_ep_df[only_ep_df['Статус дефектуры'] == 'Активная']
                    print(f"     • Закончившихся эпизодов: {len(finished):,}")
                    print(f"     • Активных эпизодов: {len(active):,}")
                
                # Показываем примеры
                sample = list(only_in_episodes)[:5]
                if sample:
                    print(f"\n   Примеры КАГ:")
                    for kag in sample:
                        kag_data = only_ep_df[only_ep_df[Config.COL_KAG].astype(str) == kag].iloc[0]
                        kag_name = kag_data.get(Config.COL_KAG_NAME, '')
                        status = kag_data.get('Статус дефектуры', '')
                        print(f"      {kag} ({kag_name}) - {status}")
            
            # Сохраняем анализ в файл
            analysis_file = Config.OUT_DIR / 'analysis_intersection.txt'
            with open(analysis_file, 'w', encoding='utf-8') as f:
                f.write("="*60 + "\n")
                f.write("АНАЛИЗ ПЕРЕСЕЧЕНИЙ МЕЖДУ ФАЙЛАМИ\n")
                f.write("="*60 + "\n\n")
                
                f.write(f"Дата анализа: {pd.Timestamp.now().strftime('%d.%m.%Y %H:%M')}\n\n")
                
                f.write("ОБЩАЯ СТАТИСТИКА:\n")
                f.write(f"  КАГ в last_point: {len(kags_last):,}\n")
                f.write(f"  КАГ в episodes: {len(kags_episodes):,}\n")
                f.write(f"  Пересечение: {len(intersection):,}\n")
                f.write(f"  Только в last_point: {len(only_in_last):,}\n")
                f.write(f"  Только в episodes: {len(only_in_episodes):,}\n\n")
                
                if only_in_last:
                    f.write("ТОЛЬКО В LAST_POINT (текущие дефектуры без приходов):\n")
                    for kag in sorted(only_in_last):
                        kag_data = result_last[result_last[Config.COL_KAG].astype(str) == kag].iloc[0]
                        kag_name = kag_data.get(Config.COL_KAG_NAME, '')
                        f.write(f"  {kag} - {kag_name}\n")
                    f.write("\n")
                
                if only_in_episodes:
                    f.write("ТОЛЬКО В EPISODES (завершённые дефектуры):\n")
                    for kag in sorted(only_in_episodes):
                        kag_data = only_ep_df[only_ep_df[Config.COL_KAG].astype(str) == kag].iloc[0]
                        kag_name = kag_data.get(Config.COL_KAG_NAME, '')
                        status = kag_data.get('Статус дефектуры', '')
                        f.write(f"  {kag} - {kag_name} ({status})\n")
            
            print(f"\n📄 Детальный анализ сохранён: {analysis_file}")
        
        print()
        
    except FileNotFoundError:
        print(f"\n❌ ФАЙЛ НЕ НАЙДЕН: {DATA_FILE}")
        print("💡 Измените DATA_FILE в main.py\n")
    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}\n")
        import traceback
        traceback.print_exc()