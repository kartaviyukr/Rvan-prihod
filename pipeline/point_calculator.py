"""
Анализ дефектуры: последняя точка и эпизоды

Объединяет компоненты пайплайна в единый анализатор:
    DataPreparator -> DefecturaDetector -> ArrivalDetector -> MetricsCalculator -> ResultExporter
"""
import pandas as pd
from typing import Optional

from config import Config
from pipeline.data_preparation import DataPreparator
from pipeline.defectura_detection import DefecturaDetector
from pipeline.arrival_detection import ArrivalDetector
from pipeline.metrics import MetricsCalculator
from pipeline.export import ResultExporter
from pipeline.visualization import Visualizer


class DefecturaAnalyzer:
    """
    Основной класс анализа дефектуры.

    Использование:
        analyzer = DefecturaAnalyzer(df)
        result = analyzer.analyze_last_point()
        result = analyzer.analyze_episodes()
    """

    def __init__(self, df: pd.DataFrame, config: Config = Config):
        self.config = config
        self.preparator = DataPreparator(config)
        self.defect_detector = DefecturaDetector(config)
        self.arrival_detector = ArrivalDetector(config)
        self.metrics_calc = MetricsCalculator(config)
        self.exporter = ResultExporter(config)
        self.visualizer = Visualizer(config)

        self.data, self.work_date = self.preparator.validate_and_prepare(df)
        self.eligible_kags = self.defect_detector.filter_eligible_kags(self.data, self.work_date)

        self._kag_store = None
        self._last_point_result = None
        self._episodes_result = None

    def analyze_last_point(self, export: bool = True, visualize: bool = False) -> pd.DataFrame:
        """Анализ КАГ в дефектуре на последней дате"""
        if self._last_point_result is not None:
            return self._last_point_result

        defects_last = self.defect_detector.find_last_point_defects(self.data, self.eligible_kags)
        if defects_last.empty:
            return pd.DataFrame()

        result = self.metrics_calc.add_categories_and_metadata(defects_last, self.data, self.work_date)

        arrival_stats = self.metrics_calc.add_detailed_arrival_stats(result, self.data, self.work_date)
        stock_snapshots = self.metrics_calc.add_stock_snapshots(result, self.data, self.work_date)

        if not arrival_stats.empty and self.config.COL_KAG in arrival_stats.columns:
            result = result.merge(arrival_stats, on=self.config.COL_KAG, how='left')
        if not stock_snapshots.empty and self.config.COL_KAG in stock_snapshots.columns:
            result = result.merge(stock_snapshots, on=self.config.COL_KAG, how='left')

        result = self._add_first_arrival_metrics(result)
        self._last_point_result = result

        if export:
            self.exporter.export_final_table(result, 'final_table_last_point', self.work_date)
        if visualize:
            self.visualizer.plot_stocks_for_kags(self.data, result[self.config.COL_KAG].unique().tolist())

        print(f"Анализ завершен. Найдено КАГ: {len(result):,}")
        return result

    def analyze_episodes(self, lookback_days: int = None,
                         export: bool = True, visualize: bool = False) -> pd.DataFrame:
        """Анализ всех эпизодов дефектуры за период"""
        if self._episodes_result is not None:
            return self._episodes_result

        if self._kag_store is None:
            self._kag_store = self.preparator.build_kag_store(self.data, self.eligible_kags)

        episodes = self.defect_detector.find_defectura_episodes(
            self._kag_store, self.eligible_kags, self.work_date, lookback_days)
        if episodes.empty:
            return pd.DataFrame()

        episodes_with_arrival = self.metrics_calc.enrich_with_arrival_metrics(
            episodes, self._kag_store, self.work_date)

        result = episodes_with_arrival[episodes_with_arrival['arrival_flag'] == True].copy()
        if result.empty:
            return pd.DataFrame()

        result = self.metrics_calc.add_categories_and_metadata(result, self.data, self.work_date)
        stock_snapshots = self.metrics_calc.add_stock_snapshots(result, self.data, self.work_date)

        if not stock_snapshots.empty:
            result = result.merge(stock_snapshots, on=self.config.COL_KAG, how='left',
                                  suffixes=('', '_stock'))
            dup_cols = [c for c in result.columns if c.endswith('_stock')]
            result = result.drop(columns=dup_cols, errors='ignore')

        if 'arrival_competitor' in result.columns:
            result['Конкурент (первый приход)'] = result['arrival_competitor'].apply(
                self._prettify_competitors)

        result = result.rename(columns={
            'defect_start_date': 'Дата входа в дефектуру ГК',
            'defect_end_date': 'Дата выхода из дефектуры ГК',
            'arrival_first_date': 'Дата прихода у конкурента',
        })

        self._episodes_result = result

        if export:
            self.exporter.export_final_table(result, 'final_table_episodes', self.work_date)
        if visualize:
            self.visualizer.plot_stocks_for_kags(self.data, result[self.config.COL_KAG].unique().tolist())

        print(f"Анализ завершен. Эпизодов с приходом: {len(result):,}")
        return result

    def _add_first_arrival_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        first_dates, first_comps = [], []
        for _, row in df.iterrows():
            arrivals = {}
            for comp_col in self.config.COMP_COLS:
                pretty = self.config.COMP_PRETTY.get(comp_col, comp_col)
                events_str = str(row.get(f'Приходы {pretty} (дата-объём)', '0') or '0').strip()
                if events_str and events_str != '0' and events_str.lower() != 'nan':
                    parts = events_str.split(';')
                    if parts:
                        first_part = parts[0].strip()
                        if ' - ' in first_part:
                            try:
                                arrivals[comp_col] = pd.to_datetime(
                                    first_part.split(' - ')[0], format='%d.%m.%y')
                            except (ValueError, TypeError):
                                pass

            if arrivals:
                min_date = min(arrivals.values())
                comps = [c for c, d in arrivals.items() if d == min_date]
                first_dates.append(min_date)
                first_comps.append('; '.join(self.config.COMP_PRETTY.get(c, c) for c in comps))
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
        if pd.isna(x) or not str(x).strip():
            return '0'
        return '; '.join(
            self.config.COMP_PRETTY.get(p.strip(), p.strip())
            for p in str(x).split(';') if p.strip()
        ) or '0'


def analyze_last_point(df, export=True, visualize=False, config=Config):
    return DefecturaAnalyzer(df, config).analyze_last_point(export=export, visualize=visualize)


def analyze_episodes(df, lookback_days=None, export=True, visualize=False, config=Config):
    return DefecturaAnalyzer(df, config).analyze_episodes(
        lookback_days=lookback_days, export=export, visualize=visualize)


if __name__ == '__main__':
    DATA_FILE = str(Config.CLEAN_PARQUET)
    print(f"Загрузка: {DATA_FILE}")

    if DATA_FILE.endswith('.parquet'):
        df = pd.read_parquet(DATA_FILE)
    else:
        df = pd.read_excel(DATA_FILE)

    print(f"Загружено: {len(df):,} строк")

    result_last = analyze_last_point(df, export=True, visualize=False)
    result_episodes = analyze_episodes(df, lookback_days=90, export=True, visualize=False)

    print(f"\nТекущие дефектуры: {len(result_last):,}")
    print(f"Эпизоды: {len(result_episodes):,}")
