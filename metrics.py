"""
Расчёт метрик и статистики для отчётов
"""
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple
from tqdm.auto import tqdm
from config import Config, CategoryConfig
from arrival_detection import ArrivalDetector
from defectura_detection import DefecturaDetector


class MetricsCalculator:
    """Класс для расчёта метрик и обогащения данных"""
    
    def __init__(self, config: Config = Config):
        self.config = config
        self.arrival_detector = ArrivalDetector(config)
        self.defect_detector = DefecturaDetector(config)
    
    def enrich_with_arrival_metrics(
        self,
        episodes: pd.DataFrame,
        kag_store: dict,
        work_date: pd.Timestamp
    ) -> pd.DataFrame:
        """
        Обогащает эпизоды дефектуры метриками прихода
        
        Args:
            episodes: DataFrame с эпизодами дефектуры
            kag_store: Предподготовленные numpy-массивы
            work_date: Рабочая дата
            
        Returns:
            Обогащённый DataFrame
        """
        rows = []
        
        for _, episode in tqdm(
            episodes.iterrows(), 
            total=len(episodes),
            desc="▶ Расчёт метрик прихода"
        ):
            kag = episode[self.config.COL_KAG]
            start_date = pd.Timestamp(episode['defect_start_date']).normalize()
            end_date = episode['defect_end_date'] if pd.notna(episode['defect_end_date']) else None
            
            st = kag_store.get(kag)
            if st is None:
                continue
            
            # Детектим приход в окне эпизода
            metrics = self.arrival_detector.detect_from_numpy_store(
                st, start_date, end_date, work_date
            )
            
            # Объединяем с данными эпизода
            row = {
                self.config.COL_KAG: kag,
                'defect_start_date': start_date,
                'defect_end_date': pd.Timestamp(end_date).normalize() if end_date else pd.NaT,
                'is_finished': bool(episode['is_finished']),
                **metrics
            }
            
            rows.append(row)
        
        if not rows:
            return pd.DataFrame()
        
        result = pd.DataFrame(rows)
        
        print(f"\n✅ Метрики рассчитаны для {len(result):,} эпизодов")
        print(f"   эпизодов с приходом: {result['arrival_flag'].sum():,}")
        
        return result
    
    def add_detailed_arrival_stats(
        self,
        base_df: pd.DataFrame,
        full_data: pd.DataFrame,
        work_date: pd.Timestamp
    ) -> pd.DataFrame:
        """
        Добавляет детальную статистику прихода для каждого конкурента
        
        Args:
            base_df: Базовый DataFrame (с эпизодами или последними точками)
            full_data: Полные данные
            work_date: Рабочая дата
            
        Returns:
            DataFrame с детальной статистикой
        """
        kags_to_process = base_df[self.config.COL_KAG].unique()
        kag2df = {k: g for k, g in full_data.groupby(self.config.COL_KAG, sort=False)}
        
        rows = []
        
        for _, rec in tqdm(
            base_df.iterrows(),
            total=len(base_df),
            desc="▶ Детальная статистика прихода"
        ):
            kag = rec[self.config.COL_KAG]
            one = kag2df.get(str(kag))
            
            if one is None or one.empty:
                continue
            
            # Персональный порог для этого КАГ
            threshold = self.defect_detector._get_threshold(kag)
            
            # Ищем последнюю дату, когда ГК был >= порога (т.е. НЕ в дефектуре)
            one_sorted = one.sort_values('date_n')
            pos_gk = one_sorted[one_sorted[self.config.COL_GK] >= threshold]
            
            if pos_gk.empty:
                # У ГК никогда не было остатков выше порога - пропускаем
                continue
            
            # Дата входа в дефектуру = первый день после последнего ГК >= порога
            last_pos_date = pos_gk['date_n'].max()
            
            # Находим первую дату ПОСЛЕ last_pos_date (это и есть дата входа в дефектуру)
            after_pos = one_sorted[one_sorted['date_n'] > last_pos_date]
            
            if after_pos.empty:
                # Нет данных после последнего положительного ГК
                continue
            
            deficit_start_date = after_pos['date_n'].min()
            end_date = one['date_n'].max()
            
            # Получаем события прихода по каждому конкуренту
            # Ищем приходы В ПЕРИОД ДЕФЕКТУРЫ (от deficit_start_date до end_date)
            comp_events = self.arrival_detector.detect_for_all_competitors(
                one, deficit_start_date, end_date
            )
            
            # Формируем запись
            row = {self.config.COL_KAG: kag}
            
            # Общая статистика
            total_events = sum(len(events) for events in comp_events.values())
            total_volume = sum(
                sum(vol for _, vol in events) 
                for events in comp_events.values()
            )
            
            row['Приходов после дефектуры (всего)'] = int(total_events)
            row['Общий объём прихода после дефектуры'] = float(total_volume)
            
            # Конкуренты с приходами
            comps_with_arrivals = list(comp_events.keys())
            row['Кол-во конкурентов с приходами'] = len(comps_with_arrivals)
            row['Конкуренты с приходами'] = '; '.join([
                self.config.COMP_PRETTY.get(c, c) for c in comps_with_arrivals
            ]) if comps_with_arrivals else '0'
            
            # Детали по каждому конкуренту
            for comp_col in self.config.COMP_COLS:
                pretty_name = self.config.COMP_PRETTY.get(comp_col, comp_col)
                events = comp_events.get(comp_col, [])
                
                # Форматированная строка "дата - объём"
                row[f'Приходы {pretty_name} (дата-объём)'] = self._format_events(events)
                
                # Суммарный объём
                row[f'Объём прихода {pretty_name} (сумма)'] = sum(vol for _, vol in events)
            
            rows.append(row)
        
        return pd.DataFrame(rows)
    
    def add_stock_snapshots(
    self,
    base_df: pd.DataFrame,
    full_data: pd.DataFrame,
    work_date: pd.Timestamp
    ) -> pd.DataFrame:
        """
        Добавляет срезы остатков (на вчера и на последнюю дату КАГ)
        
        Returns:
            DataFrame с остатками
        """
        yesterday = (work_date - pd.Timedelta(days=1)).normalize()
        kags = base_df[self.config.COL_KAG].unique()
        kag2df = {k: g for k, g in full_data.groupby(self.config.COL_KAG, sort=False)}
        
        rows = []
        
        for kag in kags:
            one = kag2df.get(str(kag))
            if one is None or one.empty:
                continue
            
            row = {self.config.COL_KAG: kag}
            
            # Остатки на вчера
            row['Дата остатков конкурентов (вчера)'] = yesterday
            for comp_col in self.config.COMP_COLS:
                pretty = self.config.COMP_PRETTY.get(comp_col, comp_col)
                row[f'Остаток {pretty} (вчера)'] = self._get_value_on_date(
                    one, comp_col, yesterday
                )
            
            # Остатки на последнюю дату КАГ
            last_date = one['date_n'].max()
            row['Последняя дата (КАГ)'] = last_date
            
            for comp_col in self.config.COMP_COLS:
                pretty = self.config.COMP_PRETTY.get(comp_col, comp_col)
                row[f'Остаток {pretty} (последняя дата)'] = self._get_value_on_date(
                    one, comp_col, last_date
                )
            
            # ГК на последнюю дату
            row['Остаток ГК (последняя дата)'] = self._get_value_on_date(
                one, self.config.COL_GK, last_date
            )
            
            rows.append(row)
        
        return pd.DataFrame(rows)
    
    def add_categories_and_metadata(
        self,
        df: pd.DataFrame,
        full_data: pd.DataFrame,
        work_date: pd.Timestamp
    ) -> pd.DataFrame:
        """
        Добавляет категории, названия, статусы
        """
        result = df.copy()
        
        # Названия КАГ
        if self.config.COL_KAG_NAME in full_data.columns:
            name_map = full_data.groupby(self.config.COL_KAG)[
                self.config.COL_KAG_NAME
            ].first().to_dict()
            result[self.config.COL_KAG_NAME] = result[self.config.COL_KAG].map(name_map).fillna('')
        
        # Последняя дата КАГ
        last_date_map = full_data.groupby(self.config.COL_KAG)['date_n'].max().to_dict()
        result['Последняя дата КАГ'] = result[self.config.COL_KAG].map(last_date_map)
        
        # Категория дефектуры
        # Дата входа в дефектуру и категория
        if 'defect_start_date' in result.columns:
            # Для эпизодов - уже есть
            result['Дата входа в дефектуру ГК'] = pd.to_datetime(result['defect_start_date']).dt.normalize()
            days_ago = (work_date - pd.to_datetime(result['defect_start_date'])).dt.days.fillna(10**9)
            result['Категория'] = [
                CategoryConfig.categorize_by_days_ago(int(d)) for d in days_ago
            ]
        else:
            # Для last_point - нужно найти последнюю дату с ГК >= порога
            kag2df_temp = {k: g for k, g in full_data.groupby(self.config.COL_KAG, sort=False)}
            
            deficit_dates = []
            for _, row in result.iterrows():
                kag = str(row[self.config.COL_KAG])
                one = kag2df_temp.get(kag)
                
                if one is None or one.empty:
                    deficit_dates.append(pd.NaT)
                    continue
                
                # Персональный порог для этого КАГ
                threshold = self.defect_detector._get_threshold(kag)
                
                # Ищем последнюю дату с ГК >= порога (НЕ в дефектуре)
                pos_gk = one[one[self.config.COL_GK] >= threshold].sort_values('date_n')
                
                if pos_gk.empty:
                    deficit_dates.append(pd.NaT)
                    continue
                
                last_pos_date = pos_gk['date_n'].max()
                
                # Первая дата ПОСЛЕ последнего ГК >= порога
                after_pos = one[one['date_n'] > last_pos_date].sort_values('date_n')
                
                if after_pos.empty:
                    deficit_dates.append(pd.NaT)
                else:
                    deficit_dates.append(after_pos['date_n'].min())
            
            result['Дата входа в дефектуру ГК'] = deficit_dates
            
            # Категория на основе вычисленной даты
            days_ago = (work_date - pd.to_datetime(result['Дата входа в дефектуру ГК'])).dt.days.fillna(10**9)
            result['Категория'] = [
                CategoryConfig.categorize_by_days_ago(int(d)) for d in days_ago
            ]
        
        # Статус дефектуры
        if 'is_finished' in result.columns:
            result['Статус дефектуры'] = np.where(
                result['is_finished'], 
                'Закончившаяся', 
                'Активная'
            )
        
        # Длительность дефектуры
        if 'defect_start_date' in result.columns and 'defect_end_date' in result.columns:
            result['Длительность дефектуры, дней'] = np.where(
                result['is_finished'] & result['defect_end_date'].notna(),
                (pd.to_datetime(result['defect_end_date']) - 
                 pd.to_datetime(result['defect_start_date'])).dt.days,
                (work_date - pd.to_datetime(result['defect_start_date'])).dt.days
            )
        
        # Лаг реакции
        if 'arrival_first_date' in result.columns and 'defect_start_date' in result.columns:
            result['Лаг реакции, дней'] = (
                pd.to_datetime(result['arrival_first_date']) - 
                pd.to_datetime(result['defect_start_date'])
            ).dt.days
        
        return result
    
    @staticmethod
    def _format_events(events: List[Tuple[pd.Timestamp, float]]) -> str:
        """Форматирует события в строку 'дд.мм.гг - N шт'"""
        if not events:
            return '0'
        
        # Группируем по дате (может быть несколько событий в день)
        by_date = {}
        for dt, vol in events:
            if pd.isna(dt):
                continue
            dt = pd.to_datetime(dt).normalize()
            by_date[dt] = by_date.get(dt, 0.0) + float(vol)
        
        if not by_date:
            return '0'
        
        parts = [
            f"{dt.strftime('%d.%m.%y')} - {int(round(vol))} шт"
            for dt, vol in sorted(by_date.items())
        ]
        
        return '; '.join(parts)
    
    @staticmethod
    def _get_value_on_date(
        df: pd.DataFrame, 
        column: str, 
        target_date: pd.Timestamp
    ) -> float:
        """Получает значение колонки на указанную дату"""
        target_n = pd.to_datetime(target_date).normalize()
        mask = df['date_n'] == target_n
        
        if not mask.any():
            return 0.0
        
        return float(
            pd.to_numeric(df.loc[mask, column], errors='coerce')
            .fillna(0)
            .iloc[-1]
        )