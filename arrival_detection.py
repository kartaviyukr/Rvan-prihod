"""
Детекция "прихода" товаров у конкурентов
"""
import numpy as np
import pandas as pd
from typing import List, Tuple, Dict, Optional
from config import Config


class ArrivalDetector:
    """
    Детектор прихода товаров у конкурентов
    
    "Приход" = значительный рост остатков между соседними наблюдениями:
    - delta >= DELTA_ARRIVAL (абсолютный рост)
    - delta >= MIN_PCT_FROM_YESTERDAY * yesterday_stock (относительный рост)
    """
    
    def __init__(self, config: Config = Config):
        self.config = config
    
    def detect_in_window(
        self,
        data: pd.DataFrame,
        competitor_col: str,
        start_date: pd.Timestamp,
        end_date: Optional[pd.Timestamp] = None
    ) -> List[Tuple[pd.Timestamp, float]]:
        """
        Детектирует события прихода для одного конкурента в заданном окне
        
        Args:
            data: Датафрейм с данными (должен быть отсортирован по date_n)
            competitor_col: Название колонки конкурента
            start_date: Начало окна
            end_date: Конец окна (если None - до конца данных)
            
        Returns:
            Список событий [(дата_прихода, объём_прихода)]
        """
        if data.empty or len(data) < 2:
            return []
        
        # Фильтруем окно
        start_n = pd.to_datetime(start_date).normalize()
        end_n = pd.to_datetime(end_date).normalize() if end_date else data['date_n'].max()
        
        window = data[
            (data['date_n'] >= start_n) & (data['date_n'] <= end_n)
        ].copy()
        
        if len(window) < 2:
            return []
        
        # Преобразуем в numpy для скорости
        arr = pd.to_numeric(window[competitor_col], errors='coerce').fillna(0).to_numpy(dtype=float)
        
        # Вычисляем дельты
        prev = arr[:-1]
        delta = np.diff(arr)
        threshold_pct = self.config.MIN_PCT_FROM_YESTERDAY * prev
        
        # Маска прихода
        hit_mask = (delta >= self.config.DELTA_ARRIVAL) & (delta >= threshold_pct)
        hit_indices = np.where(hit_mask)[0]
        
        if len(hit_indices) == 0:
            return []
        
        # Формируем события
        events = []
        for idx in hit_indices:
            arrival_date = pd.to_datetime(window.iloc[idx + 1]['date_n']).normalize()
            arrival_volume = float(delta[idx])
            events.append((arrival_date, arrival_volume))
        
        return events
    
    def detect_for_all_competitors(
        self,
        data: pd.DataFrame,
        start_date: pd.Timestamp,
        end_date: Optional[pd.Timestamp] = None
    ) -> Dict[str, List[Tuple[pd.Timestamp, float]]]:
        """
        Детектирует приход для всех конкурентов
        
        Returns:
            Словарь {competitor_col: [события]}
        """
        results = {}
        
        for comp_col in self.config.COMP_COLS:
            events = self.detect_in_window(data, comp_col, start_date, end_date)
            if events:
                results[comp_col] = events
        
        return results
    
    def detect_from_numpy_store(
        self,
        store: dict,
        start_date: pd.Timestamp,
        end_date: Optional[pd.Timestamp],
        work_date: pd.Timestamp
    ) -> dict:
        """
        Быстрая детекция через предподготовленные numpy массивы
        
        Args:
            store: Словарь с массивами {dates, gk, sumc, comps}
            start_date: Начало окна
            end_date: Конец окна (может быть None)
            work_date: Рабочая дата (для активных дефектур)
            
        Returns:
            Словарь с метриками прихода
        """
        dates = store['dates']
        sumc = store['sumc']
        comps = store['comps']
        
        # Границы окна
        start_np = np.datetime64(pd.to_datetime(start_date).normalize())
        if end_date is None or pd.isna(end_date):
            end_np = np.datetime64(work_date)
        else:
            end_np = np.datetime64(pd.to_datetime(end_date).normalize())
        
        # Используем searchsorted для быстрого поиска границ
        left = np.searchsorted(dates, start_np, side='left')
        right = np.searchsorted(dates, end_np, side='right')
        
        if right - left < 2:
            return self._empty_result()
        
        # Статистика sum_competitors в окне
        sum_slice = sumc[left:right]
        max_sum = float(np.max(sum_slice)) if sum_slice.size else np.nan
        min_sum = float(np.min(sum_slice)) if sum_slice.size else np.nan
        
        # Детектим приход по каждому конкуренту
        first_hit_by_comp = {}
        total_hits = 0
        max_delta_global = None
        
        for comp_col in self.config.COMP_COLS:
            arr = comps[comp_col][left:right]
            
            prev = arr[:-1]
            delta = np.diff(arr)
            threshold_pct = self.config.MIN_PCT_FROM_YESTERDAY * prev
            
            hit_mask = (delta >= self.config.DELTA_ARRIVAL) & (delta >= threshold_pct)
            
            if not np.any(hit_mask):
                continue
            
            hit_idx = np.where(hit_mask)[0]
            total_hits += int(hit_idx.size)
            
            # Максимальная дельта
            local_max = float(np.max(delta[hit_idx]))
            if max_delta_global is None or local_max > max_delta_global:
                max_delta_global = local_max
            
            # Первая дата прихода по этому конкуренту
            j = int(hit_idx[0] + 1)
            hit_date = pd.Timestamp(dates[left + j]).normalize()
            first_hit_by_comp[comp_col] = hit_date
        
        if not first_hit_by_comp:
            return {
                **self._empty_result(),
                'max_sum_comp_in_window': max_sum,
                'min_sum_comp_in_window': min_sum,
            }
        
        # Определяем самую раннюю дату прихода
        min_date = min(first_hit_by_comp.values())
        comps_on_min_date = [c for c, dt in first_hit_by_comp.items() if dt == min_date]
        
        return {
            'arrival_flag': True,
            'arrival_events_cnt': int(total_hits),
            'arrival_first_date': min_date,
            'arrival_competitor': '; '.join(comps_on_min_date),
            'arrival_max_delta': float(max_delta_global) if max_delta_global else np.nan,
            'max_sum_comp_in_window': max_sum,
            'min_sum_comp_in_window': min_sum,
        }
    
    @staticmethod
    def _empty_result() -> dict:
        """Возвращает пустой результат (приход не обнаружен)"""
        return {
            'arrival_flag': False,
            'arrival_events_cnt': 0,
            'arrival_first_date': pd.NaT,
            'arrival_competitor': None,
            'arrival_max_delta': np.nan,
            'max_sum_comp_in_window': np.nan,
            'min_sum_comp_in_window': np.nan,
        }
