"""
Детекция прихода товаров у конкурентов

Приход = значительный рост остатков:
    delta >= DELTA_ARRIVAL  И  delta >= MIN_PCT_FROM_YESTERDAY * yesterday
"""
import numpy as np
import pandas as pd
from typing import List, Tuple, Dict, Optional

from config import Config


class ArrivalDetector:

    def __init__(self, config: Config = Config):
        self.config = config

    def detect_in_window(
        self, data: pd.DataFrame, competitor_col: str,
        start_date: pd.Timestamp, end_date: Optional[pd.Timestamp] = None,
    ) -> List[Tuple[pd.Timestamp, float]]:
        """Детектирует приходы для одного конкурента в окне"""
        if data.empty or len(data) < 2:
            return []

        start_n = pd.to_datetime(start_date).normalize()
        end_n = pd.to_datetime(end_date).normalize() if end_date else data['date_n'].max()

        window = data[(data['date_n'] >= start_n) & (data['date_n'] <= end_n)]
        if len(window) < 2:
            return []

        arr = pd.to_numeric(window[competitor_col], errors='coerce').fillna(0).to_numpy(dtype=float)
        prev = arr[:-1]
        delta = np.diff(arr)
        hit_mask = (delta >= self.config.DELTA_ARRIVAL) & (delta >= self.config.MIN_PCT_FROM_YESTERDAY * prev)

        return [
            (pd.to_datetime(window.iloc[idx + 1]['date_n']).normalize(), float(delta[idx]))
            for idx in np.where(hit_mask)[0]
        ]

    def detect_for_all_competitors(
        self, data: pd.DataFrame,
        start_date: pd.Timestamp, end_date: Optional[pd.Timestamp] = None,
    ) -> Dict[str, List[Tuple[pd.Timestamp, float]]]:
        results = {}
        for comp_col in self.config.COMP_COLS:
            events = self.detect_in_window(data, comp_col, start_date, end_date)
            if events:
                results[comp_col] = events
        return results

    def detect_from_numpy_store(
        self, store: dict, start_date: pd.Timestamp,
        end_date: Optional[pd.Timestamp], work_date: pd.Timestamp,
    ) -> dict:
        """Быстрая детекция через предподготовленные numpy массивы"""
        dates, sumc, comps = store['dates'], store['sumc'], store['comps']

        start_np = np.datetime64(pd.to_datetime(start_date).normalize())
        end_np = np.datetime64(work_date) if end_date is None or pd.isna(end_date) \
            else np.datetime64(pd.to_datetime(end_date).normalize())

        left = np.searchsorted(dates, start_np, side='left')
        right = np.searchsorted(dates, end_np, side='right')

        if right - left < 2:
            return self._empty_result()

        sum_slice = sumc[left:right]
        max_sum = float(np.max(sum_slice))
        min_sum = float(np.min(sum_slice))

        first_hit_by_comp = {}
        total_hits = 0
        max_delta_global = None

        for comp_col in self.config.COMP_COLS:
            arr = comps[comp_col][left:right]
            prev = arr[:-1]
            delta = np.diff(arr)
            hit_mask = (delta >= self.config.DELTA_ARRIVAL) & (delta >= self.config.MIN_PCT_FROM_YESTERDAY * prev)
            if not np.any(hit_mask):
                continue

            hit_idx = np.where(hit_mask)[0]
            total_hits += int(hit_idx.size)
            local_max = float(np.max(delta[hit_idx]))
            if max_delta_global is None or local_max > max_delta_global:
                max_delta_global = local_max

            j = int(hit_idx[0] + 1)
            first_hit_by_comp[comp_col] = pd.Timestamp(dates[left + j]).normalize()

        if not first_hit_by_comp:
            return {**self._empty_result(), 'max_sum_comp_in_window': max_sum,
                    'min_sum_comp_in_window': min_sum}

        min_date = min(first_hit_by_comp.values())
        return {
            'arrival_flag': True,
            'arrival_events_cnt': total_hits,
            'arrival_first_date': min_date,
            'arrival_competitor': '; '.join(c for c, dt in first_hit_by_comp.items() if dt == min_date),
            'arrival_max_delta': float(max_delta_global) if max_delta_global else np.nan,
            'max_sum_comp_in_window': max_sum,
            'min_sum_comp_in_window': min_sum,
        }

    @staticmethod
    def _empty_result() -> dict:
        return {
            'arrival_flag': False, 'arrival_events_cnt': 0,
            'arrival_first_date': pd.NaT, 'arrival_competitor': None,
            'arrival_max_delta': np.nan, 'max_sum_comp_in_window': np.nan,
            'min_sum_comp_in_window': np.nan,
        }
