"""
Детекция эпизодов дефектуры товаров

Порог дефектуры для каждого КАГ:
    - ЭО >= MIN_EO_FOR_PCT  ->  порог = DEFECT_EO_PCT * ЭО
    - ЭО < MIN_EO_FOR_PCT   ->  порог = 0 (дефектура только при ГК == 0)
    - КАГ нет в справочнике  ->  порог = 0
"""
import numpy as np
import pandas as pd
from typing import List, Dict, Optional
from tqdm.auto import tqdm

from config import Config


class DefecturaDetector:
    """Поиск эпизодов дефектуры"""

    def __init__(self, config: Config = Config):
        self.config = config
        self._eo_map: Optional[Dict[str, float]] = None

    def _load_eo_map(self) -> Dict[str, float]:
        if self._eo_map is not None:
            return self._eo_map

        try:
            df_eo = pd.read_excel(self.config.EO_FILE)
        except FileNotFoundError:
            print(f"Файл ЭО не найден: {self.config.EO_FILE}")
            self._eo_map = {}
            return self._eo_map

        def norm(x):
            try:
                return str(int(float(str(x).strip())))
            except Exception:
                return str(x).strip()

        eo_col = None
        for candidate in ['ЭО общая', 'ЭО']:
            if candidate in df_eo.columns:
                eo_col = candidate
                break

        if eo_col is None:
            self._eo_map = {}
            return self._eo_map

        df_eo[self.config.COL_KAG] = df_eo[self.config.COL_KAG].apply(norm)
        df_eo[eo_col] = pd.to_numeric(df_eo[eo_col], errors='coerce').fillna(0)
        df_eo = df_eo.drop_duplicates(subset=[self.config.COL_KAG], keep='first')

        self._eo_map = dict(zip(df_eo[self.config.COL_KAG], df_eo[eo_col]))
        print(f"Загружен справочник ЭО: {len(self._eo_map):,} КАГ")
        return self._eo_map

    def _get_threshold(self, kag) -> float:
        eo_map = self._load_eo_map()
        kag_str = str(kag).strip()
        try:
            kag_str = str(int(float(kag_str)))
        except Exception:
            pass
        eo = eo_map.get(kag_str, 0.0)
        if eo < self.config.MIN_EO_FOR_PCT:
            return 0.0
        return eo * self.config.DEFECT_EO_PCT

    def filter_eligible_kags(self, df: pd.DataFrame, work_date: pd.Timestamp) -> List[str]:
        """
        Фильтрует КАГ для анализа:
        1. ГК когда-либо > 0
        2. >= N наблюдений за 30 дней
        """
        gk_ever_positive = df.groupby(self.config.COL_KAG)[self.config.COL_GK].max() > 0
        last30_start = (work_date - pd.Timedelta(days=self.config.LAST30_DAYS - 1)).normalize()
        obs_last30 = df[df['date_n'] >= last30_start].groupby(self.config.COL_KAG)['date_n'].size()
        has_enough = obs_last30 >= self.config.MIN_OBS_LAST30

        eligible = [
            k for k in gk_ever_positive.index
            if bool(gk_ever_positive.get(k, False))
            and bool(has_enough.get(k, False))
        ]

        print(f"Подходящих КАГ: {len(eligible):,}")
        return eligible

    def find_last_point_defects(self, df: pd.DataFrame, eligible_kags: List[str]) -> pd.DataFrame:
        """Поиск КАГ в дефектуре на последней дате"""
        last_points = (
            df[df[self.config.COL_KAG].isin(eligible_kags)]
            .sort_values([self.config.COL_KAG, 'date_n'])
            .groupby(self.config.COL_KAG, sort=False, as_index=False)
            .tail(1)
            .copy()
        )

        last_points['_threshold'] = last_points[self.config.COL_KAG].apply(self._get_threshold)
        mask = (
            (last_points[self.config.COL_GK] < last_points['_threshold'])
            & (last_points['sum_competitors'] > 0)
        )
        result = last_points[mask].drop(columns=['_threshold']).copy()

        print(f"КАГ в дефектуре: {len(result):,}")
        return result

    def find_defectura_episodes(
        self, kag_store: dict, eligible_kags: List[str],
        work_date: pd.Timestamp, lookback_days: int = None,
    ) -> pd.DataFrame:
        """Поиск всех эпизодов дефектуры за период"""
        if lookback_days is None:
            lookback_days = self.config.LOOKBACK_DEFECTS_DAYS
        lookback_start = (work_date - pd.Timedelta(days=lookback_days - 1)).normalize()

        episodes = []
        for kag in tqdm(eligible_kags, desc="Поиск дефектур"):
            st = kag_store.get(kag)
            if st is None:
                continue
            dates, gk = st['dates'], st['gk']
            if len(gk) < 2:
                continue

            threshold = self._get_threshold(kag)
            in_defect = gk < threshold
            prev_in = np.r_[False, in_defect[:-1]]
            starts = np.where(in_defect & ~prev_in)[0]
            if starts.size == 0:
                continue

            next_ok = np.full(len(gk), -1, dtype=int)
            nxt = -1
            for i in range(len(gk) - 1, -1, -1):
                next_ok[i] = nxt
                if not in_defect[i]:
                    nxt = i

            for si in starts:
                start_date = pd.Timestamp(dates[si]).normalize()
                if start_date < lookback_start or start_date > work_date:
                    continue
                ei = next_ok[si]
                episodes.append({
                    self.config.COL_KAG: kag,
                    'defect_start_date': start_date,
                    'defect_end_date': pd.Timestamp(dates[ei]).normalize() if ei != -1 else pd.NaT,
                    'is_finished': ei != -1,
                })

        if not episodes:
            return pd.DataFrame(columns=[self.config.COL_KAG, 'defect_start_date',
                                         'defect_end_date', 'is_finished'])
        result = pd.DataFrame(episodes)
        print(f"Эпизодов: {len(result):,} (активных: {(~result['is_finished']).sum():,})")
        return result
