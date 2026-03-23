"""
Подготовка и валидация данных для анализа дефектуры
"""
import numpy as np
import pandas as pd
from typing import Tuple

from config import Config


class DataPreparator:
    """Подготовка данных: валидация, типизация, дедупликация"""

    def __init__(self, config: Config = Config):
        self.config = config

    def validate_and_prepare(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Timestamp]:
        """
        Валидирует и подготавливает данные.
        Returns: (подготовленный df, рабочая дата)
        """
        required = self.config.get_required_columns()
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise KeyError(f"Отсутствуют колонки: {missing}")

        d = df.copy()

        # Типизация
        d[self.config.COL_DATE] = pd.to_datetime(d[self.config.COL_DATE], errors='coerce')
        d = d.dropna(subset=[self.config.COL_DATE])
        d['date_n'] = d[self.config.COL_DATE].dt.normalize()

        d[self.config.COL_KAG] = pd.to_numeric(d[self.config.COL_KAG], errors='coerce')
        d = d.dropna(subset=[self.config.COL_KAG])
        d[self.config.COL_KAG] = d[self.config.COL_KAG].astype(np.int64).astype(str)

        for col in [self.config.COL_GK, *self.config.COMP_COLS]:
            d[col] = pd.to_numeric(d[col], errors='coerce').fillna(0.0)

        d['sum_competitors'] = d[self.config.COMP_COLS].sum(axis=1)

        # Сортировка и дедупликация
        d = (d.sort_values([self.config.COL_KAG, 'date_n', self.config.COL_DATE])
              .drop_duplicates(subset=[self.config.COL_KAG, 'date_n'], keep='last')
              .reset_index(drop=True))

        work_date = d['date_n'].max()

        print(f"Рабочая дата: {work_date.date()}")
        print(f"Строк: {len(d):,}, КАГ: {d[self.config.COL_KAG].nunique():,}, "
              f"Дат: {d['date_n'].nunique():,}")

        return d, work_date

    def build_kag_store(self, df: pd.DataFrame, eligible_kags: list) -> dict:
        """Строит словарь numpy-массивов по КАГ для быстрого доступа"""
        kag_store = {}
        subset = df[df[self.config.COL_KAG].isin(eligible_kags)]

        for kag, one in subset.groupby(self.config.COL_KAG, sort=False):
            kag_store[kag] = {
                'dates': one['date_n'].to_numpy(),
                'gk': one[self.config.COL_GK].to_numpy(dtype=float),
                'sumc': one['sum_competitors'].to_numpy(dtype=float),
                'comps': {c: one[c].to_numpy(dtype=float) for c in self.config.COMP_COLS},
            }

        return kag_store
