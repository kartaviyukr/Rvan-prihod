"""
Подготовка и валидация данных для анализа
"""
import numpy as np
import pandas as pd
from typing import Tuple
from config import Config


class DataPreparator:
    """Класс для подготовки и валидации данных"""
    
    def __init__(self, config: Config = Config):
        self.config = config
    
    def validate_and_prepare(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Timestamp]:
        """
        Валидирует и подготавливает данные для анализа
        
        Args:
            df: Исходный датафрейм
            
        Returns:
            Кортеж (подготовленный df, работная дата)
            
        Raises:
            KeyError: Если отсутствуют обязательные колонки
        """
        # Валидация колонок
        self._validate_columns(df)
        
        # Копируем данные
        d = df.copy()
        
        # Преобразуем даты
        d = self._prepare_dates(d)
        
        # Преобразуем КАГ
        d = self._prepare_kag(d)
        
        # Преобразуем остатки
        d = self._prepare_stocks(d)
        
        # Добавляем служебные поля
        d = self._add_computed_fields(d)
        
        # Сортировка и дедупликация
        d = self._sort_and_deduplicate(d)
        
        # Определяем рабочую дату
        work_date = d['date_n'].max()
        
        # Диагностика
        self._print_diagnostics(d, work_date)
        
        return d, work_date
    
    def _validate_columns(self, df: pd.DataFrame):
        """Проверяет наличие обязательных колонок"""
        required = self.config.get_required_columns()
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise KeyError(f"В датафрейме отсутствуют колонки: {missing}")
    
    def _prepare_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Преобразует даты и добавляет нормализованную дату"""
        df[self.config.COL_DATE] = pd.to_datetime(
            df[self.config.COL_DATE], errors='coerce'
        )
        df = df.dropna(subset=[self.config.COL_DATE])
        df['date_n'] = df[self.config.COL_DATE].dt.normalize()
        return df
    
    def _prepare_kag(self, df: pd.DataFrame) -> pd.DataFrame:
        """Преобразует КАГ в строковый формат"""
        df[self.config.COL_KAG] = pd.to_numeric(
            df[self.config.COL_KAG], errors='coerce'
        )
        df = df.dropna(subset=[self.config.COL_KAG])
        df[self.config.COL_KAG] = (
            df[self.config.COL_KAG].astype(np.int64).astype(str)
        )
        return df
    
    def _prepare_stocks(self, df: pd.DataFrame) -> pd.DataFrame:
        """Преобразует остатки в числовой формат, NaN -> 0"""
        stock_cols = [self.config.COL_GK, *self.config.COMP_COLS]
        for col in stock_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        return df
    
    def _add_computed_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Добавляет вычисляемые поля"""
        # Сумма остатков конкурентов
        df['sum_competitors'] = df[self.config.COMP_COLS].sum(axis=1)
        return df
    
    def _sort_and_deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Сортирует и удаляет дубликаты по КАГ+дате"""
        df = df.sort_values(
            [self.config.COL_KAG, 'date_n', self.config.COL_DATE]
        ).reset_index(drop=True)
        
        # Если в один день несколько записей - берём последнюю
        df = df.drop_duplicates(
            subset=[self.config.COL_KAG, 'date_n'], 
            keep='last'
        ).reset_index(drop=True)
        
        return df
    
    def _print_diagnostics(self, df: pd.DataFrame, work_date: pd.Timestamp):
        """Выводит диагностическую информацию"""
        print("\n" + "="*60)
        print("ДИАГНОСТИКА ДАННЫХ")
        print("="*60)
        print(f"📅 Рабочая дата: {work_date.date()}")
        print(f"📊 Всего строк: {len(df):,}")
        print(f"🏷️  Уникальных КАГ: {df[self.config.COL_KAG].nunique():,}")
        print(f"📆 Уникальных дат: {df['date_n'].nunique():,}")
        print(f"📍 Период данных: {df['date_n'].min().date()} → {df['date_n'].max().date()}")
        
        # Проверка КАГ на актуальность
        last_dates = df.groupby(self.config.COL_KAG)['date_n'].max()
        on_work_date = (last_dates == work_date).sum()
        print(f"✅ КАГ с актуальными данными (на {work_date.date()}): {on_work_date:,}")
        
        # Топ дат по количеству КАГ
        print("\n📅 Топ-5 дат по количеству КАГ:")
        top_dates = last_dates.value_counts().head(5)
        for date, count in top_dates.items():
            print(f"   {date.date()}: {count:,} КАГ")
        print("="*60 + "\n")
    
    def build_kag_store(
        self, 
        df: pd.DataFrame, 
        eligible_kags: list
    ) -> dict:
        """
        Строит быстрый доступ к данным по КАГ через numpy-массивы
        
        Args:
            df: Подготовленный датафрейм
            eligible_kags: Список КАГ для обработки
            
        Returns:
            Словарь {kag: {dates, gk, sumc, comps}}
        """
        kag_store = {}
        
        subset = df[df[self.config.COL_KAG].isin(eligible_kags)]
        
        for kag, one in subset.groupby(self.config.COL_KAG, sort=False):
            kag_store[kag] = {
                'dates': one['date_n'].to_numpy(),
                'gk': one[self.config.COL_GK].to_numpy(dtype=float),
                'sumc': one['sum_competitors'].to_numpy(dtype=float),
                'comps': {
                    c: one[c].to_numpy(dtype=float) 
                    for c in self.config.COMP_COLS
                }
            }
        
        return kag_store
