"""
Экспорт результатов анализа
"""
import pandas as pd
from pathlib import Path
from typing import List
from config import Config


class ResultExporter:
    """Класс для экспорта результатов в различные форматы"""
    
    def __init__(self, config: Config = Config):
        self.config = config
        config.setup_directories()
    
    def export_final_table(
        self,
        df: pd.DataFrame,
        filename: str,
        work_date: pd.Timestamp
    ):
        """
        Экспортирует финальную таблицу в Excel и Parquet
        
        Args:
            df: DataFrame для экспорта
            filename: Базовое имя файла (без расширения)
            work_date: Рабочая дата (для логов)
        """
        # Подготавливаем данные
        df_export = self._prepare_for_export(df.copy())
        
        # Формируем колонки в правильном порядке
        df_export = self._arrange_columns(df_export)
        
        # Пути
        xlsx_path = self.config.OUT_DIR / f"{filename}.xlsx"
        parquet_path = self.config.OUT_DIR / f"{filename}.parquet"
        
        # Сохраняем
        df_export.to_excel(xlsx_path, index=False)
        df_export.to_parquet(parquet_path, index=False)
        
        print(f"\n{'='*60}")
        print("ЭКСПОРТ РЕЗУЛЬТАТОВ")
        print(f"{'='*60}")
        print(f"✅ Excel: {xlsx_path}")
        print(f"✅ Parquet: {parquet_path}")
        print(f"📊 Строк: {len(df_export):,}")
        print(f"📅 Рабочая дата: {work_date.strftime('%d.%m.%Y')}")
        print(f"{'='*60}\n")
        
        return xlsx_path, parquet_path
        
    def _prepare_for_export(self, df: pd.DataFrame) -> pd.DataFrame:
        """Подготавливает DataFrame для экспорта (типы, форматы)"""
        
        # ========== КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ ==========
        # Удаляем дублирующиеся колонки (оставляем первую)
        df = df.loc[:, ~df.columns.duplicated()]
        # =============================================
        
        # Даты в строковый формат
        date_columns = [
            'Последняя дата КАГ',
            'Дата входа в дефектуру ГК',
            'Дата выхода из дефектуры ГК',
            'Дата прихода у конкурента',
            'Дата остатков конкурентов (вчера)',
            'Последняя дата (КАГ)',
        ]
        
        for col in date_columns:
            if col in df.columns:
                # Безопасная проверка - убеждаемся что это Series
                col_data = df[col]
                if isinstance(col_data, pd.DataFrame):
                    # Если всё ещё DataFrame - берём первую колонку
                    col_data = col_data.iloc[:, 0]
                    df = df.drop(columns=[col])
                    df[col] = col_data
                
                # Проверяем тип колонки
                if df[col].dtype == 'object':
                    # Уже строка или смешанный тип - преобразуем аккуратно
                    df[col] = df[col].apply(lambda x: 
                        pd.to_datetime(x, errors='coerce').strftime('%d.%m.%y') 
                        if pd.notna(x) else ''
                    )
                else:
                    # Datetime - конвертируем напрямую
                    try:
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%d.%m.%y')
                    except ValueError:
                        df[col] = df[col].apply(lambda x: 
                            pd.to_datetime(x, errors='coerce').strftime('%d.%m.%y') 
                            if pd.notna(x) else ''
                        )
        
        # Текстовые поля (защита от NaN)
        text_columns = [
            self.config.COL_KAG_NAME,
            'Категория',
            'Статус дефектуры',
            'Конкуренты с приходами',
            'Конкурент (первый приход)',
        ]
        
        for col in text_columns:
            if col in df.columns:
                df[col] = (
                    df[col].astype(str)
                    .fillna('0')
                    .replace({'nan': '0', 'NaT': '0', '': '0'})
                )
        
        # Поля с приходами (дата-объём)
        for comp_col in self.config.COMP_COLS:
            pretty = self.config.COMP_PRETTY.get(comp_col, comp_col)
            col = f'Приходы {pretty} (дата-объём)'
            if col in df.columns:
                df[col] = (
                    df[col].astype(str)
                    .fillna('0')
                    .replace({'nan': '0', 'NaT': '0', '': '0'})
                )
        
        return df
    
    def _arrange_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Упорядочивает колонки в правильной последовательности"""
        # Базовые колонки
        ordered_cols = [
            self.config.COL_KAG,
            self.config.COL_KAG_NAME,
        ]
        
        # Категория и метаданные
        meta_cols = [
            'Категория',
            'Статус дефектуры',
            'Последняя дата КАГ',
            'Дата входа в дефектуру ГК',
            'Дата выхода из дефектуры ГК',
            'Длительность дефектуры, дней',
        ]
        
        # Общая статистика прихода
        arrival_summary = [
            'Приходов после дефектуры (всего)',
            'Кол-во конкурентов с приходами',
            'Конкуренты с приходами',
            'Общий объём прихода после дефектуры',
        ]
        
        # Приходы по конкурентам (дата-объём)
        arrival_details = []
        for comp_col in self.config.COMP_COLS:
            pretty = self.config.COMP_PRETTY.get(comp_col, comp_col)
            arrival_details.append(f'Приходы {pretty} (дата-объём)')
        
        # Объёмы прихода по конкурентам
        arrival_volumes = []
        for comp_col in self.config.COMP_COLS:
            pretty = self.config.COMP_PRETTY.get(comp_col, comp_col)
            arrival_volumes.append(f'Объём прихода {pretty} (сумма)')
        
        # Остатки на вчера
        stocks_yesterday = ['Дата остатков конкурентов (вчера)']
        for comp_col in self.config.COMP_COLS:
            pretty = self.config.COMP_PRETTY.get(comp_col, comp_col)
            stocks_yesterday.append(f'Остаток {pretty} (вчера)')
        
        # Остатки на последнюю дату
        stocks_last = ['Последняя дата (КАГ)']
        for comp_col in self.config.COMP_COLS:
            pretty = self.config.COMP_PRETTY.get(comp_col, comp_col)
            stocks_last.append(f'Остаток {pretty} (последняя дата)')
        stocks_last.append('Остаток ГК (последняя дата)')
        
        # Метрики первого прихода
        first_arrival = [
            'Дата прихода у конкурента',
            'Лаг реакции, дней',
            'Конкурент (первый приход)',
        ]
        
        # Технические поля
        technical = [
            'arrival_events_cnt',
            'arrival_max_delta',
            'max_sum_comp_in_window',
            'min_sum_comp_in_window',
        ]
        
        # Собираем порядок
        final_order = (
            ordered_cols + meta_cols + arrival_summary + 
            arrival_details + arrival_volumes + 
            stocks_yesterday + stocks_last + 
            first_arrival + technical
        )
        
        # Фильтруем только существующие колонки
        final_order = [c for c in final_order if c in df.columns]
        
        # Добавляем остальные колонки в конец
        remaining = [c for c in df.columns if c not in final_order]
        
        return df[final_order + remaining]
    
    def export_wide_table(
        self,
        df: pd.DataFrame,
        kag_column: str,
        value_column: str,
        filename: str,
        n_last: int = None
    ):
        """
        Экспортирует wide-таблицу (даты в колонках)
        
        Args:
            df: Исходный DataFrame (long format)
            kag_column: Колонка с кодами КАГ
            value_column: Колонка со значениями
            filename: Имя файла для экспорта
            n_last: Сколько последних наблюдений брать
        """
        if n_last is None:
            n_last = self.config.N_WIDE_LAST
        
        # Берём последние n наблюдений по каждому КАГ
        last_n = (
            df.groupby(kag_column, group_keys=False)
            .tail(n_last)
            .sort_values([kag_column, 'date_n'])
        )
        
        # Pivot
        wide = last_n.pivot_table(
            index=kag_column,
            columns='date_n',
            values=value_column,
            aggfunc='first'
        ).sort_index(axis=1)
        
        # Добавляем названия КАГ
        if self.config.COL_KAG_NAME in last_n.columns:
            name_map = last_n.groupby(kag_column)[self.config.COL_KAG_NAME].first()
            wide.insert(0, self.config.COL_KAG_NAME, wide.index.map(name_map))
        
        # Экспорт
        output_path = self.config.OUT_DIR / f"{filename}.xlsx"
        wide.to_excel(output_path)
        
        print(f"✅ Wide-таблица: {output_path}")
        
        return output_path
