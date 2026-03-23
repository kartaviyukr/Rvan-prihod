"""
Экспорт результатов анализа
"""
import pandas as pd
from config import Config


class ResultExporter:
    """Экспорт результатов в различные форматы"""

    def __init__(self, config: Config = Config):
        self.config = config
        config.setup_directories()

    def export_final_table(self, df: pd.DataFrame, filename: str, work_date: pd.Timestamp):
        """Экспортирует финальную таблицу в Excel и Parquet"""
        df_export = self._prepare_for_export(df.copy())
        df_export = self._arrange_columns(df_export)

        xlsx_path = self.config.OUT_DIR / f"{filename}.xlsx"
        parquet_path = self.config.OUT_DIR / f"{filename}.parquet"

        df_export.to_excel(xlsx_path, index=False)
        df_export.to_parquet(parquet_path, index=False)

        print(f"Экспорт: {xlsx_path} ({len(df_export):,} строк)")
        return xlsx_path, parquet_path

    def _prepare_for_export(self, df: pd.DataFrame) -> pd.DataFrame:
        """Подготавливает DataFrame для экспорта"""
        df = df.loc[:, ~df.columns.duplicated()]

        date_columns = [
            'Последняя дата КАГ', 'Дата входа в дефектуру ГК',
            'Дата выхода из дефектуры ГК', 'Дата прихода у конкурента',
            'Дата остатков конкурентов (вчера)', 'Последняя дата (КАГ)',
        ]
        for col in date_columns:
            if col not in df.columns:
                continue
            col_data = df[col]
            if isinstance(col_data, pd.DataFrame):
                col_data = col_data.iloc[:, 0]
                df = df.drop(columns=[col])
                df[col] = col_data
            if df[col].dtype == 'object':
                df[col] = df[col].apply(
                    lambda x: pd.to_datetime(x, errors='coerce').strftime('%d.%m.%y') if pd.notna(x) else ''
                )
            else:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%d.%m.%y')
                except ValueError:
                    df[col] = df[col].apply(
                        lambda x: pd.to_datetime(x, errors='coerce').strftime('%d.%m.%y') if pd.notna(x) else ''
                    )

        text_columns = [
            self.config.COL_KAG_NAME, 'Категория', 'Статус дефектуры',
            'Конкуренты с приходами', 'Конкурент (первый приход)',
        ]
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).fillna('0').replace({'nan': '0', 'NaT': '0', '': '0'})

        for comp_col in self.config.COMP_COLS:
            pretty = self.config.COMP_PRETTY.get(comp_col, comp_col)
            col = f'Приходы {pretty} (дата-объём)'
            if col in df.columns:
                df[col] = df[col].astype(str).fillna('0').replace({'nan': '0', 'NaT': '0', '': '0'})

        return df

    def _arrange_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Упорядочивает колонки"""
        ordered_cols = [self.config.COL_KAG, self.config.COL_KAG_NAME]

        meta_cols = [
            'Категория', 'Статус дефектуры', 'Последняя дата КАГ',
            'Дата входа в дефектуру ГК', 'Дата выхода из дефектуры ГК',
            'Длительность дефектуры, дней',
        ]
        arrival_summary = [
            'Приходов после дефектуры (всего)', 'Кол-во конкурентов с приходами',
            'Конкуренты с приходами', 'Общий объём прихода после дефектуры',
        ]

        arrival_details, arrival_volumes = [], []
        stocks_yesterday = ['Дата остатков конкурентов (вчера)']
        stocks_last = ['Последняя дата (КАГ)']

        for comp_col in self.config.COMP_COLS:
            pretty = self.config.COMP_PRETTY.get(comp_col, comp_col)
            arrival_details.append(f'Приходы {pretty} (дата-объём)')
            arrival_volumes.append(f'Объём прихода {pretty} (сумма)')
            stocks_yesterday.append(f'Остаток {pretty} (вчера)')
            stocks_last.append(f'Остаток {pretty} (последняя дата)')

        stocks_last.append('Остаток ГК (последняя дата)')

        first_arrival = ['Дата прихода у конкурента', 'Лаг реакции, дней', 'Конкурент (первый приход)']
        technical = ['arrival_events_cnt', 'arrival_max_delta', 'max_sum_comp_in_window', 'min_sum_comp_in_window']

        final_order = (
            ordered_cols + meta_cols + arrival_summary
            + arrival_details + arrival_volumes
            + stocks_yesterday + stocks_last
            + first_arrival + technical
        )
        final_order = [c for c in final_order if c in df.columns]
        remaining = [c for c in df.columns if c not in final_order]
        return df[final_order + remaining]

    def export_wide_table(
        self, df: pd.DataFrame, kag_column: str, value_column: str,
        filename: str, n_last: int = None,
    ):
        """Экспортирует wide-таблицу (даты в колонках)"""
        if n_last is None:
            n_last = self.config.N_WIDE_LAST

        last_n = (
            df.groupby(kag_column, group_keys=False).tail(n_last)
            .sort_values([kag_column, 'date_n'])
        )
        wide = last_n.pivot_table(index=kag_column, columns='date_n', values=value_column, aggfunc='first').sort_index(axis=1)

        if self.config.COL_KAG_NAME in last_n.columns:
            name_map = last_n.groupby(kag_column)[self.config.COL_KAG_NAME].first()
            wide.insert(0, self.config.COL_KAG_NAME, wide.index.map(name_map))

        output_path = self.config.OUT_DIR / f"{filename}.xlsx"
        wide.to_excel(output_path)
        print(f"Wide-таблица: {output_path}")
        return output_path
