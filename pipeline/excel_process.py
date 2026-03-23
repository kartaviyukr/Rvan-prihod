"""
Объединение таблиц дефектуры (UNION ALL + постобработка + рейтинг + расчёт потерь)
"""
import shutil
import pandas as pd
from pathlib import Path

from config import Config


def _normalize_kag(x):
    try:
        return str(int(float(str(x).strip())))
    except Exception:
        return str(x).strip()


def backup_old_result(output_path: str, backup_dir: str):
    """Копирует старый результат в резерв перед перезаписью."""
    backup_dir = Path(backup_dir)
    backup_dir.mkdir(parents=True, exist_ok=True)
    output_base = Path(output_path)
    for ext in ['.xlsx', '.parquet']:
        src = output_base.with_suffix(ext)
        if src.exists():
            shutil.copy2(src, backup_dir / src.name)


def find_new_positions(df_current: pd.DataFrame, backup_dir: str, output_xlsx_name: str) -> pd.DataFrame:
    """Сравнивает текущие позиции с предыдущим файлом. Возвращает новые позиции."""
    backup_xlsx = Path(backup_dir) / output_xlsx_name
    if not backup_xlsx.exists():
        return df_current.copy()

    try:
        df_old = pd.read_excel(backup_xlsx, sheet_name='Текущие')
    except Exception:
        try:
            df_old = pd.read_excel(backup_xlsx, sheet_name=0)
        except Exception:
            return df_current.copy()

    key_cols = ['Код КАГ', 'Дата входа в дефектуру ФК Гранд Капитал', 'Статус']

    def make_key(df, cols):
        parts = []
        for c in cols:
            parts.append(df[c].astype(str).str.strip() if c in df.columns else pd.Series([''] * len(df), dtype=str))
        return parts[0].str.cat(parts[1:], sep='||')

    key_old = set(make_key(df_old, key_cols))
    key_new = make_key(df_current, key_cols)
    return df_current[~key_new.isin(key_old)].copy()


def save_multi_sheet(df_current: pd.DataFrame, df_new: pd.DataFrame, df_finished: pd.DataFrame, xlsx_path: Path):
    """Сохраняет результат в Excel с тремя листами."""
    with pd.ExcelWriter(xlsx_path, engine='openpyxl') as writer:
        df_current.to_excel(writer, index=False, sheet_name='Текущие')
        df_new.to_excel(writer, index=False, sheet_name='Новые позиции')
        df_finished.to_excel(writer, index=False, sheet_name='Завершившиеся')
    print(f"Текущие: {len(df_current):,}, Новые: {len(df_new):,}, Завершившиеся: {len(df_finished):,}")


def _prepare_source_df(df, column_mapping, final_columns, preferred_sources):
    """Приводит DataFrame к финальной структуре."""
    result = pd.DataFrame()
    reverse_map = {new: old for old, new in column_mapping.items() if old in df.columns}

    for col in final_columns:
        if col in preferred_sources:
            for src_col in preferred_sources[col]:
                if src_col in df.columns:
                    result[col] = df[src_col].values
                    break
            else:
                result[col] = None
        elif col in reverse_map:
            result[col] = df[reverse_map[col]].values
        elif col in df.columns:
            result[col] = df[col].values
        else:
            result[col] = None
    return result


def merge_defectura_tables(
    episodes_path: str, last_point_path: str,
    codex_path: str, eo_path: str,
    dict_rank_path: str, nor_path: str,
    output_path: str = None, backup_dir: str = None,
) -> pd.DataFrame:
    """Объединяет таблицы дефектуры, добавляет рейтинг и рассчитывает потери."""

    if output_path and backup_dir:
        backup_old_result(output_path, backup_dir)

    # Загрузка
    df_episodes = pd.read_excel(episodes_path)
    df_last_point = pd.read_excel(last_point_path)
    df_codex = pd.read_excel(codex_path)
    df_eo = pd.read_excel(eo_path)
    df_dict_rank = pd.read_excel(dict_rank_path)
    df_nor = pd.read_excel(nor_path)

    df_episodes['Статус'] = 'Завершилась'
    df_last_point['Статус'] = 'Текущая'

    # Маппинг колонок
    final_columns = [
        'Код КАГ', 'Имя КАГ', 'Статус',
        'Дата входа в дефектуру ФК Гранд Капитал',
        'Дата окончания дефектуры ФК Гранд Капитал',
        'Длительность дефектуры, дней',
        'Количество приходов после дефектуры у конкурентов (всего)',
        'Кол-во конкурентов с приходами', 'Конкуренты с приходами',
        'Общий объём прихода после дефектуры',
        'Приходы Пульс (дата-объём)', 'Приходы Катрен (дата-объём)',
        'Приходы Протек (дата-объём)', 'Приходы Фармкомплект (дата-объём)',
        'Объём прихода Пульс (сумма)', 'Объём прихода Катрен (сумма)',
        'Объём прихода Протек (сумма)', 'Объём прихода Фармкомплект (сумма)',
        'Остаток Пульс (вчера)', 'Остаток Катрен (вчера)',
        'Остаток Протек (вчера)', 'Остаток Фармкомплект (вчера)',
        'Остаток ФК Гранд Капитал (последняя дата)',
    ]

    column_mapping = {
        'Дата входа в дефектуру ГК': 'Дата входа в дефектуру ФК Гранд Капитал',
        'Дата выхода из дефектуры ГК': 'Дата окончания дефектуры ФК Гранд Капитал',
        'Приходов после дефектуры (всего)': 'Количество приходов после дефектуры у конкурентов (всего)',
        'Остаток ГК (последняя дата)': 'Остаток ФК Гранд Капитал (последняя дата)',
    }

    preferred_sources = {
        f'Остаток {name} (вчера)': [f'Остаток {name} (последняя дата)', f'Остаток {name} (вчера)']
        for name in ['Пульс', 'Катрен', 'Протек', 'Фармкомплект']
    }

    df_ep_prep = _prepare_source_df(df_episodes, column_mapping, final_columns, preferred_sources)
    df_lp_prep = _prepare_source_df(df_last_point, column_mapping, final_columns, preferred_sources)

    # UNION ALL
    df_final = pd.concat([df_lp_prep, df_ep_prep], ignore_index=True)

    # Постобработка
    date_col_start = 'Дата входа в дефектуру ФК Гранд Капитал'
    date_col_end = 'Дата окончания дефектуры ФК Гранд Капитал'
    df_final[date_col_start] = pd.to_datetime(df_final[date_col_start], dayfirst=True, errors='coerce')
    df_final[date_col_end] = pd.to_datetime(df_final[date_col_end], dayfirst=True, errors='coerce')

    today = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)

    # Удаление «Завершилась» без даты окончания
    df_final = df_final[~((df_final['Статус'] == 'Завершилась') & df_final[date_col_end].isna())].copy()

    # Пересчёт длительности текущих
    mask_current = df_final['Статус'] == 'Текущая'
    df_final.loc[mask_current, 'Длительность дефектуры, дней'] = (today - df_final.loc[mask_current, date_col_start]).dt.days

    # Удаление без приходов
    arrivals_col = 'Количество приходов после дефектуры у конкурентов (всего)'
    df_final[arrivals_col] = pd.to_numeric(df_final[arrivals_col], errors='coerce').fillna(0)
    df_final = df_final[df_final[arrivals_col] > 0].copy()

    # Удаление дубликатов среди завершившихся
    df_curr = df_final[df_final['Статус'] == 'Текущая'].copy()
    df_fin = df_final[df_final['Статус'] == 'Завершилась'].drop_duplicates(
        subset=['Код КАГ', date_col_start, date_col_end], keep='first',
    )
    df_final = pd.concat([df_curr, df_fin], ignore_index=True)

    # Удаление коротких эпизодов (< 3 дней)
    df_final['Длительность дефектуры, дней'] = pd.to_numeric(
        df_final['Длительность дефектуры, дней'], errors='coerce'
    ).fillna(0)
    df_final = df_final[df_final['Длительность дефектуры, дней'] >= 3].copy()

    # Рейтинг из Codex
    df_final['Код КАГ'] = df_final['Код КАГ'].apply(_normalize_kag)
    df_codex['Код КАГ'] = df_codex['Код КАГ'].apply(_normalize_kag)
    df_codex_clean = df_codex[['Код КАГ', 'Рейтинг Внешний']].drop_duplicates(subset=['Код КАГ'], keep='first')
    df_final = df_final.merge(df_codex_clean, on='Код КАГ', how='left')

    # ЭО и СВСС
    df_eo['Код КАГ'] = df_eo['Код КАГ'].apply(_normalize_kag)
    df_eo = df_eo.rename(columns={'ЭО общая': 'ЭО', 'Текущий СВСС': 'СВСС'})
    eo_cols = [c for c in ['Код КАГ', 'ЭО', 'СВСС'] if c in df_eo.columns]
    df_final = df_final.merge(df_eo[eo_cols].drop_duplicates(subset=['Код КАГ'], keep='first'), on='Код КАГ', how='left')

    # Длительность по статусам
    df_final[date_col_start] = pd.to_datetime(df_final[date_col_start], dayfirst=True, errors='coerce')
    df_final[date_col_end] = pd.to_datetime(df_final[date_col_end], dayfirst=True, errors='coerce')

    df_final['Длительность текущие'] = None
    mask_c = df_final['Статус'] == 'Текущая'
    df_final.loc[mask_c, 'Длительность текущие'] = (today - df_final.loc[mask_c, date_col_start]).dt.days

    df_final['Длительность завершившиеся'] = None
    mask_f = df_final['Статус'] == 'Завершилась'
    df_final.loc[mask_f, 'Длительность завершившиеся'] = (
        df_final.loc[mask_f, date_col_end] - df_final.loc[mask_f, date_col_start]
    ).dt.days

    # Прямой поставщик
    df_dict_rank['КАГ'] = df_dict_rank['КАГ'].apply(_normalize_kag)
    dr_clean = df_dict_rank[['КАГ', 'Прямой поставщик']].drop_duplicates(subset=['КАГ'], keep='first')
    dr_clean = dr_clean.rename(columns={'КАГ': 'Код КАГ'})
    df_final = df_final.merge(dr_clean, on='Код КАГ', how='left')

    # НОР
    nor_clean = df_nor[['Оригинальный поставщик', 'НОР']].drop_duplicates(subset=['Оригинальный поставщик'], keep='first')
    df_final = df_final.merge(nor_clean, left_on='Прямой поставщик', right_on='Оригинальный поставщик', how='left')
    df_final.drop(columns=['Оригинальный поставщик'], errors='ignore', inplace=True)

    # Расчёт потерь
    for col in ['ЭО', 'СВСС']:
        df_final[col] = pd.to_numeric(df_final.get(col, 0), errors='coerce').fillna(0)
    df_final['Потери, руб'] = (
        (df_final['СВСС'] * df_final['ЭО']) / 30 * df_final['Длительность дефектуры, дней']
    ).round(2)

    # Форматирование
    df_final[date_col_start] = pd.to_datetime(df_final[date_col_start], errors='coerce').dt.strftime('%d.%m.%Y').fillna('')
    df_final[date_col_end] = pd.to_datetime(df_final[date_col_end], errors='coerce').dt.strftime('%d.%m.%Y').fillna('')

    int_cols = [
        'Длительность дефектуры, дней', arrivals_col, 'Кол-во конкурентов с приходами',
        'Общий объём прихода после дефектуры',
        'Объём прихода Пульс (сумма)', 'Объём прихода Катрен (сумма)',
        'Объём прихода Протек (сумма)', 'Объём прихода Фармкомплект (сумма)',
        'Остаток Пульс (вчера)', 'Остаток Катрен (вчера)',
        'Остаток Протек (вчера)', 'Остаток Фармкомплект (вчера)',
        'Остаток ФК Гранд Капитал (последняя дата)',
        'Длительность текущие', 'Длительность завершившиеся',
    ]
    for col in int_cols:
        if col in df_final.columns:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0).astype(int)

    for col in ['ЭО', 'СВСС', 'Потери, руб']:
        if col in df_final.columns:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0).round(2)

    text_cols = [
        'Конкуренты с приходами',
        'Приходы Пульс (дата-объём)', 'Приходы Катрен (дата-объём)',
        'Приходы Протек (дата-объём)', 'Приходы Фармкомплект (дата-объём)',
    ]
    for col in text_cols:
        if col in df_final.columns:
            df_final[col] = df_final[col].fillna('0').astype(str).replace({'nan': '0', '': '0'})

    # Упорядочивание колонок
    final_column_order = [
        'Код КАГ', 'Имя КАГ', 'Рейтинг Внешний', 'ЭО', 'СВСС',
        'Прямой поставщик', 'НОР', 'Статус',
        date_col_start, date_col_end,
        'Длительность дефектуры, дней', 'Длительность текущие', 'Длительность завершившиеся',
        'Потери, руб', arrivals_col,
        'Кол-во конкурентов с приходами', 'Конкуренты с приходами',
        'Общий объём прихода после дефектуры',
        'Приходы Пульс (дата-объём)', 'Приходы Катрен (дата-объём)',
        'Приходы Протек (дата-объём)', 'Приходы Фармкомплект (дата-объём)',
        'Объём прихода Пульс (сумма)', 'Объём прихода Катрен (сумма)',
        'Объём прихода Протек (сумма)', 'Объём прихода Фармкомплект (сумма)',
        'Остаток Пульс (вчера)', 'Остаток Катрен (вчера)',
        'Остаток Протек (вчера)', 'Остаток Фармкомплект (вчера)',
        'Остаток ФК Гранд Капитал (последняя дата)',
    ]
    existing_cols = [c for c in final_column_order if c in df_final.columns]
    df_final = df_final[existing_cols]

    # Разделение и сохранение
    df_sheet_current = df_final[df_final['Статус'] == 'Текущая'].copy()
    df_sheet_finished = df_final[df_final['Статус'] == 'Завершилась'].copy()

    if output_path:
        output_path = Path(output_path)
        xlsx_path = output_path.with_suffix('.xlsx')
        parquet_path = output_path.with_suffix('.parquet')

        df_sheet_new = find_new_positions(df_sheet_current, backup_dir, xlsx_path.name) if backup_dir else df_sheet_current.copy()
        save_multi_sheet(df_sheet_current, df_sheet_new, df_sheet_finished, xlsx_path)
        df_final.to_parquet(parquet_path, index=False)

    print(f"Итого: {len(df_final):,} (текущих: {len(df_sheet_current):,}, завершившихся: {len(df_sheet_finished):,})")
    print(f"Потери: {df_final.get('Потери, руб', pd.Series([0])).sum():,.2f} руб")

    return df_final


if __name__ == '__main__':
    result = merge_defectura_tables(
        episodes_path=str(Config.OUT_DIR / 'final_table_episodes.xlsx'),
        last_point_path=str(Config.OUT_DIR / 'final_table_last_point.xlsx'),
        codex_path=str(Config.OUT_DIR / 'codex.xlsx'),
        eo_path=str(Config.OUT_DIR / 'EO.xlsx'),
        dict_rank_path=str(Config.OUT_DIR / 'dict_rank.xlsx'),
        nor_path=str(Config.OUT_DIR / 'nor.xlsx'),
        output_path=str(Config.OUT_DIR / 'final_merged_table'),
        backup_dir=str(Config.OUT_DIR / 'резерв'),
    )
