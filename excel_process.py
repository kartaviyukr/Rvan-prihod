"""
Объединение таблиц дефектуры (UNION ALL + постобработка + рейтинг + расчёт потерь)
"""

import pandas as pd
import shutil
from pathlib import Path


def backup_old_result(output_path: str, backup_dir: str):
    """Копирует старый final_merged_table в папку резерв перед перезаписью."""
    backup_dir = Path(backup_dir)
    backup_dir.mkdir(parents=True, exist_ok=True)

    output_base = Path(output_path)
    for ext in ['.xlsx', '.parquet']:
        src = output_base.with_suffix(ext)
        if src.exists():
            dst = backup_dir / src.name
            shutil.copy2(src, dst)
            print(f"   📦 Скопирован резерв: {src} → {dst}")
        else:
            print(f"   ⚠️ Файл не найден (пропуск): {src}")


def find_new_positions(df_current: pd.DataFrame, backup_dir: str, output_xlsx_name: str) -> pd.DataFrame:
    """
    Сравнивает текущие позиции с предыдущим файлом из резерва.
    Возвращает DataFrame с новыми позициями (которых не было раньше).

    Ключ сравнения: (Код КАГ, Дата входа в дефектуру ФК Гранд Капитал, Статус)
    """
    backup_xlsx = Path(backup_dir) / output_xlsx_name

    if not backup_xlsx.exists():
        print("   ⚠️ Старый файл в резерве не найден — все текущие считаются новыми.")
        return df_current.copy()

    print(f"\n🔍 Сравнение с предыдущей версией: {backup_xlsx}")

    # Читаем лист "Текущие" если есть, иначе первый лист
    try:
        df_old = pd.read_excel(backup_xlsx, sheet_name='Текущие')
    except Exception:
        try:
            df_old = pd.read_excel(backup_xlsx, sheet_name=0)
        except Exception:
            print("   ⚠️ Не удалось прочитать старый файл — все текущие считаются новыми.")
            return df_current.copy()

    key_cols = ['Код КАГ', 'Дата входа в дефектуру ФК Гранд Капитал', 'Статус']

    def make_key(df, cols):
        parts = []
        for c in cols:
            if c in df.columns:
                parts.append(df[c].astype(str).str.strip())
            else:
                parts.append(pd.Series([''] * len(df), dtype=str))
        return parts[0].str.cat(parts[1:], sep='||')

    key_new = make_key(df_current, key_cols)
    key_old = make_key(df_old, key_cols)

    old_keys_set = set(key_old)
    mask_new = ~key_new.isin(old_keys_set)

    df_diff = df_current[mask_new].copy()

    print(f"   📊 Строк в старом файле (текущие): {len(df_old):,}")
    print(f"   📊 Строк в новом файле (текущие): {len(df_current):,}")
    print(f"   🆕 Новых позиций: {len(df_diff):,}")

    return df_diff


def save_multi_sheet(
    df_current: pd.DataFrame,
    df_new_positions: pd.DataFrame,
    df_finished: pd.DataFrame,
    xlsx_path: Path,
):
    """Сохраняет результат в Excel с тремя листами."""
    with pd.ExcelWriter(xlsx_path, engine='openpyxl') as writer:
        df_current.to_excel(writer, index=False, sheet_name='Текущие')
        df_new_positions.to_excel(writer, index=False, sheet_name='Новые позиции')
        df_finished.to_excel(writer, index=False, sheet_name='Завершившиеся')

    print(f"\n   ✅ Лист 'Текущие': {len(df_current):,} строк")
    print(f"   ✅ Лист 'Новые позиции': {len(df_new_positions):,} строк")
    print(f"   ✅ Лист 'Завершившиеся': {len(df_finished):,} строк")


def merge_defectura_tables(
    episodes_path: str,
    last_point_path: str,
    codex_path: str,
    eo_path: str,
    dict_rank_path: str,
    nor_path: str,
    output_path: str = None,
    backup_dir: str = None
) -> pd.DataFrame:
    
    # ================================================================
    # 0. РЕЗЕРВНОЕ КОПИРОВАНИЕ СТАРОГО РЕЗУЛЬТАТА
    # ================================================================
    if output_path and backup_dir:
        print("=" * 60)
        print("РЕЗЕРВНОЕ КОПИРОВАНИЕ")
        print("=" * 60)
        backup_old_result(output_path, backup_dir)
    
    print("\n" + "=" * 60)
    print("ОБЪЕДИНЕНИЕ ТАБЛИЦ (UNION ALL)")
    print("=" * 60)
    
    # ================================================================
    # 1. ЗАГРУЗКА
    # ================================================================
    print("\n📂 Загрузка файлов...")
    
    df_episodes = pd.read_excel(episodes_path)
    df_last_point = pd.read_excel(last_point_path)
    df_codex = pd.read_excel(codex_path)
    df_eo = pd.read_excel(eo_path)
    df_dict_rank = pd.read_excel(dict_rank_path)
    df_nor = pd.read_excel(nor_path)
    
    print(f"   Episodes: {len(df_episodes):,} строк")
    print(f"   Last Point: {len(df_last_point):,} строк")
    print(f"   Codex: {len(df_codex):,} строк")
    print(f"   ЭО: {len(df_eo):,} строк")
    print(f"   Dict Rank: {len(df_dict_rank):,} строк")
    print(f"   NOR: {len(df_nor):,} строк")
    
    for name, df in [('Episodes', df_episodes), ('Last Point', df_last_point)]:
        ost_cols = [c for c in df.columns if 'Остаток' in c]
        print(f"   {name} колонки остатков: {ost_cols}")
    
    # ================================================================
    # 2. ДОБАВЛЕНИЕ СТАТУСА
    # ================================================================
    df_episodes['Статус'] = 'Завершилась'
    df_last_point['Статус'] = 'Текущая'
    
    # ================================================================
    # 3. ПРИВЕДЕНИЕ К ЕДИНОЙ СТРУКТУРЕ
    # ================================================================
    print("\n🔧 Приведение к единой структуре...")
    
    final_columns = [
        'Код КАГ',
        'Имя КАГ',
        'Статус',
        'Дата входа в дефектуру ФК Гранд Капитал',
        'Дата окончания дефектуры ФК Гранд Капитал',
        'Длительность дефектуры, дней',
        'Количество приходов после дефектуры у конкурентов (всего)',
        'Кол-во конкурентов с приходами',
        'Конкуренты с приходами',
        'Общий объём прихода после дефектуры',
        'Приходы Пульс (дата-объём)',
        'Приходы Катрен (дата-объём)',
        'Приходы Протек (дата-объём)',
        'Приходы Фармкомплект (дата-объём)',
        'Объём прихода Пульс (сумма)',
        'Объём прихода Катрен (сумма)',
        'Объём прихода Протек (сумма)',
        'Объём прихода Фармкомплект (сумма)',
        'Остаток Пульс (вчера)',
        'Остаток Катрен (вчера)',
        'Остаток Протек (вчера)',
        'Остаток Фармкомплект (вчера)',
        'Остаток ФК Гранд Капитал (последняя дата)',
    ]
    
    column_mapping = {
        'Код КАГ': 'Код КАГ',
        'Имя КАГ': 'Имя КАГ',
        'Статус': 'Статус',
        'Дата входа в дефектуру ГК': 'Дата входа в дефектуру ФК Гранд Капитал',
        'Дата выхода из дефектуры ГК': 'Дата окончания дефектуры ФК Гранд Капитал',
        'Длительность дефектуры, дней': 'Длительность дефектуры, дней',
        'Приходов после дефектуры (всего)': 'Количество приходов после дефектуры у конкурентов (всего)',
        'Кол-во конкурентов с приходами': 'Кол-во конкурентов с приходами',
        'Конкуренты с приходами': 'Конкуренты с приходами',
        'Общий объём прихода после дефектуры': 'Общий объём прихода после дефектуры',
        'Приходы Пульс (дата-объём)': 'Приходы Пульс (дата-объём)',
        'Приходы Катрен (дата-объём)': 'Приходы Катрен (дата-объём)',
        'Приходы Протек (дата-объём)': 'Приходы Протек (дата-объём)',
        'Приходы Фармкомплект (дата-объём)': 'Приходы Фармкомплект (дата-объём)',
        'Объём прихода Пульс (сумма)': 'Объём прихода Пульс (сумма)',
        'Объём прихода Катрен (сумма)': 'Объём прихода Катрен (сумма)',
        'Объём прихода Протек (сумма)': 'Объём прихода Протек (сумма)',
        'Объём прихода Фармкомплект (сумма)': 'Объём прихода Фармкомплект (сумма)',
        'Остаток ГК (последняя дата)': 'Остаток ФК Гранд Капитал (последняя дата)',
    }
    
    preferred_sources = {
        'Остаток Пульс (вчера)': [
            'Остаток Пульс (последняя дата)',
            'Остаток Пульс (вчера)',
        ],
        'Остаток Катрен (вчера)': [
            'Остаток Катрен (последняя дата)',
            'Остаток Катрен (вчера)',
        ],
        'Остаток Протек (вчера)': [
            'Остаток Протек (последняя дата)',
            'Остаток Протек (вчера)',
        ],
        'Остаток Фармкомплект (вчера)': [
            'Остаток Фармкомплект (последняя дата)',
            'Остаток Фармкомплект (вчера)',
        ],
    }
    
    def prepare_df(df, source_name):
        """Приводит DataFrame к финальной структуре"""
        result = pd.DataFrame()
        
        reverse_map = {}
        for old, new in column_mapping.items():
            if old in df.columns:
                reverse_map[new] = old
        
        for col in final_columns:
            if col in preferred_sources:
                found = False
                for src_col in preferred_sources[col]:
                    if src_col in df.columns:
                        result[col] = df[src_col].values
                        print(f"      {col} ← взято из '{src_col}'")
                        found = True
                        break
                if not found:
                    result[col] = None
                    print(f"      {col} ← НЕ НАЙДЕНО, заполнено None")
            elif col in reverse_map:
                result[col] = df[reverse_map[col]].values
            elif col in df.columns:
                result[col] = df[col].values
            else:
                result[col] = None
        
        print(f"   {source_name}: {len(result):,} строк")
        
        for c in ['Остаток Пульс (вчера)', 'Остаток Катрен (вчера)',
                   'Остаток Протек (вчера)', 'Остаток Фармкомплект (вчера)']:
            non_null = pd.to_numeric(result[c], errors='coerce').fillna(0)
            non_zero = (non_null > 0).sum()
            print(f"      {c}: {non_zero} ненулевых из {len(result)}")
        
        return result
    
    df_ep_prep = prepare_df(df_episodes, "Episodes")
    df_lp_prep = prepare_df(df_last_point, "Last Point")
    
    # ================================================================
    # 4. UNION ALL
    # ================================================================
    print("\n📋 UNION ALL...")
    
    df_final = pd.concat([df_lp_prep, df_ep_prep], ignore_index=True)
    
    print(f"   Итого после объединения: {len(df_final):,} строк")
    
    # ================================================================
    # 5. ПОСТОБРАБОТКА
    # ================================================================
    print("\n🔧 Постобработка...")
    
    date_col_start = 'Дата входа в дефектуру ФК Гранд Капитал'
    date_col_end = 'Дата окончания дефектуры ФК Гранд Капитал'
    
    df_final[date_col_start] = pd.to_datetime(
        df_final[date_col_start], 
        dayfirst=True,
        errors='coerce'
    )
    df_final[date_col_end] = pd.to_datetime(
        df_final[date_col_end], 
        dayfirst=True,
        errors='coerce'
    )
    
    today = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
    
    # --- Условие 1: Удалить "Завершилась" без даты окончания ---
    before = len(df_final)
    mask_invalid_finished = (
        (df_final['Статус'] == 'Завершилась') & 
        (df_final[date_col_end].isna())
    )
    df_final = df_final[~mask_invalid_finished].copy()
    print(f"   ❌ Удалено (Завершилась без даты окончания): {before - len(df_final):,}")
    
    # --- Условие 2: Для "Текущая" пересчитать длительность ---
    mask_current = df_final['Статус'] == 'Текущая'
    df_final.loc[mask_current, 'Длительность дефектуры, дней'] = (
        today - df_final.loc[mask_current, date_col_start]
    ).dt.days
    print(f"   ✅ Пересчитана длительность для Текущих: {mask_current.sum():,}")
    
    # --- Условие 3: Удалить где приходов = 0 или пропуск ---
    arrivals_col = 'Количество приходов после дефектуры у конкурентов (всего)'
    before = len(df_final)
    df_final[arrivals_col] = pd.to_numeric(df_final[arrivals_col], errors='coerce').fillna(0)
    mask_no_arrivals = df_final[arrivals_col] == 0
    df_final = df_final[~mask_no_arrivals].copy()
    print(f"   ❌ Удалено (нет приходов): {before - len(df_final):,}")
    
    # --- Условие 4: Удалить дубликаты среди "Завершилась" ---
    before = len(df_final)
    
    df_current = df_final[df_final['Статус'] == 'Текущая'].copy()
    df_finished = df_final[df_final['Статус'] == 'Завершилась'].copy()
    
    df_finished = df_finished.drop_duplicates(
        subset=['Код КАГ', date_col_start, date_col_end],
        keep='first'
    )
    
    df_final = pd.concat([df_current, df_finished], ignore_index=True)
    print(f"   ❌ Удалено дубликатов (Завершилась): {before - len(df_final):,}")
    
    # --- Условие 5: Удалить где длительность < 3 дней ---
    before = len(df_final)
    df_final['Длительность дефектуры, дней'] = pd.to_numeric(
        df_final['Длительность дефектуры, дней'], errors='coerce'
    ).fillna(0)
    mask_short = df_final['Длительность дефектуры, дней'] < 3
    df_final = df_final[~mask_short].copy()
    print(f"   ❌ Удалено (длительность < 3 дней): {before - len(df_final):,}")
    
    # ================================================================
    # 6. ДОБАВЛЕНИЕ РЕЙТИНГА ИЗ CODEX
    # ================================================================
    print("\n📊 Добавление рейтинга из Codex...")

    def normalize_kag(x):
        try:
            return str(int(float(str(x).strip())))
        except:
            return str(x).strip()

    df_final['Код КАГ'] = df_final['Код КАГ'].apply(normalize_kag)
    df_codex['Код КАГ'] = df_codex['Код КАГ'].apply(normalize_kag)

    codex_cols = ['Код КАГ', 'Рейтинг Внешний']
    df_codex_clean = df_codex[codex_cols].drop_duplicates(subset=['Код КАГ'], keep='first')

    df_final = df_final.merge(df_codex_clean, on='Код КАГ', how='left')

    matched = df_final['Рейтинг Внешний'].notna().sum()
    print(f"   ✅ Сопоставлено с рейтингом: {matched:,} из {len(df_final):,}")
    
    # ================================================================
    # 6.1. ДОБАВЛЕНИЕ ЭО И СВСС
    # ================================================================
    print("\n📊 Добавление ЭО и СВСС...")
    
    df_eo['Код КАГ'] = df_eo['Код КАГ'].apply(normalize_kag)
    
    eo_column_mapping = {
        'ЭО общая': 'ЭО',
        'Текущий СВСС': 'СВСС'
    }
    df_eo = df_eo.rename(columns=eo_column_mapping)
    
    eo_cols = ['Код КАГ', 'ЭО', 'СВСС']
    available_eo_cols = [c for c in eo_cols if c in df_eo.columns]
    df_eo_clean = df_eo[available_eo_cols].drop_duplicates(subset=['Код КАГ'], keep='first')
    
    df_final = df_final.merge(df_eo_clean, on='Код КАГ', how='left')
    
    matched_eo = df_final['ЭО'].notna().sum()
    matched_svss = df_final['СВСС'].notna().sum()
    print(f"   ✅ Сопоставлено ЭО: {matched_eo:,} из {len(df_final):,}")
    print(f"   ✅ Сопоставлено СВСС: {matched_svss:,} из {len(df_final):,}")
    
    # ================================================================
    # 6.2. РАСЧЁТ ДЛИТЕЛЬНОСТИ ТЕКУЩИЕ / ЗАВЕРШИВШИЕСЯ
    # ================================================================
    print("\n📊 Расчёт длительности по статусам...")
    
    df_final[date_col_start] = pd.to_datetime(df_final[date_col_start], dayfirst=True, errors='coerce')
    df_final[date_col_end] = pd.to_datetime(df_final[date_col_end], dayfirst=True, errors='coerce')
    
    df_final['Длительность текущие'] = None
    mask_current = df_final['Статус'] == 'Текущая'
    df_final.loc[mask_current, 'Длительность текущие'] = (
        today - df_final.loc[mask_current, date_col_start]
    ).dt.days
    
    df_final['Длительность завершившиеся'] = None
    mask_finished = df_final['Статус'] == 'Завершилась'
    df_final.loc[mask_finished, 'Длительность завершившиеся'] = (
        df_final.loc[mask_finished, date_col_end] - df_final.loc[mask_finished, date_col_start]
    ).dt.days
    
    print(f"   ✅ Рассчитана длительность текущих: {mask_current.sum():,}")
    print(f"   ✅ Рассчитана длительность завершившихся: {mask_finished.sum():,}")
    
    # ================================================================
    # 6.2.1. ДОБАВЛЕНИЕ ПРЯМОГО ПОСТАВЩИКА ИЗ DICT_RANK
    # ================================================================
    print("\n📊 Добавление прямого поставщика...")
    
    df_dict_rank['КАГ'] = df_dict_rank['КАГ'].apply(normalize_kag)
    
    df_dict_rank_clean = df_dict_rank[['КАГ', 'Прямой поставщик']].drop_duplicates(
        subset=['КАГ'], keep='first'
    ).rename(columns={'КАГ': 'Код КАГ'})
    
    df_final = df_final.merge(df_dict_rank_clean, on='Код КАГ', how='left')
    
    matched_rank = df_final['Прямой поставщик'].notna().sum()
    print(f"   ✅ Сопоставлено прямой поставщик: {matched_rank:,} из {len(df_final):,}")
    
    # ================================================================
    # 6.2.2. ДОБАВЛЕНИЕ НОР ИЗ NOR
    # ================================================================
    print("\n📊 Добавление НОР...")
    
    nor_cols = ['Оригинальный поставщик', 'НОР']
    df_nor_clean = df_nor[nor_cols].drop_duplicates(
        subset=['Оригинальный поставщик'], keep='first'
    )
    
    df_final = df_final.merge(
        df_nor_clean,
        left_on='Прямой поставщик',
        right_on='Оригинальный поставщик',
        how='left'
    )
    
    if 'Оригинальный поставщик' in df_final.columns:
        df_final.drop(columns=['Оригинальный поставщик'], inplace=True)
    
    matched_nor = df_final['НОР'].notna().sum()
    print(f"   ✅ Сопоставлено НОР: {matched_nor:,} из {len(df_final):,}")
    
    # ================================================================
    # 6.3. РАСЧЁТ ПОТЕРЬ
    # ================================================================
    print("\n💰 Расчёт потерь...")
    
    df_final['ЭО'] = pd.to_numeric(df_final['ЭО'], errors='coerce').fillna(0)
    df_final['СВСС'] = pd.to_numeric(df_final['СВСС'], errors='coerce').fillna(0)
    df_final['Длительность дефектуры, дней'] = pd.to_numeric(
        df_final['Длительность дефектуры, дней'], errors='coerce'
    ).fillna(0)
    
    df_final['Потери, руб'] = (
        (df_final['СВСС'] * df_final['ЭО']) / 30 * df_final['Длительность дефектуры, дней']
    ).round(2)
    
    total_losses = df_final['Потери, руб'].sum()
    print(f"   ✅ Рассчитаны потери для {(df_final['Потери, руб'] > 0).sum():,} строк")
    print(f"   💰 Общая сумма потерь: {total_losses:,.2f} руб")
    
    # ================================================================
    # 7. ФОРМАТИРОВАНИЕ
    # ================================================================
    print("\n🎨 Форматирование...")
    
    df_final[date_col_start] = pd.to_datetime(df_final[date_col_start], errors='coerce').dt.strftime('%d.%m.%Y').fillna('')
    df_final[date_col_end] = pd.to_datetime(df_final[date_col_end], errors='coerce').dt.strftime('%d.%m.%Y').fillna('')
    
    int_cols = [
        'Длительность дефектуры, дней',
        'Количество приходов после дефектуры у конкурентов (всего)',
        'Кол-во конкурентов с приходами',
        'Общий объём прихода после дефектуры',
        'Объём прихода Пульс (сумма)',
        'Объём прихода Катрен (сумма)',
        'Объём прихода Протек (сумма)',
        'Объём прихода Фармкомплект (сумма)',
        'Остаток Пульс (вчера)',
        'Остаток Катрен (вчера)',
        'Остаток Протек (вчера)',
        'Остаток Фармкомплект (вчера)',
        'Остаток ФК Гранд Капитал (последняя дата)',
        'Длительность текущие',
        'Длительность завершившиеся',
    ]
    
    for col in int_cols:
        if col in df_final.columns:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0).astype(int)
    
    float_cols = ['ЭО', 'СВСС', 'Потери, руб']
    for col in float_cols:
        if col in df_final.columns:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0).round(2)
    
    text_cols = [
        'Конкуренты с приходами',
        'Приходы Пульс (дата-объём)',
        'Приходы Катрен (дата-объём)',
        'Приходы Протек (дата-объём)',
        'Приходы Фармкомплект (дата-объём)',
    ]
    
    for col in text_cols:
        if col in df_final.columns:
            df_final[col] = df_final[col].fillna('0').astype(str).replace({'nan': '0', '': '0'})
    
    # ================================================================
    # 8. УПОРЯДОЧИВАНИЕ КОЛОНОК
    # ================================================================
    final_column_order = [
        'Код КАГ',
        'Имя КАГ',
        'Рейтинг Внешний',
        'ЭО',
        'СВСС',
        'Прямой поставщик',
        'НОР',
        'Статус',
        'Дата входа в дефектуру ФК Гранд Капитал',
        'Дата окончания дефектуры ФК Гранд Капитал',
        'Длительность дефектуры, дней',
        'Длительность текущие',
        'Длительность завершившиеся',
        'Потери, руб',
        'Количество приходов после дефектуры у конкурентов (всего)',
        'Кол-во конкурентов с приходами',
        'Конкуренты с приходами',
        'Общий объём прихода после дефектуры',
        'Приходы Пульс (дата-объём)',
        'Приходы Катрен (дата-объём)',
        'Приходы Протек (дата-объём)',
        'Приходы Фармкомплект (дата-объём)',
        'Объём прихода Пульс (сумма)',
        'Объём прихода Катрен (сумма)',
        'Объём прихода Протек (сумма)',
        'Объём прихода Фармкомплект (сумма)',
        'Остаток Пульс (вчера)',
        'Остаток Катрен (вчера)',
        'Остаток Протек (вчера)',
        'Остаток Фармкомплект (вчера)',
        'Остаток ФК Гранд Капитал (последняя дата)',
    ]
    
    existing_cols = [c for c in final_column_order if c in df_final.columns]
    df_final = df_final[existing_cols]
    
    # ================================================================
    # 9. РАЗДЕЛЕНИЕ НА ЛИСТЫ И СОХРАНЕНИЕ
    # ================================================================
    print("\n📄 Разделение по листам...")
    
    df_sheet_current = df_final[df_final['Статус'] == 'Текущая'].copy()
    df_sheet_finished = df_final[df_final['Статус'] == 'Завершилась'].copy()
    
    if output_path:
        output_path = Path(output_path)
        xlsx_path = output_path.with_suffix('.xlsx')
        parquet_path = output_path.with_suffix('.parquet')
        
        # Находим новые позиции (сравнение с резервом)
        if backup_dir:
            df_sheet_new = find_new_positions(
                df_sheet_current, backup_dir, xlsx_path.name
            )
        else:
            df_sheet_new = df_sheet_current.copy()
        
        # Сохраняем Excel с тремя листами
        save_multi_sheet(df_sheet_current, df_sheet_new, df_sheet_finished, xlsx_path)
        print(f"\n✅ Сохранено: {xlsx_path}")
        
        # Parquet — полный датасет
        df_final.to_parquet(parquet_path, index=False)
        print(f"✅ Сохранено: {parquet_path}")
    
    # ================================================================
    # 10. СТАТИСТИКА
    # ================================================================
    print("\n" + "=" * 60)
    print("ИТОГО")
    print("=" * 60)
    print(f"📊 Всего: {len(df_final):,}")
    print(f"   🔴 Текущих: {len(df_sheet_current):,}")
    print(f"   🟢 Завершившихся: {len(df_sheet_finished):,}")
    print(f"   ⭐ С рейтингом: {df_final['Рейтинг Внешний'].notna().sum():,}")
    print(f"   📈 С ЭО: {(df_final['ЭО'] > 0).sum():,}")
    print(f"   📈 С СВСС: {(df_final['СВСС'] > 0).sum():,}")
    print(f"   🏭 С прямым поставщиком: {df_final['Прямой поставщик'].notna().sum():,}")
    print(f"   📋 С НОР: {df_final['НОР'].notna().sum():,}")
    print(f"   💰 Общие потери: {df_final['Потери, руб'].sum():,.2f} руб")
    print("=" * 60)
    
    return df_final


# ================================================================
# ЗАПУСК
# ================================================================
if __name__ == '__main__':
    
    EPISODES_FILE = r'C:\Проекты\Project_etl_power_bi\data\result\final_table_episodes.xlsx'
    LAST_POINT_FILE = r'C:\Проекты\Project_etl_power_bi\data\result\final_table_last_point.xlsx'
    CODEX_FILE = r'C:\Проекты\Project_etl_power_bi\data\result\codex.xlsx'
    EO_FILE = r'C:\Проекты\Project_etl_power_bi\data\result\EO.xlsx'
    DICT_RANK_FILE = r'C:\Проекты\Project_etl_power_bi\data\result\dict_rank.xlsx'
    NOR_FILE = r'C:\Проекты\Project_etl_power_bi\data\result\nor.xlsx'
    OUTPUT_FILE = r'C:\Проекты\Project_etl_power_bi\data\result\final_merged_table'
    BACKUP_DIR = r'C:\Проекты\Project_etl_power_bi\data\result\резерв'
    
    result = merge_defectura_tables(
        episodes_path=EPISODES_FILE,
        last_point_path=LAST_POINT_FILE,
        codex_path=CODEX_FILE,
        eo_path=EO_FILE,
        dict_rank_path=DICT_RANK_FILE,
        nor_path=NOR_FILE,
        output_path=OUTPUT_FILE,
        backup_dir=BACKUP_DIR
    )
    
    print("\n✅ Готово!")