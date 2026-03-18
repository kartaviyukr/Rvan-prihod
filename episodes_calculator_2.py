"""
Расчёт эпизодов дефектуры (ИСПРАВЛЕННАЯ ВЕРСИЯ)
"""

import numpy as np
import pandas as pd
from pathlib import Path
from tqdm.auto import tqdm


# ==========================================================
# НАСТРОЙКИ
# ==========================================================
MIN_OBS_LAST30 =5
LAST30_DAYS = 30
LOOKBACK_DEFECTS_DAYS = 90

DELTA_ARRIVAL = 100
MIN_PCT_FROM_YESTERDAY = 0.20

# ==========================================================
# КОЛОНКИ
# ==========================================================
COL_DATE = "Дата"
COL_KAG = "Код КАГ"
COL_KAG_NAME = "Имя КАГ"

COL_GK = "ГК (остатки гранд капитала)"
COL_PULS = "Пульс (остатки пульса)"
COL_KATREN = "Катрен (остатки катрена)"
COL_PROTEK = "Протек (остатки протека)"
COL_FK = "Фармкомплект (остатки фармкомплекта)"

COMP_COLS = [COL_PULS, COL_KATREN, COL_PROTEK, COL_FK]

COMP_PRETTY = {
    COL_PULS: "Пульс",
    COL_KATREN: "Катрен",
    COL_PROTEK: "Протек",
    COL_FK: "Фармкомплект",
}

# Пути
DATA_FILE = r'C:\Проекты\Project_etl_power_bi\data\preproc_parquet\big_data_clean.parquet'
OUTPUT_DIR = Path(r'C:\Проекты\Project_etl_power_bi\data\result')


def calculate_episodes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Рассчитывает эпизоды дефектуры по проверенной логике
    """
    
    print("=" * 60)
    print("РАСЧЁТ ЭПИЗОДОВ ДЕФЕКТУРЫ")
    print("=" * 60)
    
    # ==========================================================
    # 0) ВАЛИДАЦИЯ И ПОДГОТОВКА
    # ==========================================================
    need = [COL_DATE, COL_KAG, COL_GK, *COMP_COLS]
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise KeyError(f"В df не хватает колонок: {miss}")

    d = df.copy()

    d[COL_DATE] = pd.to_datetime(d[COL_DATE], errors="coerce")
    d = d.dropna(subset=[COL_DATE])

    d[COL_KAG] = pd.to_numeric(d[COL_KAG], errors="coerce")
    d = d.dropna(subset=[COL_KAG])
    d[COL_KAG] = d[COL_KAG].astype(np.int64).astype(str)

    for c in [COL_GK, *COMP_COLS]:
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0.0)

    d["date_n"] = d[COL_DATE].dt.normalize()
    d = d.sort_values([COL_KAG, "date_n", COL_DATE]).reset_index(drop=True)
    d = d.drop_duplicates(subset=[COL_KAG, "date_n"], keep="last").reset_index(drop=True)
    d["sum_competitors"] = d[COMP_COLS].sum(axis=1)

    work_date = d["date_n"].max()
    print(f"\n📅 work_date = {work_date.date()}")

    # ==========================================================
    # 1) ОТСЕЧКИ БАЗОВЫЕ
    # ==========================================================
    last30_start = (work_date - pd.Timedelta(days=LAST30_DAYS - 1)).normalize()
    print(f"🧾 last30_start = {last30_start.date()} | MIN_OBS_LAST30 = {MIN_OBS_LAST30}")

    gk_ever_positive = d.groupby(COL_KAG)[COL_GK].max() > 0
    is_in_last30 = d["date_n"] >= last30_start
    obs_last30 = d[is_in_last30].groupby(COL_KAG)["date_n"].size()
    has_enough_history = obs_last30 >= MIN_OBS_LAST30

    eligible_kags = [
        k for k in gk_ever_positive.index
        if bool(gk_ever_positive.get(k, False)) and bool(has_enough_history.get(k, False))
    ]

    print(f"\n✅ Eligible КАГ: {len(eligible_kags):,}")

    # ==========================================================
    # 2) БЫСТРЫЙ ДОСТУП: numpy-массивы
    # ==========================================================
    kag_store = {}

    for kag, one in tqdm(d[d[COL_KAG].isin(eligible_kags)].groupby(COL_KAG, sort=False),
                         total=len(eligible_kags), desc="▶ Подготовка массивов"):
        kag_store[kag] = {
            "dates": one["date_n"].to_numpy(),
            "gk": one[COL_GK].to_numpy(dtype=float),
            "sumc": one["sum_competitors"].to_numpy(dtype=float),
            "comps": {c: one[c].to_numpy(dtype=float) for c in COMP_COLS},
        }

    # Маппинги
    if COL_KAG_NAME in d.columns:
        name_map = d.groupby(COL_KAG)[COL_KAG_NAME].first().to_dict()
    else:
        name_map = {}
    
    last_date_map = d.groupby(COL_KAG)["date_n"].max().to_dict()
    yesterday = (work_date - pd.Timedelta(days=1)).normalize()

    # ==========================================================
    # 3) ЭПИЗОДЫ ДЕФЕКТУР + ДЕТЕКТ ПРИХОДА (ВСЁ В ОДНОМ ЦИКЛЕ)
    # ==========================================================
    lookback_start = (work_date - pd.Timedelta(days=LOOKBACK_DEFECTS_DAYS - 1)).normalize()
    print(f"\n🕘 Ищем старты дефектур: {lookback_start.date()} → {work_date.date()}")

    all_rows = []

    for kag in tqdm(eligible_kags, desc="▶ Обработка эпизодов", total=len(eligible_kags)):
        st = kag_store.get(kag)
        if st is None:
            continue

        dates = st["dates"]
        gk = st["gk"]
        sumc = st["sumc"]
        comps = st["comps"]

        if len(gk) < 2:
            continue

        # Находим старты дефектур
        prev_gk = np.r_[np.nan, gk[:-1]]
        start_indices = np.where((gk == 0) & (prev_gk > 0))[0]
        
        if start_indices.size == 0:
            continue

        # Precompute next positive index
        next_pos = np.full(len(gk), -1, dtype=int)
        nxt = -1
        for i in range(len(gk) - 1, -1, -1):
            next_pos[i] = nxt
            if gk[i] > 0:
                nxt = i

        # Обрабатываем каждый эпизод
        for si in start_indices:
            start_date = pd.Timestamp(dates[si]).normalize()

            if start_date < lookback_start or start_date > work_date:
                continue

            ei = next_pos[si]
            if ei == -1:
                end_date = pd.NaT
                is_finished = False
            else:
                end_date = pd.Timestamp(dates[ei]).normalize()
                is_finished = True

            # === ДЕТЕКТ ПРИХОДА В ОКНЕ ЭТОГО ЭПИЗОДА ===
            start_np = np.datetime64(start_date)
            if pd.isna(end_date):
                end_np = np.datetime64(work_date)
            else:
                end_np = np.datetime64(end_date)

            left = np.searchsorted(dates, start_np, side="left")
            # ============================================================
            # ИСПРАВЛЕНИЕ 1: Пропускаем день входа в дефектуру.
            # Засчитываем приходы только со СЛЕДУЮЩЕГО дня после дефектуры.
            # ============================================================
            left += 1
            
            # ============================================================
            # ИСПРАВЛЕНИЕ 2: Для ЗАКОНЧИВШИХСЯ дефектур исключаем дату выхода.
            # Если ГК вышел из дефектуры 19.12, приход у конкурента 19.12 не считаем.
            # Для АКТИВНЫХ дефектур — включаем work_date.
            # ============================================================
            if is_finished:
                right = np.searchsorted(dates, end_np, side="left")  # исключаем end_date
            else:
                right = np.searchsorted(dates, end_np, side="right")  # включаем work_date

            # Базовые данные эпизода
            row = {
                COL_KAG: kag,
                COL_KAG_NAME: name_map.get(kag, ''),
                "defect_start_date": start_date,
                "defect_end_date": end_date,
                "is_finished": is_finished,
                "Последняя дата КАГ": last_date_map.get(kag),
            }

            # Длительность
            if is_finished:
                row["Длительность дефектуры, дней"] = (end_date - start_date).days
            else:
                row["Длительность дефектуры, дней"] = (work_date - start_date).days

            # Детект прихода
            # Теперь требуем минимум 2 точки ПОСЛЕ start_date (т.к. left сдвинут на 1)
            if right - left < 2:
                row["arrival_flag"] = False
                row["arrival_events_cnt"] = 0
                row["arrival_first_date"] = pd.NaT
                row["arrival_competitor"] = None
                row["Приходов после дефектуры (всего)"] = 0
                row["Общий объём прихода после дефектуры"] = 0
                row["Кол-во конкурентов с приходами"] = 0
                row["Конкуренты с приходами"] = '0'
                for comp_col in COMP_COLS:
                    pretty = COMP_PRETTY.get(comp_col, comp_col)
                    row[f'Приходы {pretty} (дата-объём)'] = '0'
                    row[f'Объём прихода {pretty} (сумма)'] = 0
            else:
                first_hit_by_comp = {}
                total_hits = 0
                total_volume = 0
                max_delta_global = None
                comps_with_arrivals = []

                for comp_col in COMP_COLS:
                    pretty = COMP_PRETTY.get(comp_col, comp_col)
                    arr = comps[comp_col][left:right]
                    
                    if len(arr) < 2:
                        row[f'Приходы {pretty} (дата-объём)'] = '0'
                        row[f'Объём прихода {pretty} (сумма)'] = 0
                        continue
                    
                    prev_arr = arr[:-1]
                    dlt = np.diff(arr)
                    thr_pct = MIN_PCT_FROM_YESTERDAY * prev_arr

                    hit_mask = (dlt >= DELTA_ARRIVAL) & (dlt >= thr_pct)
                    hit_idx = np.where(hit_mask)[0]

                    if len(hit_idx) == 0:
                        row[f'Приходы {pretty} (дата-объём)'] = '0'
                        row[f'Объём прихода {pretty} (сумма)'] = 0
                        continue

                    comps_with_arrivals.append(pretty)
                    total_hits += len(hit_idx)

                    local_max = float(np.max(dlt[hit_idx]))
                    if max_delta_global is None or local_max > max_delta_global:
                        max_delta_global = local_max

                    # Первая дата прихода
                    j = int(hit_idx[0] + 1)
                    hit_date = pd.Timestamp(dates[left + j]).normalize()
                    first_hit_by_comp[comp_col] = hit_date

                    # Детали прихода
                    events = []
                    vol_sum = 0
                    for idx in hit_idx:
                        j = idx + 1
                        ev_date = pd.Timestamp(dates[left + j]).normalize()
                        vol = float(dlt[idx])
                        vol_sum += vol
                        events.append(f"{ev_date.strftime('%d.%m.%y')} - {int(vol)} шт")

                    total_volume += vol_sum
                    row[f'Приходы {pretty} (дата-объём)'] = '; '.join(events)
                    row[f'Объём прихода {pretty} (сумма)'] = int(vol_sum)

                row["Приходов после дефектуры (всего)"] = total_hits
                row["Общий объём прихода после дефектуры"] = int(total_volume)
                row["Кол-во конкурентов с приходами"] = len(comps_with_arrivals)
                row["Конкуренты с приходами"] = '; '.join(comps_with_arrivals) if comps_with_arrivals else '0'

                if first_hit_by_comp:
                    row["arrival_flag"] = True
                    row["arrival_events_cnt"] = total_hits
                    min_date = min(first_hit_by_comp.values())
                    row["arrival_first_date"] = min_date
                    comps_on_min = [COMP_PRETTY.get(c, c) for c, dt in first_hit_by_comp.items() if dt == min_date]
                    row["arrival_competitor"] = '; '.join(comps_on_min)
                else:
                    row["arrival_flag"] = False
                    row["arrival_events_cnt"] = 0
                    row["arrival_first_date"] = pd.NaT
                    row["arrival_competitor"] = None

            # === ОСТАТКИ НА ВЧЕРА И ПОСЛЕДНЮЮ ДАТУ ===
            yesterday_np = np.datetime64(yesterday)
            idx_yesterday = np.searchsorted(dates, yesterday_np, side="left")
            
            if idx_yesterday < len(dates) and dates[idx_yesterday] == yesterday_np:
                for comp_col in COMP_COLS:
                    pretty = COMP_PRETTY.get(comp_col, comp_col)
                    row[f'Остаток {pretty} (вчера)'] = int(comps[comp_col][idx_yesterday])
            else:
                for comp_col in COMP_COLS:
                    pretty = COMP_PRETTY.get(comp_col, comp_col)
                    row[f'Остаток {pretty} (вчера)'] = 0

            if len(dates) > 0:
                last_idx = len(dates) - 1
                for comp_col in COMP_COLS:
                    pretty = COMP_PRETTY.get(comp_col, comp_col)
                    row[f'Остаток {pretty} (последняя дата)'] = int(comps[comp_col][last_idx])
                row['Остаток ГК (последняя дата)'] = int(gk[last_idx])

            all_rows.append(row)

    if not all_rows:
        print("⚠️ Эпизодов не найдено")
        return pd.DataFrame()

    result_df = pd.DataFrame(all_rows)

    print(f"\n📊 Эпизодов найдено: {len(result_df):,}")
    print(f"   С приходом: {result_df['arrival_flag'].sum():,}")
    print(f"   Без прихода: {(~result_df['arrival_flag']).sum():,}")

    # ==========================================================
    # 4) ФИЛЬТРАЦИЯ: ТОЛЬКО С ПРИХОДОМ
    # ==========================================================
    result_df = result_df[result_df["arrival_flag"]].copy()
    print(f"\n✅ Оставлено эпизодов с приходом: {len(result_df):,}")

    # ==========================================================
    # 5) УДАЛЕНИЕ ДУБЛИКАТОВ
    # ==========================================================
    before_dedup = len(result_df)
    result_df = result_df.drop_duplicates(
        subset=[COL_KAG, "defect_start_date", "defect_end_date"],
        keep="first"
    ).copy()
    print(f"❌ Удалено дубликатов: {before_dedup - len(result_df):,}")

    # ==========================================================
    # 6) ФИНАЛЬНОЕ ФОРМАТИРОВАНИЕ
    # ==========================================================
    
    # Категория
    def categorize(days):
        if pd.isna(days):
            return "Неизвестно"
        days = int(days)
        if days <= 40:
            return "Мы вошли в дефектуру 1–40 дней назад"
        elif days <= 90:
            return "Мы вошли в дефектуру 41–90 дней назад"
        else:
            return "Мы вошли в дефектуру более 90 дней назад"

    result_df["Категория"] = result_df["Длительность дефектуры, дней"].apply(
        lambda x: categorize((work_date - result_df.loc[result_df["Длительность дефектуры, дней"] == x, "defect_start_date"].iloc[0]).days) if pd.notna(x) else "Неизвестно"
    )
    
    # Пересчитываем категорию правильно
    result_df["Категория"] = (work_date - result_df["defect_start_date"]).dt.days.apply(categorize)

    result_df["Статус дефектуры"] = np.where(result_df["is_finished"], "Закончившаяся", "Активная")
    
    result_df["Дата входа в дефектуру ГК"] = result_df["defect_start_date"]
    result_df["Дата выхода из дефектуры ГК"] = result_df["defect_end_date"]
    result_df["Дата прихода у конкурента"] = result_df["arrival_first_date"]
    result_df["Конкурент (первый приход)"] = result_df["arrival_competitor"]
    
    result_df["Лаг реакции, дней"] = (
        result_df["arrival_first_date"] - result_df["defect_start_date"]
    ).dt.days

    result_df["Дата остатков конкурентов (вчера)"] = yesterday
    result_df["Последняя дата (КАГ)"] = result_df[COL_KAG].map(last_date_map)

    # Порядок колонок
    output_cols = [
        COL_KAG,
        COL_KAG_NAME,
        "Категория",
        "Статус дефектуры",
        "Последняя дата КАГ",
        "Дата входа в дефектуру ГК",
        "Дата выхода из дефектуры ГК",
        "Длительность дефектуры, дней",
        "Приходов после дефектуры (всего)",
        "Кол-во конкурентов с приходами",
        "Конкуренты с приходами",
        "Общий объём прихода после дефектуры",
        "Приходы Пульс (дата-объём)",
        "Приходы Катрен (дата-объём)",
        "Приходы Протек (дата-объём)",
        "Приходы Фармкомплект (дата-объём)",
        "Объём прихода Пульс (сумма)",
        "Объём прихода Катрен (сумма)",
        "Объём прихода Протек (сумма)",
        "Объём прихода Фармкомплект (сумма)",
        "Дата остатков конкурентов (вчера)",
        "Остаток Пульс (вчера)",
        "Остаток Катрен (вчера)",
        "Остаток Протек (вчера)",
        "Остаток Фармкомплект (вчера)",
        "Последняя дата (КАГ)",
        "Остаток Пульс (последняя дата)",
        "Остаток Катрен (последняя дата)",
        "Остаток Протек (последняя дата)",
        "Остаток Фармкомплект (последняя дата)",
        "Остаток ГК (последняя дата)",
        "Дата прихода у конкурента",
        "Лаг реакции, дней",
        "Конкурент (первый приход)",
        "arrival_events_cnt",
        "arrival_flag",
        "arrival_competitor",
        "is_finished",
    ]

    existing_cols = [c for c in output_cols if c in result_df.columns]
    final = result_df[existing_cols].copy()

    # Форматируем даты В САМОМ КОНЦЕ
    date_cols = [
        "Последняя дата КАГ",
        "Дата входа в дефектуру ГК",
        "Дата выхода из дефектуры ГК",
        "Дата прихода у конкурента",
        "Дата остатков конкурентов (вчера)",
        "Последняя дата (КАГ)",
    ]

    for col in date_cols:
        if col in final.columns:
            final[col] = pd.to_datetime(final[col], errors='coerce').dt.strftime('%d.%m.%y')
            final[col] = final[col].fillna('')

    # Сортировка
    final = final.sort_values(
        ["Статус дефектуры", "Дата входа в дефектуру ГК"],
        ascending=[True, False]
    ).reset_index(drop=True)

    return final


# ==========================================================
# ЗАПУСК
# ==========================================================
if __name__ == '__main__':
    
    print("📂 Загрузка данных...")
    
    if DATA_FILE.endswith('.parquet'):
        df = pd.read_parquet(DATA_FILE)
    elif DATA_FILE.endswith('.csv'):
        df = pd.read_csv(DATA_FILE)
    else:
        df = pd.read_excel(DATA_FILE)
    
    print(f"✅ Загружено: {len(df):,} строк")
    
    result = calculate_episodes(df)
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    xlsx_path = OUTPUT_DIR / 'final_table_episodes.xlsx'
    parquet_path = OUTPUT_DIR / 'final_table_episodes.parquet'
    
    result.to_excel(xlsx_path, index=False)
    result.to_parquet(parquet_path, index=False)
    
    print("\n" + "=" * 60)
    print("ГОТОВО!")
    print("=" * 60)
    print(f"✅ {xlsx_path}")
    print(f"✅ {parquet_path}")
    print(f"📊 Записей: {len(result):,}")
    print("=" * 60)