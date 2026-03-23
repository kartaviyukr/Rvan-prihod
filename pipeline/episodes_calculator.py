"""
Расчет эпизодов дефектуры (standalone-версия)

Использует пороги из Config:
    ЭО >= MIN_EO_FOR_PCT  ->  дефектура при ГК < DEFECT_EO_PCT * ЭО
    ЭО < MIN_EO_FOR_PCT   ->  дефектура только при ГК == 0
"""
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from config import Config, CategoryConfig


def _load_eo_map() -> dict:
    """Загружает {код_каг_str: ЭО_float} из Excel"""
    try:
        df_eo = pd.read_excel(Config.EO_FILE)
    except FileNotFoundError:
        print(f"Файл ЭО не найден: {Config.EO_FILE}, используется логика ГК == 0")
        return {}

    def norm(x):
        try:
            return str(int(float(str(x).strip())))
        except Exception:
            return str(x).strip()

    eo_col = next((c for c in ['ЭО общая', 'ЭО'] if c in df_eo.columns), None)
    if eo_col is None:
        return {}

    df_eo[Config.COL_KAG] = df_eo[Config.COL_KAG].apply(norm)
    df_eo[eo_col] = pd.to_numeric(df_eo[eo_col], errors='coerce').fillna(0)
    df_eo = df_eo.drop_duplicates(subset=[Config.COL_KAG], keep='first')

    eo_map = dict(zip(df_eo[Config.COL_KAG], df_eo[eo_col]))
    print(f"Загружен справочник ЭО: {len(eo_map):,} КАГ")
    return eo_map


def _get_threshold(kag, eo_map: dict) -> float:
    kag_str = str(kag).strip()
    try:
        kag_str = str(int(float(kag_str)))
    except Exception:
        pass
    eo = eo_map.get(kag_str, 0.0)
    return eo * Config.DEFECT_EO_PCT if eo >= Config.MIN_EO_FOR_PCT else 0.0


def calculate_episodes(df: pd.DataFrame) -> pd.DataFrame:
    """Рассчитывает эпизоды дефектуры с детекцией прихода"""

    # Валидация
    need = [Config.COL_DATE, Config.COL_KAG, Config.COL_GK, *Config.COMP_COLS]
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise KeyError(f"Отсутствуют колонки: {miss}")

    # Подготовка
    d = df.copy()
    d[Config.COL_DATE] = pd.to_datetime(d[Config.COL_DATE], errors="coerce")
    d = d.dropna(subset=[Config.COL_DATE])
    d[Config.COL_KAG] = pd.to_numeric(d[Config.COL_KAG], errors="coerce")
    d = d.dropna(subset=[Config.COL_KAG])
    d[Config.COL_KAG] = d[Config.COL_KAG].astype(np.int64).astype(str)

    for c in [Config.COL_GK, *Config.COMP_COLS]:
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0.0)

    d["date_n"] = d[Config.COL_DATE].dt.normalize()
    d = d.sort_values([Config.COL_KAG, "date_n", Config.COL_DATE]).reset_index(drop=True)
    d = d.drop_duplicates(subset=[Config.COL_KAG, "date_n"], keep="last").reset_index(drop=True)
    d["sum_competitors"] = d[Config.COMP_COLS].sum(axis=1)

    work_date = d["date_n"].max()
    print(f"work_date = {work_date.date()}")

    eo_map = _load_eo_map()

    # Eligible КАГ
    last30_start = (work_date - pd.Timedelta(days=Config.LAST30_DAYS - 1)).normalize()
    gk_ever_positive = d.groupby(Config.COL_KAG)[Config.COL_GK].max() > 0
    obs_last30 = d[d["date_n"] >= last30_start].groupby(Config.COL_KAG)["date_n"].size()
    has_enough = obs_last30 >= Config.MIN_OBS_LAST30

    eligible_kags = [
        k for k in gk_ever_positive.index
        if bool(gk_ever_positive.get(k, False)) and bool(has_enough.get(k, False))
    ]
    print(f"Eligible КАГ: {len(eligible_kags):,}")

    # Numpy-массивы
    kag_store = {}
    for kag, one in tqdm(
        d[d[Config.COL_KAG].isin(eligible_kags)].groupby(Config.COL_KAG, sort=False),
        total=len(eligible_kags), desc="Подготовка массивов",
    ):
        kag_store[kag] = {
            "dates": one["date_n"].to_numpy(),
            "gk": one[Config.COL_GK].to_numpy(dtype=float),
            "sumc": one["sum_competitors"].to_numpy(dtype=float),
            "comps": {c: one[c].to_numpy(dtype=float) for c in Config.COMP_COLS},
        }

    name_map = d.groupby(Config.COL_KAG)[Config.COL_KAG_NAME].first().to_dict() \
        if Config.COL_KAG_NAME in d.columns else {}
    last_date_map = d.groupby(Config.COL_KAG)["date_n"].max().to_dict()
    yesterday = (work_date - pd.Timedelta(days=1)).normalize()

    # Эпизоды + приходы
    lookback_start = (work_date - pd.Timedelta(days=Config.LOOKBACK_DEFECTS_DAYS - 1)).normalize()
    all_rows = []

    for kag in tqdm(eligible_kags, desc="Обработка эпизодов"):
        st = kag_store.get(kag)
        if st is None:
            continue

        dates, gk, comps = st["dates"], st["gk"], st["comps"]
        if len(gk) < 2:
            continue

        threshold = _get_threshold(kag, eo_map)
        in_defect = gk < threshold
        prev_in = np.r_[False, in_defect[:-1]]
        start_indices = np.where(in_defect & ~prev_in)[0]
        if start_indices.size == 0:
            continue

        next_ok = np.full(len(gk), -1, dtype=int)
        nxt = -1
        for i in range(len(gk) - 1, -1, -1):
            next_ok[i] = nxt
            if not in_defect[i]:
                nxt = i

        for si in start_indices:
            start_date = pd.Timestamp(dates[si]).normalize()
            if start_date < lookback_start or start_date > work_date:
                continue

            ei = next_ok[si]
            end_date = pd.Timestamp(dates[ei]).normalize() if ei != -1 else pd.NaT
            is_finished = ei != -1

            start_np = np.datetime64(start_date)
            end_np = np.datetime64(end_date if is_finished else work_date)

            left = np.searchsorted(dates, start_np, side="left") + 1
            right = np.searchsorted(dates, end_np, side="left" if is_finished else "right")

            row = {
                Config.COL_KAG: kag,
                Config.COL_KAG_NAME: name_map.get(kag, ''),
                "defect_start_date": start_date,
                "defect_end_date": end_date,
                "is_finished": is_finished,
                "Последняя дата КАГ": last_date_map.get(kag),
                "Длительность дефектуры, дней": (
                    (end_date - start_date).days if is_finished else (work_date - start_date).days
                ),
            }

            if right - left < 2:
                row.update({"arrival_flag": False, "arrival_events_cnt": 0,
                            "arrival_first_date": pd.NaT, "arrival_competitor": None,
                            "Приходов после дефектуры (всего)": 0,
                            "Общий объём прихода после дефектуры": 0,
                            "Кол-во конкурентов с приходами": 0,
                            "Конкуренты с приходами": '0'})
                for cc in Config.COMP_COLS:
                    p = Config.COMP_PRETTY.get(cc, cc)
                    row[f'Приходы {p} (дата-объём)'] = '0'
                    row[f'Объём прихода {p} (сумма)'] = 0
            else:
                first_hit_by_comp = {}
                total_hits, total_volume = 0, 0
                comps_with_arrivals = []

                for cc in Config.COMP_COLS:
                    p = Config.COMP_PRETTY.get(cc, cc)
                    arr = comps[cc][left:right]
                    if len(arr) < 2:
                        row[f'Приходы {p} (дата-объём)'] = '0'
                        row[f'Объём прихода {p} (сумма)'] = 0
                        continue

                    dlt = np.diff(arr)
                    thr_pct = Config.MIN_PCT_FROM_YESTERDAY * arr[:-1]
                    hit_idx = np.where((dlt >= Config.DELTA_ARRIVAL) & (dlt >= thr_pct))[0]

                    if len(hit_idx) == 0:
                        row[f'Приходы {p} (дата-объём)'] = '0'
                        row[f'Объём прихода {p} (сумма)'] = 0
                        continue

                    comps_with_arrivals.append(p)
                    total_hits += len(hit_idx)
                    first_hit_by_comp[cc] = pd.Timestamp(dates[left + hit_idx[0] + 1]).normalize()

                    events, vol_sum = [], 0
                    for idx in hit_idx:
                        ev_date = pd.Timestamp(dates[left + idx + 1]).normalize()
                        vol = float(dlt[idx])
                        vol_sum += vol
                        events.append(f"{ev_date.strftime('%d.%m.%y')} - {int(vol)} шт")

                    total_volume += vol_sum
                    row[f'Приходы {p} (дата-объём)'] = '; '.join(events)
                    row[f'Объём прихода {p} (сумма)'] = int(vol_sum)

                row["Приходов после дефектуры (всего)"] = total_hits
                row["Общий объём прихода после дефектуры"] = int(total_volume)
                row["Кол-во конкурентов с приходами"] = len(comps_with_arrivals)
                row["Конкуренты с приходами"] = '; '.join(comps_with_arrivals) or '0'

                if first_hit_by_comp:
                    row["arrival_flag"] = True
                    row["arrival_events_cnt"] = total_hits
                    min_date = min(first_hit_by_comp.values())
                    row["arrival_first_date"] = min_date
                    row["arrival_competitor"] = '; '.join(
                        Config.COMP_PRETTY.get(c, c) for c, dt in first_hit_by_comp.items()
                        if dt == min_date)
                else:
                    row.update({"arrival_flag": False, "arrival_events_cnt": 0,
                                "arrival_first_date": pd.NaT, "arrival_competitor": None})

            # Остатки
            yesterday_np = np.datetime64(yesterday)
            idx_y = np.searchsorted(dates, yesterday_np, side="left")
            for cc in Config.COMP_COLS:
                p = Config.COMP_PRETTY.get(cc, cc)
                row[f'Остаток {p} (вчера)'] = (
                    int(comps[cc][idx_y]) if idx_y < len(dates) and dates[idx_y] == yesterday_np else 0
                )

            if len(dates) > 0:
                last_idx = len(dates) - 1
                for cc in Config.COMP_COLS:
                    p = Config.COMP_PRETTY.get(cc, cc)
                    row[f'Остаток {p} (последняя дата)'] = int(comps[cc][last_idx])
                row['Остаток ГК (последняя дата)'] = int(gk[last_idx])

            all_rows.append(row)

    if not all_rows:
        return pd.DataFrame()

    result_df = pd.DataFrame(all_rows)
    result_df = result_df[result_df["arrival_flag"]].copy()
    result_df = result_df.drop_duplicates(
        subset=[Config.COL_KAG, "defect_start_date", "defect_end_date"], keep="first").copy()

    # Форматирование
    result_df["Категория"] = (
        (work_date - result_df["defect_start_date"]).dt.days
        .apply(lambda d: CategoryConfig.categorize_by_days_ago(int(d)) if pd.notna(d) else "Неизвестно")
    )
    result_df["Статус дефектуры"] = np.where(result_df["is_finished"], "Закончившаяся", "Активная")
    result_df["Дата входа в дефектуру ГК"] = result_df["defect_start_date"]
    result_df["Дата выхода из дефектуры ГК"] = result_df["defect_end_date"]
    result_df["Дата прихода у конкурента"] = result_df["arrival_first_date"]
    result_df["Конкурент (первый приход)"] = result_df["arrival_competitor"]
    result_df["Лаг реакции, дней"] = (result_df["arrival_first_date"] - result_df["defect_start_date"]).dt.days
    result_df["Дата остатков конкурентов (вчера)"] = yesterday
    result_df["Последняя дата (КАГ)"] = result_df[Config.COL_KAG].map(last_date_map)

    # Порядок и формат дат
    output_cols = [
        Config.COL_KAG, Config.COL_KAG_NAME, "Категория", "Статус дефектуры",
        "Последняя дата КАГ", "Дата входа в дефектуру ГК", "Дата выхода из дефектуры ГК",
        "Длительность дефектуры, дней", "Приходов после дефектуры (всего)",
        "Кол-во конкурентов с приходами", "Конкуренты с приходами",
        "Общий объём прихода после дефектуры",
    ]
    for p in ["Пульс", "Катрен", "Протек", "Фармкомплект"]:
        output_cols.append(f"Приходы {p} (дата-объём)")
    for p in ["Пульс", "Катрен", "Протек", "Фармкомплект"]:
        output_cols.append(f"Объём прихода {p} (сумма)")
    output_cols.append("Дата остатков конкурентов (вчера)")
    for p in ["Пульс", "Катрен", "Протек", "Фармкомплект"]:
        output_cols.append(f"Остаток {p} (вчера)")
    output_cols.append("Последняя дата (КАГ)")
    for p in ["Пульс", "Катрен", "Протек", "Фармкомплект"]:
        output_cols.append(f"Остаток {p} (последняя дата)")
    output_cols.extend([
        "Остаток ГК (последняя дата)", "Дата прихода у конкурента",
        "Лаг реакции, дней", "Конкурент (первый приход)",
        "arrival_events_cnt", "arrival_flag", "arrival_competitor", "is_finished",
    ])

    final = result_df[[c for c in output_cols if c in result_df.columns]].copy()

    for col in ["Последняя дата КАГ", "Дата входа в дефектуру ГК", "Дата выхода из дефектуры ГК",
                "Дата прихода у конкурента", "Дата остатков конкурентов (вчера)", "Последняя дата (КАГ)"]:
        if col in final.columns:
            final[col] = pd.to_datetime(final[col], errors='coerce').dt.strftime('%d.%m.%y').fillna('')

    return final.sort_values(
        ["Статус дефектуры", "Дата входа в дефектуру ГК"], ascending=[True, False]
    ).reset_index(drop=True)


if __name__ == '__main__':
    DATA_FILE = str(Config.CLEAN_PARQUET)
    print(f"Загрузка: {DATA_FILE}")
    df = pd.read_parquet(DATA_FILE) if DATA_FILE.endswith('.parquet') else pd.read_excel(DATA_FILE)
    print(f"Загружено: {len(df):,} строк")

    result = calculate_episodes(df)

    Config.OUT_DIR.mkdir(parents=True, exist_ok=True)
    result.to_excel(Config.OUT_DIR / 'final_table_episodes.xlsx', index=False)
    result.to_parquet(Config.OUT_DIR / 'final_table_episodes.parquet', index=False)
    print(f"Записей: {len(result):,}")
