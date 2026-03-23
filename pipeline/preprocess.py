"""
Предобработка данных: нормализация, агрегация, очистка

Пайплайн:
    1. base_action()                       - фильтрация активных КАГ, дат
    2. collapse_kag_daily_smart()           - агрегация до 1 строки на (КАГ, Дата)
    3. zero_small_stocks_conditional()      - обнуление мелких остатков
    4. drop_weekends_and_holidays()         - удаление выходных/праздников
    5. fix_competitor_drop_to_zero_anomalies() - интерполяция аномальных нулей
"""
import time

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from config import Config


def _log(msg: str, show: bool):
    if show:
        print(msg)


# ---------------------------------------------------------------
# 1. Базовая фильтрация
# ---------------------------------------------------------------

def base_action(
    x: pd.DataFrame,
    name_kag: str = 'Активный КАГ',
    name_date: str = 'Дата',
    date_threshold: str = '2025-01-01',
) -> pd.DataFrame:
    """Фильтрует данные по активным КАГ и датам."""
    print(f"Исходный размер: {x.shape}")
    x = x[x[name_kag] == 'Да'].copy()
    x[name_date] = pd.to_datetime(x[name_date])
    x = x[x[name_date] > date_threshold]
    x = x.drop_duplicates()
    print(f"Конечный размер: {x.shape}")
    return x


# ---------------------------------------------------------------
# 2. Агрегация: 1 строка на (КАГ, Дата)
# ---------------------------------------------------------------

def _normalize_keys(x, *, date_col, kag_col, show_progress):
    _log("[1/6] Приведение типов (Дата, Код КАГ)", show_progress)
    x = x.copy()
    x[date_col] = pd.to_datetime(x[date_col], errors="coerce")
    x[kag_col] = pd.to_numeric(x[kag_col], errors="coerce")
    x = x.dropna(subset=[date_col, kag_col])
    x[kag_col] = x[kag_col].astype(np.int64).astype(str)
    _log(f"    Выход: {len(x):,}", show_progress)
    return x


def _normalize_numeric_cols(x, *, gk_col, comp_cols, price_cols, show_progress):
    _log("[2/6] Numeric приведение (остатки/цены)", show_progress)
    x = x.copy()
    for col in [gk_col] + comp_cols:
        x[col] = pd.to_numeric(x[col], errors="coerce").fillna(0)
    for col in price_cols:
        if col in x.columns:
            x[col] = pd.to_numeric(x[col], errors="coerce")
    return x


def _agg_gk_sum(x, *, gkeys, gk_col, show_progress):
    _log("[3/6] ГК: sum по (КАГ, Дата)", show_progress)
    return x.groupby(gkeys, sort=False)[gk_col].sum()


def _agg_competitors_dedup_or_sum(x, *, gkeys, comp_cols, show_progress):
    _log("[4/6] Конкуренты: dedup vs sum", show_progress)
    comp_sum = x.groupby(gkeys, sort=False)[comp_cols].sum()

    x_nonzero = x.copy()
    for col in comp_cols:
        x_nonzero.loc[x_nonzero[col] <= 0, col] = pd.NA
    comp_nunique = x_nonzero.groupby(gkeys, sort=False)[comp_cols].nunique()
    comp_max = x.groupby(gkeys, sort=False)[comp_cols].max()
    return comp_max.where(comp_nunique <= 1, comp_sum)


def _agg_prices_median_nonzero(x, *, gkeys, price_cols, show_progress):
    _log("[5/6] Цены: median по ненулевым", show_progress)
    prices_tmp = x[gkeys + price_cols].copy()
    for c in price_cols:
        prices_tmp[c] = prices_tmp[c].where(prices_tmp[c] > 0, np.nan)
    return prices_tmp.groupby(gkeys, sort=False)[price_cols].median().fillna(0)


def _build_daily_table(*, gk_sum, comp_final, prices_agg, date_col, kag_col, show_progress):
    _log("[6/6] Сбор финальной таблицы", show_progress)
    parts = [gk_sum, comp_final]
    if prices_agg is not None:
        parts.append(prices_agg)
    out = pd.concat(parts, axis=1).reset_index()
    return out.sort_values([kag_col, date_col]).reset_index(drop=True)


def _merge_kag_text_info(out, x, *, kag_col, name_kag_col, product_code_col,
                         product_name_col, show_progress):
    text_cols = [c for c in [name_kag_col, product_code_col, product_name_col]
                 if c in x.columns]
    if not text_cols:
        return out

    xx = x[[kag_col] + text_cols].copy()
    for c in text_cols:
        xx[c] = xx[c].astype("string").str.strip()
        xx.loc[xx[c].isin(["", "nan", "None"]), c] = pd.NA

    def first_non_empty(s):
        for v in s.dropna().astype(str):
            if v.strip():
                return v.strip()
        return ""

    def join_unique_fast(s):
        s = s.dropna().astype(str).str.strip()
        s = s[s != ""]
        return "; ".join(pd.unique(s)) if len(s) else ""

    kag_groups = xx.groupby(kag_col, sort=False)
    iterator = tqdm(kag_groups, total=kag_groups.ngroups, desc="   КАГ",
                    mininterval=0.5) if show_progress else kag_groups

    rows = []
    for kag, g in iterator:
        row = {kag_col: kag}
        if name_kag_col in g.columns:
            row[name_kag_col] = first_non_empty(g[name_kag_col])
        if product_code_col in g.columns:
            row[product_code_col] = join_unique_fast(g[product_code_col])
        if product_name_col in g.columns:
            row[product_name_col] = join_unique_fast(g[product_name_col])
        rows.append(row)

    return out.merge(pd.DataFrame(rows), on=kag_col, how="left")


def collapse_kag_daily_smart(
    df: pd.DataFrame,
    date_col: str = "Дата",
    kag_col: str = "Код КАГ",
    name_kag_col: str = "Имя КАГ",
    product_code_col: str = "Код товара",
    product_name_col: str = "Наименование у нас",
    gk_col: str = None,
    comp_cols: list = None,
    price_cols: list = None,
    show_progress: bool = True,
) -> pd.DataFrame:
    """
    Агрегирует данные до 1 строки на (Дата, Код КАГ).
    ГК: sum, Конкуренты: dedup/sum, Цены: median
    """
    t0 = time.time()

    if gk_col is None:
        gk_col = Config.COL_GK
    if comp_cols is None:
        comp_cols = Config.COMP_COLS
    if price_cols is None:
        price_cols = Config.PRICE_COLS

    if df.empty:
        raise ValueError("Входной DataFrame пустой")

    gkeys = [kag_col, date_col]

    x = _normalize_keys(df, date_col=date_col, kag_col=kag_col, show_progress=show_progress)
    if len(x) == 0:
        return pd.DataFrame()

    x = _normalize_numeric_cols(x, gk_col=gk_col, comp_cols=comp_cols,
                                price_cols=price_cols, show_progress=show_progress)

    gk_sum = _agg_gk_sum(x, gkeys=gkeys, gk_col=gk_col, show_progress=show_progress)
    comp_final = _agg_competitors_dedup_or_sum(x, gkeys=gkeys, comp_cols=comp_cols,
                                               show_progress=show_progress)
    prices_agg = (_agg_prices_median_nonzero(x, gkeys=gkeys, price_cols=price_cols,
                                             show_progress=show_progress) if price_cols else None)

    out = _build_daily_table(gk_sum=gk_sum, comp_final=comp_final, prices_agg=prices_agg,
                             date_col=date_col, kag_col=kag_col, show_progress=show_progress)

    out["weekday"] = out[date_col].dt.dayofweek

    out = _merge_kag_text_info(out, x, kag_col=kag_col, name_kag_col=name_kag_col,
                               product_code_col=product_code_col,
                               product_name_col=product_name_col,
                               show_progress=show_progress)

    _log(f"Готово: {out.shape} за {time.time() - t0:.1f}с", show_progress)
    return out


# ---------------------------------------------------------------
# 3. Обнуление мелких остатков
# ---------------------------------------------------------------

def zero_small_stocks_conditional(
    df: pd.DataFrame,
    *,
    stock_cols: list = None,
    kag_col: str = "Код КАГ",
    small_threshold: float = 10.0,
    apply_if_max_gt: float = 100.0,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Обнуляет остатки <= threshold только для КАГ,
    где max остаток > apply_if_max_gt.
    """
    if stock_cols is None:
        stock_cols = [Config.COL_GK, *Config.COMP_COLS]

    out = df.copy()
    max_per_kag = out.groupby(kag_col)[stock_cols].max()
    apply_mask = max_per_kag > apply_if_max_gt

    zeroed = 0
    for col in stock_cols:
        allowed = apply_mask.index[apply_mask[col]].tolist()
        mask = out[kag_col].isin(allowed) & (out[col] <= small_threshold)
        zeroed += mask.sum()
        out.loc[mask, col] = 0.0

    if verbose:
        print(f"Обнулено значений: {zeroed:,}")
    return out


# ---------------------------------------------------------------
# 4. Удаление выходных и праздников
# ---------------------------------------------------------------

def drop_weekends_and_holidays(
    df: pd.DataFrame,
    *,
    date_col: str = "Дата",
    holidays: list = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """Удаляет строки на выходных (сб/вс) и праздниках."""
    if holidays is None:
        holidays = Config.RUSSIAN_HOLIDAYS

    holidays = pd.to_datetime(holidays)
    is_weekend = df[date_col].dt.weekday.isin([5, 6])
    is_holiday = df[date_col].isin(holidays)

    out = df.loc[~(is_weekend | is_holiday)].copy()

    if verbose:
        print(f"Удалено: {len(df) - len(out):,} строк (выходные/праздники)")
    return out


# ---------------------------------------------------------------
# 5. Исправление аномалий конкурентов
# ---------------------------------------------------------------

def fix_competitor_drop_to_zero_anomalies(
    df: pd.DataFrame,
    *,
    kag_col: str = "Код КАГ",
    date_col: str = "Дата",
    competitor_cols: list = None,
    max_gap_days: int = 2,
    return_tol_pct: float = 0.05,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Интерполирует аномальные провалы в 0 у конкурентов.
    Если остаток упал в 0 на N дней, затем вернулся к уровню +-5% --
    заменяем на интерполяцию.
    """
    if competitor_cols is None:
        competitor_cols = Config.COMP_COLS

    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    out = out.dropna(subset=[kag_col, date_col])
    out = out.sort_values([kag_col, date_col]).reset_index(drop=True)

    for col in competitor_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)

    out["__market_zero"] = (out[competitor_cols].sum(axis=1) == 0)
    out['__pos'] = out.groupby(kag_col).cumcount()

    total_fixed = 0
    g = out.groupby(kag_col, sort=False)

    for col in competitor_cols:
        prev = g[col].shift(1)
        mz = out["__market_zero"]
        cur_eff_zero = (out[col] == 0) | mz
        fixed_col = 0

        # 1-day gap
        nxt1 = g[col].shift(-1)
        denom = prev.abs().clip(lower=1.0)
        rel_diff = ((nxt1 - prev).abs() / denom)
        is_gap_1 = (prev > 0) & cur_eff_zero & (nxt1 > 0) & (rel_diff <= return_tol_pct)
        idx_1 = is_gap_1[is_gap_1].index
        if len(idx_1):
            v = (prev.loc[idx_1] + nxt1.loc[idx_1]) / 2
            out.loc[idx_1, col] = np.round(v.values).astype(int)
            fixed_col += len(idx_1)

        # 2-day gap
        if max_gap_days >= 2:
            nxt2 = g[col].shift(-2)
            mz1 = g["__market_zero"].shift(-1).fillna(False)
            nxt1_eff_zero = (nxt1 == 0) | mz1
            denom2 = prev.abs().clip(lower=1.0)
            rel_diff2 = ((nxt2 - prev).abs() / denom2)
            is_gap_2 = (prev > 0) & cur_eff_zero & nxt1_eff_zero & (nxt2 > 0) & (rel_diff2 <= return_tol_pct)
            idx_2 = is_gap_2[is_gap_2].index
            if len(idx_2):
                pos_cur = out.loc[idx_2, '__pos'].values
                max_idx = (idx_2 + 1).max()
                if max_idx < len(out):
                    pos_next = out.loc[idx_2 + 1, '__pos'].values
                    valid = pos_next == pos_cur + 1
                    idx_2_safe = idx_2[valid]
                    if len(idx_2_safe):
                        delta = nxt2.loc[idx_2_safe] - prev.loc[idx_2_safe]
                        out.loc[idx_2_safe, col] = np.round(
                            (prev.loc[idx_2_safe] + delta * (1 / 3)).values
                        ).astype(int)
                        out.loc[idx_2_safe + 1, col] = np.round(
                            (prev.loc[idx_2_safe] + delta * (2 / 3)).values
                        ).astype(int)
                        fixed_col += 2 * len(idx_2_safe)

        total_fixed += fixed_col

    if verbose:
        print(f"Исправлено аномалий: {total_fixed:,} значений")

    return out.drop(columns=["__market_zero", "__pos"])
