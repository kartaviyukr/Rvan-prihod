import time
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

# Эта функция проводит базовые действия: отсекает не те даты, чикает дубликаты, приводит дату к нормлаьному формату, убирает выведенные позиции
def base_action(
    x: pd.DataFrame,
    name_kag: str = 'Активный КАГ',
    name_date: str = 'Дата',
    date_threshold: str = '2025-01-01'
) -> pd.DataFrame:
    """Фильтрует данные по активным КАГ и датам."""
    
    print(f"Исходный размер: {x.shape}")
    
    x = x[x[name_kag] == 'Да'].copy()
    
    x[name_date] = pd.to_datetime(x[name_date])
    
    x = x[x[name_date] > date_threshold]
    
    x = x.drop_duplicates()
    
    print(f"Конечный размер: {x.shape}")
    
    if x.empty:
        print("⚠️ ВНИМАНИЕ: После фильтрации не осталось данных!")
    
    return x


def _log(msg: str, show: bool):
    if show:
        print(msg)



# 1) Приведение типов ключей (Дата, Код КАГ)
# У нас будут ключи аггрегации - это Дата плюс код каг. Вся функция будет работать именно на их сочетании
# Приводим всё к единому стилю через ту нуметрик и эстайп инт и стр
def _normalize_keys(
    x: pd.DataFrame,
    *,
    date_col: str,
    kag_col: str,
    show_progress: bool,
) -> pd.DataFrame:
    """Приводит дату к datetime64, КАГ к str. Удаляет невалидные значения."""
    
    _log("▶ [1/6] Приведение типов (Дата, Код КАГ)", show_progress)
    
    x = x.copy()
    n_initial = len(x)
    
    # Подсчитываем невалидные ПЕРЕД удалением
    x[date_col] = pd.to_datetime(x[date_col], errors="coerce")
    n_bad_date = x[date_col].isna().sum()
    
    x[kag_col] = pd.to_numeric(x[kag_col], errors="coerce")
    n_bad_kag = x[kag_col].isna().sum()
    
    # Удаляем одной операцией (эффективнее)
    x = x.dropna(subset=[date_col, kag_col])
    
    # КАГ в строку (без промежуточного int64 — нет смысла)
    x[kag_col] = x[kag_col].astype(np.int64).astype(str)
    
    _log(
        f"    Вход: {n_initial:,} | Дата✗: {n_bad_date:,} | КАГ✗: {n_bad_kag:,} | "
        f"Выход: {len(x):,}",
        show_progress,
    )
    
    return x




# 2) Numeric-приведение остатков/цен

def _normalize_numeric_cols(
    x: pd.DataFrame,
    *,
    gk_col: str,
    comp_cols: list[str],
    price_cols: list[str],
    show_progress: bool,
) -> pd.DataFrame:
    """
    Numeric-приведение:
    - ГК + конкуренты: to_numeric, NaN→0 (остатки не могут быть пустыми)
    - Цены: to_numeric, NaN сохраняются (отсутствие цены допустимо)
    """
    _log("▶ [2/6] Numeric приведение (остатки/цены)", show_progress)
    
    x = x.copy()
    
    def convert_columns(cols: list[str], fill_na: bool = False) -> dict[str, int]:
        """Конвертирует колонки в numeric, возвращает кол-во ошибок."""
        errors = {}
        for col in cols:
            if col not in x.columns:
                raise KeyError(f"Нет колонки: {col}")
            
            x[col] = pd.to_numeric(x[col], errors="coerce")
            errors[col] = x[col].isna().sum()
            
            if fill_na:
                x[col] = x[col].fillna(0)
        
        return errors
    
    # Остатки: NaN → 0
    stock_errors = convert_columns([gk_col] + comp_cols, fill_na=True)
    
    # Цены: NaN сохраняются
    price_errors = convert_columns(price_cols, fill_na=False) if price_cols else {}
    
    # Логирование
    def format_errors(errors: dict, label: str) -> str:
        bad = {k: v for k, v in errors.items() if v > 0}
        if not bad:
            return f"    • {label}: все значения числовые"
        details = "\n".join(f"        - {k}: {v:,}" for k, v in bad.items())
        return f"    • {label}: нечисловые значения:\n{details}"
    
    _log(format_errors(stock_errors, "Остатки/конкуренты"), show_progress)
    if price_cols:
        _log(format_errors(price_errors, "Цены"), show_progress)
    
    return x




# 3) ГК: sum по (КАГ, Дата)

def _agg_gk_sum(
    x: pd.DataFrame,
    *,
    gkeys: list[str],
    gk_col: str,
    show_progress: bool,
) -> pd.Series:
    """Суммирует ГК по группам (КАГ, Дата)."""
    _log("▶ [3/6] ГК: sum по (КАГ, Дата)", show_progress)
    
    gk_sum = x.groupby(gkeys, sort=False)[gk_col].sum()
    
    _log(
        f"    Строк: {len(x):,} → Групп: {len(gk_sum):,} | "
        f"Сумма: {gk_sum.sum():,.0f}",
        show_progress,
    )
    
    return gk_sum



def _agg_competitors_dedup_or_sum(
    x: pd.DataFrame,
    *,
    gkeys: list[str],
    comp_cols: list[str],
    show_progress: bool,
) -> pd.DataFrame:
    """
    Для конкурентов по (КАГ, Дата):
    - Если все ненулевые значения одинаковы → берём это значение
    - Иначе → суммируем
    
    БЫСТРАЯ ВЕКТОРИЗОВАННАЯ ВЕРСИЯ (без apply!)
    """
    _log("▶ [4/6] Конкуренты: dedup vs sum", show_progress)
    
    # ШАГ 1: Суммируем
    comp_sum = x.groupby(gkeys, sort=False)[comp_cols].sum()
    
    # ШАГ 2: Считаем уникальные ненулевые ВЕКТОРИЗОВАННО
    # Фильтруем ненулевые значения
    x_nonzero = x.copy()
    for col in comp_cols:
        x_nonzero.loc[x_nonzero[col] <= 0, col] = pd.NA
    
    # Считаем уникальные (встроенная функция - быстро!)
    comp_nunique = x_nonzero.groupby(gkeys, sort=False)[comp_cols].nunique()
    
    # ШАГ 3: Max по группам
    comp_max = x.groupby(gkeys, sort=False)[comp_cols].max()
    
    # ШАГ 4: Выбираем max или sum
    # Если уникальных ≤1 → берём max, иначе sum
    comp_final = comp_max.where(comp_nunique <= 1, comp_sum)
    
    n_dedup = (comp_nunique <= 1).sum().sum()
    n_total = comp_nunique.size
    
    _log(
        f"    Групп: {len(comp_final):,} | "
        f"Дедуп: {n_dedup:,}/{n_total:,} ячеек",
        show_progress,
    )
    
    return comp_final




# 5) Цены: median по ненулевым

def _agg_prices_median_nonzero(
    x: pd.DataFrame,
    *,
    gkeys: list[str],
    price_cols: list[str],
    show_progress: bool,
) -> pd.DataFrame:
    """Агрегирует цены как median по ненулевым (<=0 считаются отсутствием)."""
    _log("▶ [5/6] Цены: median по ненулевым", show_progress)
    
    prices_tmp = x[gkeys + price_cols].copy()
    
    # Нули и отрицательные → NaN
    for c in price_cols:
        prices_tmp[c] = prices_tmp[c].where(prices_tmp[c] > 0, np.nan)
    
    # Median по группам, NaN → 0
    prices_agg = (
        prices_tmp
        .groupby(gkeys, sort=False)[price_cols]
        .median()
        .fillna(0)
    )
    
    _log(f"    Групп: {len(prices_agg):,}", show_progress)
    
    return prices_agg




# 6) Сбор финальной таблицы

def _build_daily_table(
    *,
    gk_sum: pd.Series,
    comp_final: pd.DataFrame,
    prices_agg: pd.DataFrame | None,
    date_col: str,
    kag_col: str,
    show_progress: bool,
) -> pd.DataFrame:
    """Объединяет агрегаты в одну таблицу с проверкой согласованности."""
    _log("▶ [6/6] Сбор финальной таблицы", show_progress)
    
    # Проверяем согласованность индексов
    base_index = gk_sum.index
    
    if not comp_final.index.equals(base_index):
        raise ValueError("Несогласованные индексы: comp_final")
    
    if prices_agg is not None and not prices_agg.index.equals(base_index):
        raise ValueError("Несогласованные индексы: prices_agg")
    
    # Объединяем
    parts = [gk_sum, comp_final]
    if prices_agg is not None:
        parts.append(prices_agg)
    
    out = pd.concat(parts, axis=1).reset_index()
    
    # Сортируем
    out = out.sort_values([kag_col, date_col]).reset_index(drop=True)
    
    _log(f"    Итог: {out.shape}", show_progress)
    
    return out




# 7) Тексты: схлопывание на уровне КАГ

def _merge_kag_text_info(
    out: pd.DataFrame,
    x: pd.DataFrame,
    *,
    kag_col: str,
    name_kag_col: str,
    product_code_col: str,
    product_name_col: str,
    show_progress: bool,
) -> pd.DataFrame:
    """
    Схлопывает текстовые поля на уровне КАГ и мержит в out:
      - Имя КАГ: первое непустое
      - Код товара и Наименование: join уникальных через '; '
    tqdm по КАГ (если show_progress=True).
    """
    text_cols = []
    if name_kag_col in x.columns:
        text_cols.append(name_kag_col)
    if product_code_col in x.columns:
        text_cols.append(product_code_col)
    if product_name_col in x.columns:
        text_cols.append(product_name_col)

    if not text_cols:
        _log("▶ Тексты: текстовых колонок нет — пропускаю", show_progress)
        return out

    n_out_rows_before = len(out)
    out_kags = out[kag_col].nunique(dropna=True)
    x_kags = x[kag_col].nunique(dropna=True)

    _log(
        "▶ Тексты: схлопывание по КАГ (tqdm)\n"
        f"    • найденные колонки: {', '.join(text_cols)}\n"
        f"    • уникальных КАГ: out={out_kags:,} | source={x_kags:,}",
        show_progress,
    )

    xx = x[[kag_col] + text_cols].copy()
    for c in text_cols:
        xx[c] = xx[c].astype("string").str.strip()
        xx.loc[xx[c].isin(["", "nan", "None"]), c] = pd.NA

    def first_non_empty(s: pd.Series) -> str:
        for v in s.dropna().astype(str):
            v = v.strip()
            if v:
                return v
        return ""

    def join_unique_fast(s: pd.Series) -> str:
        s = s.dropna().astype(str).str.strip()
        s = s[s != ""]
        return "; ".join(pd.unique(s)) if len(s) else ""

    kag_groups = xx.groupby(kag_col, sort=False)
    iterator = kag_groups
    if show_progress:
        iterator = tqdm(kag_groups, total=kag_groups.ngroups, desc="   • КАГ", mininterval=0.5)

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

    kag_info = pd.DataFrame(rows)

    _log(
        f"    • строк в kag_info: {len(kag_info):,}\n"
        f"    • уникальных КАГ в kag_info: {kag_info[kag_col].nunique(dropna=True):,}",
        show_progress,
    )

    out2 = out.merge(kag_info, on=kag_col, how="left")

    n_out_rows_after = len(out2)
    _log(
        f"    • контроль строк после merge: "
        f"{n_out_rows_before:,} → {n_out_rows_after:,} "
        f"({'OK' if n_out_rows_before == n_out_rows_after else 'FAIL'})",
        show_progress,
    )

    # coverage: сколько строк out получили непустые значения
    def _filled_share(col: str) -> tuple[int, float]:
        if col not in out2.columns:
            return 0, 0.0
        filled = out2[col].notna() & (out2[col].astype("string").str.strip() != "")
        cnt = int(filled.sum())
        share = cnt / len(out2) if len(out2) else 0.0
        return cnt, share

    for col in [name_kag_col, product_code_col, product_name_col]:
        if col in out2.columns:
            cnt, share = _filled_share(col)
            _log(f"    • заполнено {col}: {cnt:,} ({share:.1%})", show_progress)

    _log("    • merge текстов завершён", show_progress)
    return out2




# 🚀 Главная функция (тонкая): orchestrates steps

def collapse_kag_daily_smart(
    df: pd.DataFrame,
    date_col: str = "Дата",
    kag_col: str = "Код КАГ",
    name_kag_col: str = "Имя КАГ",
    product_code_col: str = "Код товара",
    product_name_col: str = "Наименование у нас",
    gk_col: str = "ГК (остатки гранд капитала)",
    comp_cols: list[str] | None = None,
    price_cols: list[str] | None = None,
    show_progress: bool = True,
) -> pd.DataFrame:
    """
    Агрегирует данные до 1 строки на (Дата, Код КАГ):
    - ГК: sum
    - Конкуренты: dedup если одинаковые, иначе sum
    - Цены: median по ненулевым
    - Текстовые поля: схлопывание на уровне КАГ
    
    Args:
        df: Исходный DataFrame
        comp_cols: ОБЯЗАТЕЛЬНО! Список колонок с остатками конкурентов
        price_cols: Список колонок с ценами (опционально)
        
    Returns:
        Агрегированный DataFrame
    """
    t0 = time.time()
    
    # Валидация входных данных
    if df.empty:
        raise ValueError("Входной DataFrame пустой")
    
    if comp_cols is None:
        raise ValueError(
            "Параметр comp_cols обязателен. "
            "Укажите список колонок конкурентов, например: ['Конкурент_1', 'Конкурент_2']"
        )
    
    # Проверка существования колонок
    required_cols = [date_col, kag_col, gk_col]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Отсутствуют обязательные колонки: {missing}")
    
    missing_comp = [c for c in comp_cols if c not in df.columns]
    if missing_comp:
        raise ValueError(f"Отсутствуют колонки конкурентов: {missing_comp}")
    
    if price_cols is None:
        price_cols = []
    
    # Пайплайн обработки
    gkeys = [kag_col, date_col]
    
    x = _normalize_keys(df, date_col=date_col, kag_col=kag_col, show_progress=show_progress)
    
    if len(x) == 0:
        _log("⚠️ После нормализации не осталось данных!", show_progress)
        return pd.DataFrame()
    
    x = _normalize_numeric_cols(
        x, gk_col=gk_col, comp_cols=comp_cols, 
        price_cols=price_cols, show_progress=show_progress
    )
    
    gk_sum = _agg_gk_sum(x, gkeys=gkeys, gk_col=gk_col, show_progress=show_progress)
    comp_final = _agg_competitors_dedup_or_sum(x, gkeys=gkeys, comp_cols=comp_cols, show_progress=show_progress)
    
    prices_agg = None
    if price_cols:
        prices_agg = _agg_prices_median_nonzero(x, gkeys=gkeys, price_cols=price_cols, show_progress=show_progress)
    
    out = _build_daily_table(
        gk_sum=gk_sum, comp_final=comp_final, prices_agg=prices_agg,
        date_col=date_col, kag_col=kag_col, show_progress=show_progress
    )
    
    # Добавляем день недели
    out["weekday"] = out[date_col].dt.dayofweek
    
    # Добавляем текстовые поля
    out = _merge_kag_text_info(
        out, x, kag_col=kag_col, name_kag_col=name_kag_col,
        product_code_col=product_code_col, product_name_col=product_name_col,
        show_progress=show_progress
    )
    
    _log(f"✓ Готово: {out.shape} за {time.time() - t0:.1f}с", show_progress)
    return out



def zero_small_stocks_conditional(
    df: pd.DataFrame,
    *,
    stock_cols: list[str] | None = None,
    kag_col: str = "Код КАГ",
    small_threshold: float = 10.0,
    apply_if_max_gt: float = 100.0,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Обнуляет остатки ≤ small_threshold ТОЛЬКО для тех (КАГ, колонка),
    где максимальный остаток за всё время > apply_if_max_gt.
    
    Пример:
        Если у КАГ=123 max(ГК) = 500 > 100, то все ГК ≤ 10 обнуляются.
        Если у КАГ=456 max(ГК) = 50 < 100, остатки не трогаем.
    """
    
    # Дефолтные колонки
    if stock_cols is None:
        stock_cols = [
            "ГК (остатки гранд капитала)",
            "Пульс (остатки пульса)",
            "Катрен (остатки катрена)",
            "Протек (остатки протека)",
            "Фармкомплект (остатки фармкомплекта)",
        ]
    
    # Проверяем наличие колонок
    missing = [c for c in stock_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Отсутствуют колонки: {missing}")
    
    out = df.copy()
    
    if verbose:
        print(
            f"▶ Обнуление мелких остатков (≤ {small_threshold}) "
            f"где max > {apply_if_max_gt}"
        )
    
    # Считаем max по (КАГ × колонка)
    max_per_kag = out.groupby(kag_col)[stock_cols].max()
    
    # Где применяем правило
    apply_mask = max_per_kag > apply_if_max_gt
    
    if verbose:
        print(
            f"  Пар (КАГ×колонка): {apply_mask.size:,} | "
            f"Применяем: {apply_mask.sum().sum():,}"
        )
    
    # Обнуляем (эффективно через isin)
    zeroed_total = 0
    
    for col in stock_cols:
        # КАГи, где можно применять правило для этой колонки
        allowed_kags = apply_mask.index[apply_mask[col]].tolist()
        
        # Маска: КАГ разрешён И значение маленькое
        mask = out[kag_col].isin(allowed_kags) & (out[col] <= small_threshold)
        
        zeroed_total += mask.sum()
        out.loc[mask, col] = 0.0
    
    if verbose:
        print(f"  Обнулено значений: {zeroed_total:,}\n")
    
    return out


def drop_weekends_and_holidays(
    df: pd.DataFrame,
    *,
    date_col: str = "Дата",
    holidays: list[pd.Timestamp] | list[str] | None = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Удаляет строки, попадающие на выходные (сб/вс) и праздники.
    
    Args:
        df: Исходный DataFrame
        date_col: Название колонки с датами
        holidays: Список дат праздников (опционально). Если None, используется 
                 полный список российских праздников и предпраздничных дней на 2025-2026
        verbose: Логировать ли процесс
    
    Returns:
        DataFrame без выходных и праздников
    """
    
    # Подготовка праздников
    if holidays is None:
        # Полный список российских праздников и предпраздничных дней 2025-2026
        holidays = [
            # === 2025 год ===
            # Новогодние каникулы (30 декабря 2024 - 9 января 2025)
            '2024-12-30', '2024-12-31',
            '2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04',
            '2025-01-05', '2025-01-06', '2025-01-07', '2025-01-08', '2025-01-09',
            
            # День защитника Отечества
            '2025-02-22',  # предпраздничный 
            '2025-02-23',  # праздник
            '2025-02-24',  # перенос с субботы
            
            # Международный женский день
            '2025-03-07',  # предпраздничный
            '2025-03-08',  # праздник
            '2025-03-10',  # перенос с воскресенья
            
            # Праздник Весны и Труда
            '2025-04-30',  # предпраздничный
            '2025-05-01',  # праздник
            '2025-05-02',  # перенос
            
            # День Победы
            '2025-05-08',  # предпраздничный
            '2025-05-09',  # праздник
            
            # День России
            '2025-06-11',  # предпраздничный
            '2025-06-12',  # праздник
            '2025-06-13',  # перенос
            
            # День народного единства
            '2025-11-03',  # предпраздничный
            '2025-11-04',  # праздник
            
            # === 2026 год ===
            # Новогодние каникулы (30 декабря 2025 - 9 января 2026)
            '2025-12-30', '2025-12-31',
            '2026-01-01', '2026-01-02', '2026-01-03', '2026-01-04',
            '2026-01-05', '2026-01-06', '2026-01-07', '2026-01-08', '2026-01-09',
            
            # День защитника Отечества
            '2026-02-21',  # предпраздничный (пятница)
            '2026-02-23',  # праздник (понедельник)
            
            # Международный женский день
            '2026-03-07',  # предпраздничный
            '2026-03-09',  # перенос с воскресенья
            
            # Праздник Весны и Труда
            '2026-04-30',  # предпраздничный
            '2026-05-01',  # праздник
            '2026-05-04',  # перенос с субботы
            
            # День Победы
            '2026-05-08',  # предпраздничный
            '2026-05-11',  # перенос с воскресенья
            
            # День России
            '2026-06-11',  # предпраздничный
            '2026-06-12',  # праздник
            
            # День народного единства
            '2026-11-03',  # предпраздничный
            '2026-11-04',  # праздник
        ]
    
    try:
        holidays = pd.to_datetime(holidays)
    except Exception as e:
        raise ValueError(f"Невалидные праздники: {e}")
    
    # Вычисляем маски (один раз!)
    is_weekend = df[date_col].dt.weekday.isin([5, 6])
    is_holiday = df[date_col].isin(holidays)
    
    # Логирование
    if verbose:
        total_rows = len(df)
        weekend_cnt = is_weekend.sum()
        holiday_cnt = is_holiday.sum()
        print(
            f"▶ Удаление выходных и праздников | "
            f"Всего: {total_rows:,} | "
            f"Выходных: {weekend_cnt:,} | "
            f"Праздников: {holiday_cnt:,}"
        )
    
    # Фильтруем
    out = df.loc[~(is_weekend | is_holiday)].copy()
    
    if verbose:
        removed = len(df) - len(out)
        print(f"  Удалено: {removed:,} | Осталось: {len(out):,}\n")
    
    return out




def drop_inactive_by_last_months_stock(
    df: pd.DataFrame,
    *,
    kag_col: str = "Код КАГ",
    date_col: str = "Дата",
    gk_col: str = "ГК (остатки гранд капитала)",
    competitor_cols: list[str] | None = None,
    months: int = 3,
    stock_positive_eps: float = 0.0,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Удаляет КАГи, неактивные за последние N месяцев.
    
    КАГ считается неактивным, если:
    - У ГК НИ РАЗУ не было остатка > eps
    - И у конкурентов НИ РАЗУ не было остатка > eps
    
    Args:
        months: Сколько месяцев назад смотреть от max_date
        stock_positive_eps: Порог "положительного" остатка
    """
    
    # Валидация
    if stock_positive_eps < 0:
        raise ValueError(f"stock_positive_eps должен быть >= 0, получен: {stock_positive_eps}")
    
    if competitor_cols is None:
        competitor_cols = [
            "Пульс (остатки пульса)",
            "Катрен (остатки катрена)",
            "Протек (остатки протека)",
            "Фармкомплект (остатки фармкомплекта)",
        ]
    
    # Проверка наличия колонок
    cols_need = [date_col, kag_col, gk_col] + competitor_cols
    missing = [c for c in cols_need if c not in df.columns]
    if missing:
        raise KeyError(f"Отсутствуют колонки: {missing}")
    
    # Подготовка данных
    df_clean = df.copy()
    df_clean[date_col] = pd.to_datetime(df_clean[date_col], errors="coerce")
    df_clean = df_clean.dropna(subset=[date_col, kag_col])
    
    max_date = df_clean[date_col].max()
    if pd.isna(max_date):
        if verbose:
            print("▶ Фильтр активности: нет валидных дат, возвращаю пустой DataFrame")
        return pd.DataFrame()
    
    # Берём последние N месяцев
    cutoff = max_date - pd.DateOffset(months=months)
    last = df_clean[df_clean[date_col] >= cutoff]
    
    # Приводим остатки к numeric (если ещё не сделано)
    stock_cols = [gk_col] + competitor_cols
    for col in stock_cols:
        last[col] = pd.to_numeric(last[col], errors="coerce").fillna(0.0)
    
    # Определяем активные КАГи
    g = last.groupby(kag_col, sort=False)
    
    gk_active = g[gk_col].max() > stock_positive_eps
    comp_active = g[competitor_cols].max().max(axis=1) > stock_positive_eps
    
    # КАГ активен, если активен ГК ИЛИ хотя бы один конкурент
    active_kags = gk_active.index[gk_active | comp_active].tolist()
    
    # Логирование
    if verbose:
        total_kags = df_clean[kag_col].nunique()
        kept_cnt = len(active_kags)
        drop_cnt = total_kags - kept_cnt
        
        print(f"▶ Фильтр активности за {months} мес.")
        print(f"  Период: {cutoff.date()} – {max_date.date()}")
        print(f"  КАГ: {total_kags:,} | Удалено: {drop_cnt:,} | Осталось: {kept_cnt:,}\n")
    
    return df_clean[df_clean[kag_col].isin(active_kags)]


def fix_competitor_drop_to_zero_anomalies(
    df: pd.DataFrame,
    *,
    kag_col: str = "Код КАГ",
    date_col: str = "Дата",
    competitor_cols: list[str] | None = None,
    max_gap_days: int = 2,
    return_tol_pct: float = 0.05,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Восстанавливает аномальные провалы конкурентов в 0.
    
    Логика:
    - Если остаток упал в 0 на N дней, затем вернулся к прежнему уровню (±5%),
      то интерполируем пропущенные значения.
    """
    
    if competitor_cols is None:
        competitor_cols = [
            "Пульс (остатки пульса)",
            "Катрен (остатки катрена)",
            "Протек (остатки протека)",
            "Фармкомплект (остатки фармкомплекта)",
        ]
    
    # Подготовка
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    out = out.dropna(subset=[kag_col, date_col])  # копия не нужна
    out = out.sort_values([kag_col, date_col]).reset_index(drop=True)
    
    # Numeric конкурентов
    for col in competitor_cols:
        if col not in out.columns:
            raise KeyError(f"Отсутствует колонка: {col}")
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    
    # Флаг "рынок упал в 0"
    out["__market_zero"] = (out[competitor_cols].sum(axis=1) == 0)
    
    # Вспомогательная колонка: позиция строки в группе КАГ
    out['__pos'] = out.groupby(kag_col).cumcount()
    
    if verbose:
        print(
            f"▶ Исправление аномалий конкурентов "
            f"(gap≤{max_gap_days}дн, возврат≤{return_tol_pct*100:.0f}%)"
        )
    
    total_fixed = 0
    
    # Один groupby для всех колонок
    g = out.groupby(kag_col, sort=False)
    
    for col in competitor_cols:
        prev = g[col].shift(1)
        
        # Эффективный ноль (включая рыночный)
        mz = out["__market_zero"]
        cur_eff_zero = (out[col] == 0) | mz
        
        fixed_col = 0
        
        # --- 1 день ---
        nxt1 = g[col].shift(-1)
        
        denom = prev.abs().clip(lower=1.0)
        rel_diff = ((nxt1 - prev).abs() / denom)
        
        is_gap_1 = (
            (prev > 0) & 
            cur_eff_zero & 
            (nxt1 > 0) & 
            (rel_diff <= return_tol_pct)
        )
        
        idx_1 = is_gap_1[is_gap_1].index
        
        if len(idx_1):
            # Интерполируем
            v = (prev.loc[idx_1] + nxt1.loc[idx_1]) / 2
            out.loc[idx_1, col] = np.round(v.values).astype(int)
            fixed_col += len(idx_1)
        
        # --- 2 дня ---
        if max_gap_days >= 2:
            nxt2 = g[col].shift(-2)
            mz1 = g["__market_zero"].shift(-1).fillna(False)
            
            nxt1_eff_zero = (nxt1 == 0) | mz1
            
            denom2 = prev.abs().clip(lower=1.0)
            rel_diff2 = ((nxt2 - prev).abs() / denom2)
            
            is_gap_2 = (
                (prev > 0) & 
                cur_eff_zero & 
                nxt1_eff_zero & 
                (nxt2 > 0) & 
                (rel_diff2 <= return_tol_pct)
            )
            
            idx_2 = is_gap_2[is_gap_2].index
            
            if len(idx_2):
                # Проверяем, что idx+1 существует и принадлежит тому же КАГ
                pos_cur = out.loc[idx_2, '__pos'].values
                pos_next = out.loc[idx_2 + 1, '__pos'].values if (idx_2 + 1).max() < len(out) else []
                
                # Безопасная проверка
                valid = (pos_next == pos_cur + 1) if len(pos_next) == len(pos_cur) else []
                idx_2_safe = idx_2[valid] if len(valid) else []
                
                if len(idx_2_safe):
                    # Линейная интерполяция
                    delta = nxt2.loc[idx_2_safe] - prev.loc[idx_2_safe]
                    v1 = prev.loc[idx_2_safe] + delta * (1/3)
                    v2 = prev.loc[idx_2_safe] + delta * (2/3)
                    
                    out.loc[idx_2_safe, col] = np.round(v1.values).astype(int)
                    out.loc[idx_2_safe + 1, col] = np.round(v2.values).astype(int)
                    
                    fixed_col += 2 * len(idx_2_safe)
        
        total_fixed += fixed_col
        
        if verbose:
            print(f"  {col}: исправлено {fixed_col:,} значений")
    
    if verbose:
        print(f"✓ Всего исправлено: {total_fixed:,} значений\n")
    
    out = out.drop(columns=["__market_zero", "__pos"])
    return out