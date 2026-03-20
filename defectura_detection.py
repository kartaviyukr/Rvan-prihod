"""
Детекция эпизодов дефектуры товаров

Порог дефектуры для каждого КАГ:
    - ЭО >= MIN_EO_FOR_PCT  →  порог = DEFECT_EO_PCT × ЭО
    - ЭО < MIN_EO_FOR_PCT   →  порог = 0  (дефектура только при ГК == 0)
    - КАГ нет в справочнике  →  порог = 0
"""
import numpy as np
import pandas as pd
from typing import List, Dict, Optional
from tqdm.auto import tqdm
from config import Config


class DefecturaDetector:
    """Класс для поиска эпизодов дефектуры"""

    def __init__(self, config: Config = Config):
        self.config = config
        self._eo_map: Optional[Dict[str, float]] = None

    # ------------------------------------------------------------------
    # Загрузка ЭО
    # ------------------------------------------------------------------
    def _load_eo_map(self) -> Dict[str, float]:
        """
        Загружает справочник ЭО из файла и возвращает {код_каг: ЭО}.
        Кэширует результат.
        """
        if self._eo_map is not None:
            return self._eo_map

        eo_path = self.config.EO_FILE

        try:
            df_eo = pd.read_excel(eo_path)
        except FileNotFoundError:
            print(f"⚠️  Файл ЭО не найден: {eo_path}")
            print("   Будет использована логика ГК == 0")
            self._eo_map = {}
            return self._eo_map

        # Нормализуем код КАГ
        def norm(x):
            try:
                return str(int(float(str(x).strip())))
            except Exception:
                return str(x).strip()

        # Определяем колонку ЭО (может быть «ЭО общая» или «ЭО»)
        eo_col = None
        for candidate in ['ЭО общая', 'ЭО']:
            if candidate in df_eo.columns:
                eo_col = candidate
                break

        if eo_col is None:
            print(f"⚠️  В файле ЭО не найдена колонка ЭО. Колонки: {list(df_eo.columns)}")
            self._eo_map = {}
            return self._eo_map

        df_eo[self.config.COL_KAG] = df_eo[self.config.COL_KAG].apply(norm)
        df_eo[eo_col] = pd.to_numeric(df_eo[eo_col], errors='coerce').fillna(0)

        # Берём первое уникальное значение на КАГ
        df_eo = df_eo.drop_duplicates(subset=[self.config.COL_KAG], keep='first')

        self._eo_map = dict(zip(
            df_eo[self.config.COL_KAG],
            df_eo[eo_col]
        ))

        print(f"✅ Загружен справочник ЭО: {len(self._eo_map):,} КАГ")
        return self._eo_map

    # ------------------------------------------------------------------
    # Порог дефектуры
    # ------------------------------------------------------------------
    def _get_threshold(self, kag) -> float:
        """
        Возвращает порог дефектуры для конкретного КАГ.

        Формула:
            ЭО >= MIN_EO_FOR_PCT  →  порог = DEFECT_EO_PCT × ЭО
            ЭО <  MIN_EO_FOR_PCT  →  порог = 0  (дефектура только при ГК == 0)
            КАГ нет в справочнике  →  порог = 0
        """
        eo_map = self._load_eo_map()

        kag_str = str(kag).strip()
        try:
            kag_str = str(int(float(kag_str)))
        except Exception:
            pass

        eo = eo_map.get(kag_str, 0.0)

        if eo < self.config.MIN_EO_FOR_PCT:
            return 0.0

        return eo * self.config.DEFECT_EO_PCT

    # ------------------------------------------------------------------
    # Фильтрация eligible КАГ
    # ------------------------------------------------------------------
    def filter_eligible_kags(
        self,
        df: pd.DataFrame,
        work_date: pd.Timestamp
    ) -> List[str]:
        """
        Фильтрует КАГ, подходящие для анализа дефектуры

        Критерии:
            1. У ГК когда-либо был остаток > 0
            2. Минимум N наблюдений за последние 30 дней
        """
        # Проверка 1: ГК был положительным хотя бы раз
        gk_ever_positive = df.groupby(self.config.COL_KAG)[self.config.COL_GK].max() > 0

        # Проверка 2: достаточно наблюдений в последние 30 дней
        last30_start = (work_date - pd.Timedelta(days=self.config.LAST30_DAYS - 1)).normalize()
        is_in_last30 = df['date_n'] >= last30_start
        obs_last30 = df[is_in_last30].groupby(self.config.COL_KAG)['date_n'].size()
        has_enough_history = obs_last30 >= self.config.MIN_OBS_LAST30

        # Объединяем условия
        eligible = [
            k for k in gk_ever_positive.index
            if bool(gk_ever_positive.get(k, False))
            and bool(has_enough_history.get(k, False))
        ]

        # Предзагружаем ЭО для статистики
        eo_map = self._load_eo_map()
        eligible_with_eo = sum(1 for k in eligible if eo_map.get(str(k), 0) > 0)

        pct_label = f"{self.config.DEFECT_EO_PCT:.0%}"

        print(f"\n{'=' * 60}")
        print("ФИЛЬТРАЦИЯ КАГ ДЛЯ АНАЛИЗА")
        print(f"{'=' * 60}")
        print(f"✅ КАГ с положительными остатками ГК: {gk_ever_positive.sum():,}")
        print(f"✅ КАГ с достаточной историей (≥{self.config.MIN_OBS_LAST30} набл.): {has_enough_history.sum():,}")
        print(f"🎯 ИТОГО подходящих КАГ: {len(eligible):,}")
        print(f"   из них с ЭО > 0: {eligible_with_eo:,}")
        print(f"   порог дефектуры: ГК < {pct_label} × ЭО (при ЭО ≥ {self.config.MIN_EO_FOR_PCT})")
        print(f"                    ГК == 0 (при ЭО < {self.config.MIN_EO_FOR_PCT} или ЭО неизвестен)")
        print(f"{'=' * 60}\n")

        return eligible

    # ------------------------------------------------------------------
    # Режим 1: Последняя точка
    # ------------------------------------------------------------------
    def find_last_point_defects(
        self,
        df: pd.DataFrame,
        eligible_kags: List[str]
    ) -> pd.DataFrame:
        """
        Поиск КАГ в дефектуре на ПОСЛЕДНЕЙ дате наблюдений

        Дефектура = ГК < DEFECT_EO_PCT × ЭО  И  конкуренты > 0
        (если ЭО неизвестен — ГК == 0 И конкуренты > 0)
        """
        eo_map = self._load_eo_map()

        # Берём последнюю строку для каждого КАГ
        last_points = (
            df[df[self.config.COL_KAG].isin(eligible_kags)]
            .sort_values([self.config.COL_KAG, 'date_n'])
            .groupby(self.config.COL_KAG, sort=False, as_index=False)
            .tail(1)
            .copy()
        )

        # Вычисляем персональный порог для каждого КАГ
        last_points['_threshold'] = last_points[self.config.COL_KAG].apply(
            self._get_threshold
        )

        # Фильтруем: ГК < порог  И  конкуренты > 0
        mask = (
            (last_points[self.config.COL_GK] < last_points['_threshold'])
            & (last_points['sum_competitors'] > 0)
        )

        result = last_points[mask].copy()

        # Статистика
        n_with_eo = (result['_threshold'] > 0).sum()
        n_zero_threshold = (result['_threshold'] == 0).sum()

        result = result.drop(columns=['_threshold'])

        pct_label = f"{self.config.DEFECT_EO_PCT:.0%}"

        print(f"\n{'=' * 60}")
        print("ПОИСК ДЕФЕКТУРЫ (ПОСЛЕДНЯЯ ТОЧКА)")
        print(f"{'=' * 60}")
        print(f"📊 Проверено КАГ: {len(eligible_kags):,}")
        print(f"🔴 КАГ в дефектуре (ГК < {pct_label} × ЭО, конкуренты > 0): {len(result):,}")
        print(f"   из них с порогом по ЭО: {n_with_eo:,}")
        print(f"   из них с порогом = 0 (нет ЭО): {n_zero_threshold:,}")
        print(f"{'=' * 60}\n")

        return result

    # ------------------------------------------------------------------
    # Режим 2: Эпизоды
    # ------------------------------------------------------------------
    def find_defectura_episodes(
        self,
        kag_store: dict,
        eligible_kags: List[str],
        work_date: pd.Timestamp,
        lookback_days: int = None
    ) -> pd.DataFrame:
        """
        Поиск ВСЕХ эпизодов дефектуры за период

        Эпизод = переход ГК из ≥ порога в < порога
        Порог = DEFECT_EO_PCT × ЭО  (0 если ЭО неизвестен)
        """
        if lookback_days is None:
            lookback_days = self.config.LOOKBACK_DEFECTS_DAYS

        lookback_start = (work_date - pd.Timedelta(days=lookback_days - 1)).normalize()

        pct_label = f"{self.config.DEFECT_EO_PCT:.0%}"

        print(f"\n{'=' * 60}")
        print("ПОИСК ЭПИЗОДОВ ДЕФЕКТУРЫ")
        print(f"{'=' * 60}")
        print(f"📅 Период поиска: {lookback_start.date()} → {work_date.date()}")
        print(f"🔍 Обрабатываем {len(eligible_kags):,} КАГ...")
        print(f"   Порог: ГК < {pct_label} × ЭО (при ЭО ≥ {self.config.MIN_EO_FOR_PCT}, иначе ГК == 0)")

        episodes = []

        for kag in tqdm(eligible_kags, desc="▶ Поиск старта дефектур"):
            st = kag_store.get(kag)
            if st is None:
                continue

            dates = st['dates']
            gk = st['gk']

            if len(gk) < 2:
                continue

            # Персональный порог для этого КАГ
            threshold = self._get_threshold(kag)

            # Булева маска: True = в дефектуре
            in_defect = gk < threshold

            # Находим точки старта: переход из «не дефектура» в «дефектура»
            prev_in_defect = np.r_[False, in_defect[:-1]]
            start_indices = np.where(in_defect & ~prev_in_defect)[0]

            if start_indices.size == 0:
                continue

            # Предвычисляем следующую позицию «не в дефектуре» для каждой точки
            next_ok = np.full(len(gk), -1, dtype=int)
            nxt = -1
            for i in range(len(gk) - 1, -1, -1):
                next_ok[i] = nxt
                if not in_defect[i]:
                    nxt = i

            # Обрабатываем каждый старт
            for start_idx in start_indices:
                start_date = pd.Timestamp(dates[start_idx]).normalize()

                # Фильтр по периоду
                if start_date < lookback_start or start_date > work_date:
                    continue

                # Ищем конец дефектуры
                end_idx = next_ok[start_idx]
                if end_idx == -1:
                    # Активная дефектура (не закончилась)
                    end_date = pd.NaT
                    is_finished = False
                else:
                    end_date = pd.Timestamp(dates[end_idx]).normalize()
                    is_finished = True

                episodes.append({
                    self.config.COL_KAG: kag,
                    'defect_start_date': start_date,
                    'defect_end_date': end_date,
                    'is_finished': is_finished,
                })

        if not episodes:
            print("⚠️  Эпизодов не найдено")
            return pd.DataFrame(columns=[
                self.config.COL_KAG,
                'defect_start_date',
                'defect_end_date',
                'is_finished'
            ])

        result = pd.DataFrame(episodes)

        print(f"\n✅ Найдено эпизодов: {len(result):,}")
        print(f"   из них закончившихся: {result['is_finished'].sum():,}")
        print(f"   из них активных: {(~result['is_finished']).sum():,}")
        print(f"{'=' * 60}\n")

        return result