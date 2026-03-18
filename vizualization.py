"""
Визуализация данных
"""
import re
import matplotlib.pyplot as plt
from pathlib import Path
from tqdm.auto import tqdm
from config import Config


class Visualizer:
    """Класс для построения графиков"""
    
    def __init__(self, config: Config = Config):
        self.config = config
        config.setup_directories()
    
    def plot_stocks_for_kags(
        self,
        df,
        kag_list: list,
        n_last: int = None
    ):
        """
        Строит графики остатков для списка КАГ
        
        Args:
            df: DataFrame с данными
            kag_list: Список КАГ для визуализации
            n_last: Сколько последних наблюдений показать
        """
        if n_last is None:
            n_last = self.config.N_WIDE_LAST
        
        # Подготавливаем маппинг имён
        if self.config.COL_KAG_NAME in df.columns:
            name_map = df.groupby(self.config.COL_KAG)[
                self.config.COL_KAG_NAME
            ].first().to_dict()
        else:
            name_map = {}
        
        # Фильтруем данные
        plot_data = df[df[self.config.COL_KAG].isin(kag_list)].copy()
        
        print(f"\n{'='*60}")
        print("ПОСТРОЕНИЕ ГРАФИКОВ")
        print(f"{'='*60}")
        print(f"📊 КАГ для визуализации: {len(kag_list):,}")
        print(f"📈 Последних точек на график: {n_last}")
        print(f"📁 Папка: {self.config.PLOTS_DIR}")
        
        saved = 0
        errors = 0
        
        for kag, one in tqdm(
            plot_data.groupby(self.config.COL_KAG, sort=False),
            desc="▶ Сохранение графиков"
        ):
            try:
                # Берём последние n наблюдений
                one = one.sort_values('date_n').tail(n_last)
                
                if one.empty:
                    continue
                
                # Строим график
                self._plot_single_kag(one, kag, name_map.get(kag, ''))
                saved += 1
                
            except Exception as e:
                errors += 1
                plt.close()
                print(f"⚠️ Ошибка на КАГ {kag}: {e}")
        
        print(f"\n✅ Графиков сохранено: {saved}")
        if errors > 0:
            print(f"⚠️ Ошибок: {errors}")
        print(f"{'='*60}\n")
    
    def _plot_single_kag(self, data, kag: str, kag_name: str):
        """Строит и сохраняет график для одного КАГ"""
        # Заголовок
        title = f"КАГ {kag}"
        if kag_name:
            title += f" | {kag_name}"
        
        # Создаём фигуру
        plt.figure(figsize=(12, 5))
        
        # Линия ГК
        plt.plot(
            data['date_n'], 
            data[self.config.COL_GK], 
            label='ГК',
            linewidth=2,
            marker='o',
            markersize=4
        )
        
        # Линии конкурентов
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
        for i, comp_col in enumerate(self.config.COMP_COLS):
            pretty_name = self.config.COMP_PRETTY.get(comp_col, comp_col)
            plt.plot(
                data['date_n'],
                data[comp_col],
                label=pretty_name,
                linewidth=1.5,
                marker='s',
                markersize=3,
                alpha=0.8,
                color=colors[i % len(colors)]
            )
        
        plt.title(title, fontsize=12, fontweight='bold')
        plt.xlabel('Дата', fontsize=10)
        plt.ylabel('Остатки', fontsize=10)
        plt.legend(loc='best', ncol=3, fontsize=9)
        plt.grid(True, alpha=0.3, linestyle='--')
        plt.tight_layout()
        
        # Сохраняем
        filename = self._safe_filename(kag, kag_name)
        filepath = self.config.PLOTS_DIR / f"{filename}.png"
        plt.savefig(filepath, dpi=160, bbox_inches='tight')
        plt.close()
    
    @staticmethod
    def _safe_filename(kag: str, kag_name: str, maxlen: int = 140) -> str:
        """Создаёт безопасное имя файла"""
        if kag_name:
            name = f"{kag}__{kag_name}"
        else:
            name = str(kag)
        
        # Убираем недопустимые символы
        name = re.sub(r'[<>:"/\\|?*\n\r\t]+', '_', name)
        name = re.sub(r'\s+', ' ', name).strip()
        
        # Обрезаем длину
        if len(name) > maxlen:
            name = name[:maxlen].rstrip()
        
        return name
