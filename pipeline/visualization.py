"""
Визуализация данных
"""
import re
import matplotlib.pyplot as plt
from tqdm.auto import tqdm
from config import Config


class Visualizer:
    """Построение графиков остатков"""

    def __init__(self, config: Config = Config):
        self.config = config
        config.setup_directories()

    def plot_stocks_for_kags(self, df, kag_list: list, n_last: int = None):
        """Строит графики остатков для списка КАГ"""
        if n_last is None:
            n_last = self.config.N_WIDE_LAST

        if self.config.COL_KAG_NAME in df.columns:
            name_map = df.groupby(self.config.COL_KAG)[self.config.COL_KAG_NAME].first().to_dict()
        else:
            name_map = {}

        plot_data = df[df[self.config.COL_KAG].isin(kag_list)].copy()
        saved = 0

        for kag, one in tqdm(plot_data.groupby(self.config.COL_KAG, sort=False), desc="Сохранение графиков"):
            try:
                one = one.sort_values('date_n').tail(n_last)
                if one.empty:
                    continue
                self._plot_single_kag(one, kag, name_map.get(kag, ''))
                saved += 1
            except Exception:
                plt.close()

        print(f"Графиков сохранено: {saved}")

    def _plot_single_kag(self, data, kag: str, kag_name: str):
        title = f"КАГ {kag}"
        if kag_name:
            title += f" | {kag_name}"

        plt.figure(figsize=(12, 5))
        plt.plot(data['date_n'], data[self.config.COL_GK], label='ГК', linewidth=2, marker='o', markersize=4)

        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
        for i, comp_col in enumerate(self.config.COMP_COLS):
            pretty_name = self.config.COMP_PRETTY.get(comp_col, comp_col)
            plt.plot(
                data['date_n'], data[comp_col], label=pretty_name,
                linewidth=1.5, marker='s', markersize=3, alpha=0.8,
                color=colors[i % len(colors)],
            )

        plt.title(title, fontsize=12, fontweight='bold')
        plt.xlabel('Дата', fontsize=10)
        plt.ylabel('Остатки', fontsize=10)
        plt.legend(loc='best', ncol=3, fontsize=9)
        plt.grid(True, alpha=0.3, linestyle='--')
        plt.tight_layout()

        filename = self._safe_filename(kag, kag_name)
        filepath = self.config.PLOTS_DIR / f"{filename}.png"
        plt.savefig(filepath, dpi=160, bbox_inches='tight')
        plt.close()

    @staticmethod
    def _safe_filename(kag: str, kag_name: str, maxlen: int = 140) -> str:
        name = f"{kag}__{kag_name}" if kag_name else str(kag)
        name = re.sub(r'[<>:"/\\|?*\n\r\t]+', '_', name)
        name = re.sub(r'\s+', ' ', name).strip()
        return name[:maxlen].rstrip() if len(name) > maxlen else name
