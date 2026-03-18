import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_parquet(r'C:\Проекты\Project_etl_power_bi\data\preproc_parquet\big_data_clean.parquet')

price_col = ['Цена пульса', 'Цена протека', 'Цена катрена', 'Цена фармкомплекта']

df[price_col] = df[price_col].replace(0, np.nan)
df[price_col] = df.groupby('Код КАГ')[price_col].ffill()

df_sku_diff_max = df.sort_values(by=['Код КАГ', 'Дата'])

df_sku_diff_max['diff_inside_puls'] = df_sku_diff_max.groupby('Код КАГ')['Цена пульса'].diff()
df_sku_diff_max['diff_inside_protek'] = df_sku_diff_max.groupby('Код КАГ')['Цена протека'].diff()
df_sku_diff_max['diff_inside_katren'] = df_sku_diff_max.groupby('Код КАГ')['Цена катрена'].diff()
df_sku_diff_max['diff_inside_farm'] = df_sku_diff_max.groupby('Код КАГ')['Цена фармкомплекта'].diff()

target_cols = ['diff_inside_puls', 'diff_inside_protek', 'diff_inside_katren', 'diff_inside_farm']

df_sku_diff_max = df_sku_diff_max[df_sku_diff_max['Дата'] == df_sku_diff_max['Дата'].max()]
df_sku_diff_max['maxi'] = df_sku_diff_max[target_cols].max(axis=1)
df_sku_diff_max['mini'] = df_sku_diff_max[target_cols].min(axis=1)

df_sku_diff_max['max_col_name'] = df_sku_diff_max[target_cols].fillna(0).idxmax(axis=1)
df_sku_diff_max['min_col_name'] = df_sku_diff_max[target_cols].fillna(0).idxmin(axis=1)

dict_price_cols = {
    'diff_inside_puls': 'Цена пульса',
    'diff_inside_protek': 'Цена протека',
    'diff_inside_katren': 'Цена катрена',
    'diff_inside_farm': 'Цена фармкомплекта',
}

last_two_dates = sorted(df['Дата'].unique())[-2:]
date_yesterday, date_today = last_two_dates[0], last_two_dates[1]

df_yesterday = df[df['Дата'] == date_yesterday].set_index('Код КАГ')

# --- TOP 20 макс. рост ---
top_20 = df_sku_diff_max.nlargest(20, 'maxi').copy()

def get_prices_max(row):
    col = dict_price_cols[row['max_col_name']]
    price_today = row[col]
    sku = row['Код КАГ']
    price_yesterday = df_yesterday.loc[sku, col] if sku in df_yesterday.index else None
    return pd.Series({'Цена вчера (лидер)': price_yesterday, 'Цена сегодня (лидер)': price_today})

top_20[['Цена вчера (лидер)', 'Цена сегодня (лидер)']] = top_20.apply(get_prices_max, axis=1)
top_20['max_col_name'] = top_20['max_col_name'].map(dict_price_cols)

# --- TOP 20 макс. падение ---
top_20_min = df_sku_diff_max.nsmallest(20, 'mini').copy()

def get_prices_min(row):
    col = dict_price_cols[row['min_col_name']]
    price_today = row[col]
    sku = row['Код КАГ']
    price_yesterday = df_yesterday.loc[sku, col] if sku in df_yesterday.index else None
    return pd.Series({'Цена вчера (лидер)': price_yesterday, 'Цена сегодня (лидер)': price_today})

top_20_min[['Цена вчера (лидер)', 'Цена сегодня (лидер)']] = top_20_min.apply(get_prices_min, axis=1)
top_20_min['min_col_name'] = top_20_min['min_col_name'].map(dict_price_cols)

# --- Сохранение ---
top_20.to_excel(r'C:\Проекты\Project_etl_power_bi\data\result\top_20.xlsx')
top_20_min.to_excel(r'C:\Проекты\Project_etl_power_bi\data\result\top_20_min.xlsx')