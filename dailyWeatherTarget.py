import pandas as pd
import os

def round_up(dt):
    """
    Tam saat olmayan verileri tam saate atama.
    """
    if dt.minute >= 30:
        dt = dt + pd.Timedelta(hours=1)
    return dt.replace(minute=0, second=0, microsecond=0)

def preprocessing(df):
    """
    round_up'ile verinin kullanım öncesi işlenmesi
    """
    df['dt'] = df['dt'].apply(round_up)
    df = df.groupby(['dt', 'metcities_id'], as_index=False)['temperature'].mean()
    return df

def complete_by_hour(df, start_date, end_date):
    """
     Her metcities için saat girdisi olmasını sağlamak
    (multiIndex kullanım sebebi: her bir metcities için mümkün date_range oluşturmak)
    """
    # Saatlik date range oluşumu
    date_range = pd.date_range(start=start_date, end=end_date, freq='h')
    # date_range and metcities_id için multiIndex oluşumu
    all_dates = pd.MultiIndex.from_product([date_range, df['metcities_id'].unique()], names=['dt', 'metcities_id'])
    # Reindex'leme (dates and metcities_id) kombinasyonları için. Tekrar eden verilerin atılması.
    df = df.drop_duplicates(subset=['dt', 'metcities_id'])
    # indexleme, tüm kombinasyonların mevcut olmasını sağlıyor.
    df = df.set_index(['dt', 'metcities_id']).reindex(all_dates).reset_index()
    return df

def interpolate_and_fill(df):
    """
    Interpolation ile eksik veya boş olan kısımları doldurmak.
    """
    df = df.groupby('metcities_id', group_keys=False).apply(lambda group: group.interpolate(method='linear'))
    # Forward-fill and backward-fill (geriye kalan nan değerleri için)
    df = df.ffill().bfill()
    # ortalama değer ile boş (na) doldurma
    df['temperature'] = df.groupby('metcities_id')['temperature'].transform(lambda x: x.fillna(x.mean()))
    return df

def form_path(data_dir, csv):
    """
    CSV file'ın path'ını oluşturmak.
    """
    return os.path.join(data_dir, csv)

def parsing_date(path, date):
    """
    CSV'nin date kısmının alınması.
    """
    return pd.read_csv(path, parse_dates=[date])

data_directory = r'C:\Users\u47455\Desktop\regression_data'

w1path = form_path(data_directory, "weather_20180101_20211121.csv")
w2path = form_path(data_directory, "weather_20201004_20211222.csv")
t1path = form_path(data_directory, "target_20180101_20210930.csv")
t2path = form_path(data_directory, "target_20211001_20211223.csv")

# Read the weather and target data
w1 = parsing_date(w1path, "dt")
w2 = parsing_date(w2path, "dt")
t1 = parsing_date(t1path, "dt")
t2 = parsing_date(t2path, "dt")

# verilerin preprocess edilmesi
w1 = preprocessing(w1)
w2 = preprocessing(w2)

# w1'in eksikliklerinin dolumu
start = w1['dt'].min()
end = w1['dt'].max()
w1 = complete_by_hour(w1, start, end)
w1 = interpolate_and_fill(w1)

# w2'in eksikliklerinin dolumu
start = w2['dt'].min()
end = w2['dt'].max()
w2 = complete_by_hour(w2, start, end)
w2 = interpolate_and_fill(w2)

# weather birleşimi
weather_combined = pd.concat([w1, w2]).drop_duplicates(['dt', 'metcities_id']).sort_values(by=['dt', 'metcities_id']).reset_index(drop=True)

# target birleşimi
target_combined = pd.concat([t1, t2]).drop_duplicates('dt').sort_values(by='dt').reset_index(drop=True)

# weather ve target birleşimi
data_combined = pd.merge(weather_combined, target_combined, on='dt', how='inner')

# Save işlemi
output_path = os.path.join(data_directory, "data_combined.csv")
data_combined.to_csv(output_path, index=False)

print("Data Saved.")