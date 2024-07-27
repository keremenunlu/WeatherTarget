import pandas as pd
import numpy as np
import os


# BAŞLANGIÇ VE BİTİŞ SÜRELERİ ARASINDAKİ TRAİHLERİN VARLIĞINI KONTROL EDİYOR. EĞER EKSİK VARSA NA İŞARETLİYORUZ
def complete_NA_dates(df, start_date, end_date):
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")  # D = DAILY
    df = df.set_index("dt")

    for date in date_range:
        if date not in df.index:
            # DATE YOKSA NA İŞARETLE
            df.loc[date] = np.nan

    df = df.sort_index().reset_index()
    return df

# CSV DOSYALARININ PATH ADINI OLUŞTURMA
def form_path(data_dir, csv):
    return os.path.join(data_dir, csv)

# PATH'I ÇIKAN CSV'LERİN DATE VERİLERİNİ ELDE ETME
def parsing_date(path, date):
    return pd.read_csv(path, parse_dates=[date])

# KAYIP VERİYE SAHİP OLAN KISIMLARI BULUP, ÇIKARTIP, D2'DE OLMASI DURUMUNDA D2 DEĞERİNİ EKLEME VE DİĞER SATIRLARA
# FORWARD VE BACKWARD FİLL YAPARAK EKLEME
def merge_and_fill_na(df1, df2, date_column):
    for idx in df1[df1.isna().any(axis=1)].index:
        date = df1.loc[idx, date_column]
        if date in df2[date_column].values:
            df1.loc[idx] = df2[df2[date_column] == date].iloc[0]
    return df1.ffill().bfill()


def load_and_process_data(data_dir, weather_files, target_files):
    # DF'LERİN FİLE LİST'İNDEN ÇEKİLMESİ VE PATH, PARSING YAPILARAK EKLENMESİ
    weather_dfs = [parsing_date(form_path(data_dir, file), "dt") for file in weather_files]
    target_dfs = [parsing_date(form_path(data_dir, file), "dt") for file in target_files]

    # BOŞ DATE'LERE NA YERLEŞTİRME
    start_date = "2018-01-01"
    end_date = weather_dfs[0]["dt"].max()
    post_processed_weather = complete_NA_dates(weather_dfs[0][weather_dfs[0]["dt"] >= start_date], start_date, end_date)

    # WEATHER'IN BİRLEŞMESİ
    for weather_df in weather_dfs[1:]:
        post_processed_weather = merge_and_fill_na(post_processed_weather, weather_df, "dt")

    # İKİ DF BİRLEŞİMİ SONRASI ANA OLAN İLE ÇİFTLERİN ATILMASI
    merged_weather = pd.concat([post_processed_weather, *weather_dfs[1:]]).drop_duplicates("dt").sort_values(
        by="dt").reset_index(drop=True)
    merged_target = pd.concat(target_dfs).drop_duplicates("dt").sort_values(by="dt").reset_index(drop=True)

    data_combined = pd.merge(merged_weather, merged_target, on="dt", how="inner")

    #YENİ CSV OLUŞUMU VE KAYDEDİLMESİ
    output_path = os.path.join(data_dir, "data_combined.csv")
    data_combined.to_csv(output_path, index=False)
    print("Data Saved.")

data_directory = r'C:\Users\u47455\Desktop\regression_data'
weather_files = ["weather_20180101_20211121.csv", "weather_20201004_20211222.csv"]
target_files = ["target_20180101_20210930.csv", "target_20211001_20211223.csv"]

load_and_process_data(data_directory, weather_files, target_files)
