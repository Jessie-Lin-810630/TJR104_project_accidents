from sqlalchemy import create_engine, text, types
from sqlalchemy import inspect
import pandas as pd
from pathlib import Path
import os
from dotenv import load_dotenv
import numpy as np
from urllib.parse import quote_plus


def clean_special_symbols(df: pd.DataFrame) -> pd.DataFrame:
    # 定義需要取代的符號對應表，各符號的原意參考CODis網站的說明。
    # 建議給一個極小值，例如 0.05
    symbol_replace_map = {
        't': '0.05',
        'T': '0.05',
        '&': '0',
        'x': np.nan,
        'X': np.nan,
        "v": np.nan,
        "V": np.nan,
        "/": np.nan,
        "--": np.nan, }

    # 針對所有欄位進行字串替換
    # 先確保 DataFrame 裡面的資料都是字串，方便替換
    df = df.astype(str)

    for col in df.columns:
        if col not in ["觀測時間(hour)", "觀測站別", "觀測日期"]:
            df[col] = df[col].replace(symbol_replace_map)

            # 將欄位轉回數字
            # coerce若無法轉換成數字會改填NaN。
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def concat_df_with_same_date(files_in_batch: list,  all_col_label_to_concat: list,
                             show_column_labels: list, dtype) -> None:
    tmp_df = []
    for file in files_in_batch:
        try:
            df = pd.read_csv(file, dtype=dtype)
            df = df.iloc[1:]  # 列0是英文標題，用不到，從列1開始清

            # reindex會自動把csv中的欄位補到跟all_col_label_to_concat一樣多，其中欄位如果一開始不存在就會補值NaN
            df = df.reindex(all_col_label_to_concat, axis="columns")
            file_name = file.name

            # 新增觀測站號，以便跟其他同日期的觀測資料表合併了以後還能區分是來自哪個觀測站
            station_id, y, m, md = file_name.replace(
                ".csv", "").split("-")  # C0K400  2024 04  2 22.22
            md = md.split()[0]
            df["觀測站別"] = station_id

            # 清理觀測時間 01 -> 00:01:00
            df["觀測時間(hour)"] = "00" + ":" + \
                df["觀測時間(hour)"].astype(str) + ":" + "00"

            # 新增觀測日期(如果之後上Bigquery、這行不需要)
            df["觀測日期"] = f"{y:04}-{m:02}-{md:02}"

            # 清理特殊符號
            df = clean_special_symbols(df)

            # 轉成英文欄位
            df.columns = show_column_labels

            # 把每一個清好的df先串起來
            tmp_df.append(df)
        except Exception as e:
            print(f"檔案{file}讀取失敗: {e}")

        # 合併這一批檔案
    if tmp_df:
        batch_df = pd.concat(tmp_df, ignore_index=True)

    return batch_df


def weatherdata_save_as_csv(save_to_dir: Path, csv_file_name: Path, batch_df: pd.DataFrame) -> None:
    save_to_dir.mkdir(parents=True, exist_ok=True)
    destination = save_to_dir/csv_file_name
    batch_df.to_csv(destination, encoding="utf-8-sig", index=False)
    print("csv存檔成功")
    return None


def weatherdata_first_load_to_mysql(batch_df: pd.DataFrame, table_name: str, dtype: dict) -> None:
    # 使用 method='multi' 加速
    batch_df.to_sql(table_name,
                    con=engine,
                    if_exists='replace',
                    index=False,
                    chunksize=1000,
                    dtype=dtype,
                    method='multi')

    conn.execute(text(f"""ALTER TABLE `{table_name}`
                                ADD CONSTRAINT FOREIGN KEY (Station_ID) 
                                    REFERENCES Obs_Stations (Station_ID);"""))  # 由於表格會很多，手動定義FK約束名稱會很麻煩，乾脆交給系統。

    conn.execute(text(f"""ALTER TABLE `{table_name}`
                                ADD COLUMN Sequential_order INT AUTO_INCREMENT PRIMARY KEY FIRST;"""))

    conn.execute(
        text(f"ALTER TABLE `{table_name}` ADD COLUMN Created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP;"))

    conn.execute(
        text(f"ALTER TABLE `{table_name}` ADD COLUMN Created_by VARCHAR(50) NOT NULL;"))

    conn.execute(
        text(f"ALTER TABLE `{table_name}` ADD COLUMN Updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP;"))

    conn.execute(
        text(f"ALTER TABLE `{table_name}` ADD COLUMN Updated_by VARCHAR(50) NOT NULL;"))

    conn.execute(
        text(f"UPDATE `{table_name}` SET Created_by = (:user), Updated_by = (:user);"), {'user': "lucky460721@gmail.com"})

    conn.commit()
    print(f"已成功匯入第 {i} 到 {i+batch_size} 個檔案至MySQL")

    return None


def weatherdata_append_to_mysql(batch_df: pd.DataFrame, table_name: str, dtype: dict) -> None:
    # 使用 method='multi' 加速
    batch_df.to_sql(table_name,
                    con=engine,
                    if_exists='append',
                    index=False,
                    chunksize=1000,
                    dtype=dtype,
                    method='multi')

    conn.execute(
        text(f"UPDATE `{table_name}` SET Created_by = (:user), Updated_by = (:user);"), {'user': "lucky460721@gmail.com"})

    conn.commit()
    print(f"已成功匯入第 {i} 到 {i+batch_size} 個檔案至MySQL")

    return None


col_map = {
    "觀測站別": {"name": "Station_ID", "type_in_pd": object, "type_in_sql": types.VARCHAR(20)},
    "觀測日期": {"name": "Observation_date", "type_in_pd": object, "type_in_sql": types.DATE},
    "觀測時間(hour)": {"name": "Observation_time", "type_in_pd": object, "type_in_sql": types.TIME},
    "測站氣壓(hPa)": {"name": "Station_air_pressure", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "海平面氣壓(hPa)": {"name": "Sea_pressure", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "氣溫(℃)": {"name": "Temperature", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "露點溫度(℃)": {"name": "Temp_dew_point", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "相對溼度(%)": {"name": "Relative_humidity", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "風速(m/s)": {"name": "Wind_speed", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "風向(360degree)": {"name": "Wind_direction", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "最大瞬間風(m/s)": {"name": "Wind_speed_gust", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "最大瞬間風風向(360degree)": {"name": "Wind_distant_gust", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "降水量(mm)": {"name": "Precipitation", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "降水時數(h)": {"name": "Precipitation_hour", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "日照時數(h)": {"name": "Sun_shine_hour", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "全天空日射量(MJ/㎡)": {"name": "Global_radiation", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "能見度(km)": {"name": "Visibility", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "能見度_自動(km)": {"name": "Visibility_mean_auto", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "紫外線指數": {"name": "UVI", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "總雲量(0~10)": {"name": "Cloud_amount", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "總雲量_衛星(0~10)": {"name": "Cloud_amount_by_satellites", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "地溫0cm": {"name": "Soil_temp_at_0_cm", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "地溫5cm": {"name": "Soil_temp_at_5_cm", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "地溫10cm": {"name": "Soil_temp_at_10_cm", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "地溫20cm": {"name": "Soil_temp_at_20_cm", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "地溫30cm": {"name": "Soil_temp_at_30_cm", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "地溫50cm": {"name": "Soil_temp_at_50_cm", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
    "地溫100cm": {"name": "Soil_temp_at_100_cm", "type_in_pd": float, "type_in_sql": types.DECIMAL(6, 1)},
}

all_col_label_to_concat = [k for k in col_map.keys()]
col_name_eng = [v['name'] for v in col_map.values()]
dtype_to_pd = {v['name']: v['type_in_pd'] for k, v in col_map.items()}
dtype_to_sql = {v['name']: v['type_in_sql'] for k, v in col_map.items()}

if __name__ == "__main__":
    load_dotenv()
    # Step 1: 建立Weather_data資料表所屬資料庫的連線，假設這裡是一台本地的MySQL server
    # username = os.getenv("mysqllocal_username")
    # password = os.getenv("mysqllocal_password")
    # server = "127.0.0.1:3306"
    # DB = "TESTDB"
    # engine = create_engine(
    #     f"mysql+pymysql://{username}:{password}@{server}/{DB}",)

    # 或是連GCP上的MySQL server
    username = quote_plus(os.getenv("mysql_username"))
    password = quote_plus(os.getenv("mysql_password"))
    server = "127.0.0.1:3307"
    db_name = "test_db"
    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{server}/{db_name}",)

    # Step 2: soucrce_dir下面的子資料夾是按觀測日期分類命名，故可資料夾內讀取各測站同日的天氣觀測數據csv file，做以下清理：
    # 1. 不同代碼開頭的csv，因為內容欄位數量不同，所以會上下整併同一天所有測站的csv，將欄位數量擴展到全部一樣寬後，補值。
    # 2. 此外，欄位下的資料若為特殊符號，需要特別回去找對應政府的解釋，才能正確補值。

    curr_dir = Path().resolve()
    source_dir = curr_dir/"supplementary_weather_csv_from_CODiS"
    dir_round = 0
    for dir in source_dir.iterdir():  # dt=2026-01-01、dt=2025-12-31..
        print(f"正在尋找{dir.name}資料夾有幾個站提供的csv......")
        all_files = [file for file in dir.glob("*??-????-??-?*.csv")]
        batch_size = 100  # 因爲最誇張可能來到800個csv要打開，先設計成批次處理，避免記憶體過度負擔
        for i in range(0, len(all_files), batch_size):

            files_in_batch = all_files[i: i + batch_size]
            # 執行其中一輪step 2:
            concat_df = concat_df_with_same_date(files_in_batch, all_col_label_to_concat,
                                                 col_name_eng, dtype_to_pd)

            # Step 3: 匯入MySQL
            with engine.connect() as conn:
                if i == 0:
                    weatherdata_first_load_to_mysql(
                        concat_df, f'{dir.name}_Historical_Weather_Observations', dtype_to_sql)
                else:
                    weatherdata_append_to_mysql(
                        concat_df, f'{dir.name}_Historical_Weather_Observations', dtype_to_sql)
            # Step 4 (optional): 存成csv
            # weatherdata_save_as_csv(
            # curr_dir/"supplementary_concat_csv", f"concat_all_stn_{dir.name}_{i}to{i+batch_size}.csv", concat_df)
        if dir_round == 5:
            break
        else:
            dir_round += 1
