from sqlalchemy import create_engine, text, types
import pandas as pd
import hashlib
from pathlib import Path
import os
from dotenv import load_dotenv
import numpy as np
from urllib.parse import quote_plus

col_map = {
    "Station_record_id": {"name": "Station_record_id", "type_in_pd": int, "type_in_sql": types.INTEGER},
    "觀測站別": {"name": "Station_id", "type_in_pd": object, "type_in_sql": types.VARCHAR(10)},
    "Observation_datetime": {"name": "Observation_datetime", "type_in_pd": object, "type_in_sql": types.DATETIME},
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

all_col_label_to_concat = [k for k in col_map.keys()]  # 所有csv檔中的欄位之聯集
col_name_eng = [v['name'] for v in col_map.values()]  # 英文版欄位名
dtype_to_pd = {v['name']: v['type_in_pd'] for k, v in col_map.items()
               if k != "Station_record_id" and k != "Observation_datetime"}
dtype_to_sql = {v['name']: v['type_in_sql']
                for k, v in col_map.items() if k != "觀測時間(hour)"}


def clean_special_symbols(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the special symbols in the source document csv."""
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
        if col not in ["觀測時間(hour)", "觀測站別", "Observation_datetime"]:
            df[col] = df[col].replace(symbol_replace_map)

            # 將欄位轉回數字
            # coerce若無法轉換成數字會改填NaN。
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def concat_df_with_same_date(files_in_batch: list,  all_col_label_to_concat: list,
                             show_column_labels: list, dtype) -> pd.DataFrame | None:
    """Read the sourced csv files in seperate dataframes, and expand the column width of 
    all the dataframes to the same. After that, sequential data clean is executed."""

    # 預期到下載的csv沒有station_record_id，無法追蹤是來自哪個時期運作的哪個觀測站，故需要補回record_id，查詢record_id、預先包成字典
    def create_fk_mapping_cache(engine) -> dict:
        """Select and keep the station_record_id that has 
        been stored in MySQL server database, in a dictionary."""
        mapping_record_id = {}

        with engine.begin() as conn:
            dql_text = text("""SELECT DISTINCT Station_record_id, Station_id,
                                    State_valid_from, State_valid_to
                                FROM Obs_stations;
                            """)
            mapping_df = pd.read_sql(dql_text, conn)

            # 建立station_record_id與車禍資料的索引
            for _, row in mapping_df.iterrows():
                stn_key = row["Station_id"]
                from_date = pd.to_datetime(row["State_valid_from"])
                to_date = pd.to_datetime(row["State_valid_to"])
                stn_record_id = row["Station_record_id"]
                mapping_record_id[stn_key] = [
                    from_date, to_date, stn_record_id]

        print(f"FK映射快取建立完成：{len(mapping_record_id)} 組映射")

        return mapping_record_id  # {'U2HA40':['2018-09-04','9999-12-31',856],}

    # 呼叫函式
    mapping_record_id = create_fk_mapping_cache(engine)

    tmp_df = []
    for file in files_in_batch:
        try:
            df = pd.read_csv(file, dtype=dtype)
            df = df.iloc[1:]  # 列0是英文標題，用不到，從列1開始清

            # reindex會自動把csv中的欄位補到跟all_col_label_to_concat一樣多，其中欄位如果一開始不存在就會補值NaN
            df = df.reindex(all_col_label_to_concat, axis="columns")

            # 新增觀測站號，以便跟其他同日期的觀測資料表合併了以後還能區分是來自哪個觀測站
            file_name = file.name
            station_id, y, m, md = file_name.replace(
                ".csv", "").split("-")  # C0K400  2024 04  2 22.22
            md = md.split()[0]
            df["觀測站別"] = station_id

            # 清理觀測時間 01 -> 00:01:00
            df["觀測時間(hour)"] = np.where(df["觀測時間(hour)"] == "24", "00",
                                        df["觀測時間(hour)"])
            df["觀測時間(hour)"] = "00" + ":" + \
                df["觀測時間(hour)"].astype(str) + ":" + "00"

            # 整併成觀測日期時間Observation_datetime
            df["Observation_datetime"] = f"{y:04}-{m:02}-{md:02} " + \
                df["觀測時間(hour)"]

            # 清理特殊符號
            df = clean_special_symbols(df)

            # 補上station_record_id欄位
            df["Station_record_id"] = pd.NA  # 初始化

            for i, row in df.iterrows():
                stn_id = row["觀測站別"]
                obs_date = pd.to_datetime(row["Observation_datetime"])
                if mapping_record_id[stn_id][0] <= obs_date < mapping_record_id[stn_id][1]:
                    df.at[i, "Station_record_id"] = int(
                        mapping_record_id[stn_id][2])

            # 轉成英文欄位
            df.columns = show_column_labels

            # 只留想要的欄位
            df = df.drop(columns="Observation_time")

            # 把每一個清好的df先串起來
            tmp_df.append(df)
        except Exception as e:
            print(f"檔案{file}讀取失敗: {e}")

    # 合併這一批檔案
    if tmp_df:
        batch_df = pd.concat(tmp_df, ignore_index=True)

        # 生成PK
        pk = (batch_df["Station_record_id"].astype(str).str.strip() + "|" +
              pd.to_datetime(batch_df["Observation_datetime"]).astype(str))
        batch_df["Hash_value"] = pk.apply(
            lambda x: hashlib.sha256(x.encode()).hexdigest())
        return batch_df
    else:
        return None


def weatherdata_save_as_csv(save_path: Path, csv_file_name: Path, batch_df: pd.DataFrame) -> None:
    save_path.mkdir(parents=True, exist_ok=True)
    destination = save_path/csv_file_name
    batch_df.to_csv(destination, encoding="utf-8-sig", index=False)
    print("csv存檔成功")
    return None


def weatherdata_append_to_mysql(batch_df: pd.DataFrame, engine, table_name: str, dtype: dict, writer: str) -> None:

    try:
        with engine.begin() as conn:
            batch_df["Created_by"] = writer
            batch_df.to_sql(table_name, con=conn,
                            if_exists='append',
                            index=False,
                            dtype=dtype,
                            method='multi',
                            chunksize=200)  # 使用 method='multi' 加速
    except Exception as e:
        error_code, *error_msg = e.args
        print(f"Error! {error_code}")
    else:
        print(f"已成功匯入第 {i}到{len(files_in_batch)}個檔案至{table_name}!")
    return None


# Step 1: 準備與本地端MySQL server的連線
load_dotenv()
username = quote_plus(os.getenv("mysqllocal_username"))
password = quote_plus(os.getenv("mysqllocal_password"))
server = "127.0.0.1:3306"
DB = "TESTDB"
engine = create_engine(
    f"mysql+pymysql://{username}:{password}@{server}/{DB}",)
writer = quote_plus(os.getenv("mail_address"))

# 或是連GCP上的MySQL server
load_dotenv()
gcpusername = quote_plus(os.getenv("mysql_username"))
gcppassword = quote_plus(os.getenv("mysql_password"))
gcpserver = "127.0.0.1:3307"
gcpdb_name = "test_weather"
gcpengine = create_engine(
    f"mysql+pymysql://{gcpusername}:{gcppassword}@{gcpserver}/{gcpdb_name}",)
writer = quote_plus(os.getenv("mail_address"))


# Step 3: soucrce_dir下面的子資料夾是按觀測日期分類命名，故可資料夾內讀取各測站同日的天氣觀測數據csv file，做以下清理：
# 1. 不同代碼開頭的csv，因為內容欄位數量不同，所以會上下整併同一天所有測站的csv，將欄位數量擴展到全部一樣寬後，補值。
# 2. 此外，欄位下的資料若為特殊符號，需要特別回去找對應政府的解釋，才能正確補值。

curr_dir = Path().resolve()
source_dir = curr_dir/"processed_csv"/"partitioned_weather_csv"

for dir in source_dir.iterdir():  # dt=2026-01-01、dt=2025-12-31..
    print(f"正在尋找{dir.name}資料夾有幾個站提供的csv......")
    all_files = [file for file in dir.glob("*??-????-??-?*.csv")]
    batch_size = 500
    for i in range(0, len(all_files), batch_size):

        files_in_batch = all_files[i: i + batch_size]
        # Step 3-1&3-2:
        concat_df = concat_df_with_same_date(files_in_batch, all_col_label_to_concat,
                                             col_name_eng, dtype_to_pd)

        # Step 4: 匯入MySQL
        weatherdata_append_to_mysql(
            concat_df, gcpengine, "Historical_weather_data", dtype_to_sql, writer)

        # Step 5 (optional): 存成csv
        # weatherdata_save_as_csv(
        #     curr_dir/"processed_csv"/"concat_weather_csv", f"concat_all_stn_{dir.name}_{i}to{i+batch_size}.csv", concat_df)
