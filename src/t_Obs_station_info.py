import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from datetime import datetime
from urllib.parse import quote_plus
from dotenv import load_dotenv
import os
from e_Obs_station_info import df_weather_obs_stations
import numpy as np

# Step 1: 讀取現在資料表中的既有資料
# Step 1-1: 準備與本地端MySQL server的連線
load_dotenv()
username = quote_plus(os.getenv("mysqllocal_username"))
password = quote_plus(os.getenv("mysqllocal_password"))
server = "127.0.0.1:3306"
db_name = "TESTDB"
# 建立connection物件
conn = create_engine(
    f"mysql+pymysql://{username}:{password}@{server}/{db_name}",).connect()
writer = quote_plus(os.getenv("mail_address"))

# 或Step 1-1: 準備與GCP VM上的MySQL server的連線
# load_dotenv()
# username = quote_plus(os.getenv("mysql_username"))
# password = quote_plus(os.getenv("mysql_password"))
# server = "127.0.0.1:3307"
# db_name = "test_db"
# conn = create_engine(
#     f"mysql+pymysql://{username}:{password}@{server}/{db_name}",).connect()
# writer = quote_plus(os.getenv("mail_address"))

# Step 1-2: 建立連線

# Step 2: 準備今日爬蟲的結果，並先給適量清洗
# Step 2-1: 定義好讀資料時的資料型態。
col_map_csv = {"站號": {"name": "Station_id", "type_in_pd": object},
               "站名": {"name": "Station_name", "type_in_pd": object},
               "海拔高度(m)": {"name": "Station_sea_level", "type_in_pd": float},
               "經度": {"name": "Station_longitude_WGS84", "type_in_pd": float},
               "緯度": {"name": "Station_latitude_WGS84", "type_in_pd": float},
               "資料起始日期": {"name": "State_valid_from", "type_in_pd": object},
               "備註": {"name": "Remark", "type_in_pd": object},
               }
dtypes = {k: v["type_in_pd"] for k, v in col_map_csv.items()}

# Step 2-2: 讀取src/e_Obs_station_info.py爬下來的dataframe
# df = df_weather_obs_stations.copy()
df = pd.read_csv(Path().resolve() /
                 "supplementary_weather_csv_from_CODiS/station_info_table.csv", na_values=None)

col_name = {k: v["name"] for k, v in col_map_csv.items()}
df_crawlered_obs_stn = df.rename(columns=col_name)

# Step 2-4: 確保資料起始日期為datetime認可的格式
df_crawlered_obs_stn["State_valid_from"] = pd.to_datetime(
    df_crawlered_obs_stn["State_valid_from"], format="ISO8601", errors='coerce').dt.strftime("%Y-%m-%d")

# Step 2-5: 標示測站工作狀態，由於前面e_Obs_station_info.py指定div id為existing_station，故工作狀態都是running
df_crawlered_obs_stn["Station_working_state"] = "Running"

# Step 3: 改讀取既有資料表`Obs_Stations`到dataframe，篩選各測站最新的資料
df_recorded_obs_stn = pd.read_sql("SELECT * FROM Obs_Stations;", conn)
df_recorded_obs_stn["State_valid_to"] = pd.to_datetime(
    df_recorded_obs_stn["State_valid_to"])

df_curr_of_each_stn = df_recorded_obs_stn[df_recorded_obs_stn["State_valid_to"] ==
                                          datetime(9999, 12, 31)]
df_curr_of_each_stn = df_curr_of_each_stn[[
    "Station_id", "Station_working_state", "Station_record_id"]]

# Step 4: 將Step2&Step3做join，讓接下來可以同一列、跨欄比較要以哪幾個資料為主。
# 目標是過濾出新增的測站
# 由於爬蟲時可能發現新的測站，所以以爬蟲存下的df為左表、left-join先前存的資料表。
df_merged1 = df_crawlered_obs_stn.merge(df_curr_of_each_stn, how="left",
                                        left_on="Station_id", right_on="Station_id",
                                        suffixes=("_new", "_in_sqlserver"))

# 新的測站在既有資料表的Station_working_state這個欄位會是null，可以依此判斷爬到的結果是否屬新測站。
df_new_stn = df_merged1[df_merged1["Station_working_state_in_sqlserver"].isna()]

# Step 5: 將Step2&Step3做join，讓接下來可以同一列、跨欄比較要以哪幾個資料為主。
# 目標是過濾出狀態發生變化的既有測站，既有測站若暫停服務或中斷後重啟，則需要小心為資料表做同步更新
# 所以join時，以先前存在SQL server的資料表為左表、left-join爬蟲存下的df。
df_merged2 = df_curr_of_each_stn.merge(df_crawlered_obs_stn, how="left",
                                       left_on="Station_id", right_on="Station_id",
                                       suffixes=("_in_sqlserver", "_new"))

# 既有測站暫停服務 -> Station_working_state_new: null，Station_working_state_in_sqlserver: running
# 既有測站中斷後重啟 -> Station_working_state_new: running，Station_working_state_in_sqlserver: Previous Run
df_existing_stn_change = df_merged2[df_merged2["Station_working_state_new"]
                                    != df_merged2["Station_working_state_in_sqlserver"]]


if __name__ == "__main__":
    # (optional)存成csv，
    # (optional)設定未來存檔資料夾路徑
    curr_dir = Path().resolve()
    save_to_dir = curr_dir/"supplementary_weather_csv_from_CODiS"
    save_to_dir.mkdir(parents=True, exist_ok=True)
    df_crawlered_obs_stn.to_csv(save_to_dir/"station_info_table_Eng.csv",
                                encoding="utf-8-sig",
                                index=False)  # 不用匯入index，因為站號已經是有識別用了
    df_existing_stn_change.to_csv(save_to_dir/"Existing_stations_status_change.csv",
                                  encoding="utf-8-sig",
                                  index=False)
    df_new_stn.to_csv(save_to_dir/"New_stations_to_be_added.csv",
                      encoding="utf-8-sig",
                      index=False)
