import pandas as pd
from pathlib import Path

curr_dir = Path().resolve()

# Step 1: 定義好讀資料時的資料型態。
col_map = {"站號": {"name": "Station_ID", "Type": object},
           "站名": {"name": "Station_name", "Type": object},
           "海拔高度": {"name": "Sea_level", "Type": float},
           "經度": {"name": "Longitude (WGS84)", "Type": float},
           "緯度": {"name": "Latitude (WGS84)", "Type": float},
           "資料起始日期": {"name": "Date_of_Opening", "Type": object}
           }
dtypes = {k: v["Type"] for k, v in col_map.items()}
df = pd.read_csv(curr_dir /
                 "supplementary_weather_csv_from_CODiS/station_info_table.csv",
                 encoding="utf-8-sig",
                 dtype=dtypes)

# Step 2: 將欄位名稱由中文轉為英文。
col_name = {k: v["name"] for k, v in col_map.items()}
df_weather_obs_stations = df.rename(columns=col_name)

# Step 3: 確保資料起始日期為datetime認可的格式
pd.to_datetime(
    df_weather_obs_stations["Date_of_Opening"], format="%Y%m%d", errors="coerce").dt.strftime("%Y-%m%-d")

if __name__ == "__main__":
    # Step 3: 存檔。
    df_weather_obs_stations.to_csv(curr_dir /
                                   "supplementary_weather_csv_from_CODiS/station_info_table_Eng.csv",
                                   encoding="utf-8-sig",
                                   index=False)  # 不用匯入index，因為站號已經是有識別用了
