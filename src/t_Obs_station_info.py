import pandas as pd
from pathlib import Path

curr_dir = Path(__file__).resolve().parent

# Step 1: 定義好讀資料時的資料型態。
col_map = {"站號": {"name": "Station_ID", "Type": object},
           "站名": {"name": "Station_name", "Type": object},
           "海拔高度": {"name": "Sea_level", "Type": float},
           "經度": {"name": "Longtitude", "Type": float},
           "緯度": {"name": "Latitude", "Type": float},
           }
dtypes = {k: v["Type"] for k, v in col_map.items()}
df = pd.read_csv(curr_dir.parent /
                 "supplementary_weather_csv_from_CODiS/station_info_table.csv",
                 encoding="utf-8-sig",
                 dtype=dtypes)

# Step 2: 將欄位名稱由中文轉為英文。
col_name = {k: v["name"] for k, v in col_map.items()}
df_weather_obs_stations = df.rename(columns=col_name)

if __name__ == "__main__":
    # Step 3: 存檔。
    df_weather_obs_stations.to_csv(curr_dir.parent /
                                   "supplementary_weather_csv_from_CODiS/station_info_table_Eng.csv",
                                   encoding="utf-8",
                                   index=False)  # 不用匯入index，因為站號已經是有識別用了
