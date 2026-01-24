import pandas as pd
import geopandas as gpd  # poetry add geopandas。GeoPandas輔助判斷什麼叫「附近」、「重疊」。
from sqlalchemy import create_engine, types, text
from pathlib import Path
import os
from dotenv import load_dotenv


# 本檔案步驟:
# 讀取Obs_station_info與某一份車禍資料表主表
# 遍歷每一列車禍資料表主表，將經緯度(WGS84座標系)取出並與Obs_station_info中的觀測站經緯度比對
# 然後算出最近距離的一站。
# 將觀測站別寫入車禍資料表主表。

def find_nearest_Obs_station(df_accident: pd.DataFrame,
                             df_station: pd.DataFrame) -> pd.DataFrame:
    """Docstring待補充"""

    # Step2-1: 將 Pandas的DataFrame 轉換為 GeoPandas的GeoDataFrame
    gdf_A1 = gpd.GeoDataFrame(df_accident,
                              # geometry 把經緯度字串轉成pandas看得懂的"點(Point)"，並放在geometry這新的一欄。
                              geometry=gpd.points_from_xy(
                                  df_accident["longitude (WGS84)"], df_accident["latitude (WGS84)"]),
                              crs="EPSG:4326",)

    gdf_Obs_stn = gpd.GeoDataFrame(df_station,
                                   geometry=gpd.points_from_xy(
                                       df_station["Longitude (WGS84)"], df_station["Latitude (WGS84)"]),
                                   crs="EPSG:4326")

    # Step2-2: 將geometry那欄的座標系轉換為"EPSG 3826公尺座標系"，代表事故地點在公尺座標系下的位置
    gdf_A1_3826 = gdf_A1.to_crs(epsg=3826)
    gdf_Obs_stn_3826 = gdf_Obs_stn.to_crs(epsg=3826)

    # Step2-3: 空間連接(Spatial Join)、找最近的地點
    gdf_A1_3826 = gdf_A1_3826.set_geometry("geometry")
    gdf_A1_3826_with_nearest_Obs_stn = gpd.sjoin_nearest(gdf_A1_3826, gdf_Obs_stn_3826,
                                                         how="left",
                                                         distance_col="distances",)

    # Step2-4: 確保一個 accident_id 只對應到一個站點 (取第一個)
    gdf_A1_final = gdf_A1_3826_with_nearest_Obs_stn.drop_duplicates(subset=[
        'accident_id'])

    # Step2-5: 只取最終要寫入Accident_A1/A2主表的欄位
    df_to_append = gdf_A1_final[['accident_id', 'Station_ID']].copy()

    return df_to_append


# Step1: 讀取歷史事故資料之主表 與 觀測站基本地理資訊資料表

load_dotenv()
username = os.getenv("mysqllocal_username")
password = os.getenv("mysqllocal_password")
server = "127.0.0.1:3306"
DB = "TESTDB"
engine = create_engine(f"mysql+pymysql://{username}:{password}@{server}/{DB}",)
try:
    with engine.connect() as conn:
        df_A1 = pd.read_sql("""SELECT *
                                FROM Accident_A1""", con=conn)
        df_Obs_stn = pd.read_sql("""SELECT Station_ID, `Longitude (WGS84)`, `Latitude (WGS84)`
                                    FROM Obs_Stations""", conn)
except Exception as e:
    print(f"Error!!{e}")
finally:
    df_A1_to_append = find_nearest_Obs_station(df_A1, df_Obs_stn)
    # df_A2_to_append = find_nearest_Obs_station(df_A2, df_Obs_stn)
