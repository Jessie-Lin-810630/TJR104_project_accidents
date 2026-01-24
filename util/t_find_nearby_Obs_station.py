import pandas as pd
import geopandas as gpd  # poetry add geopandas。GeoPandas輔助判斷什麼叫「附近」、「重疊」。
from sqlalchemy import create_engine, types, text
from pathlib import Path
import os
from dotenv import load_dotenv


def find_nearby_Obs_stn_in_buffer_area(accident_df: pd.DataFrame,
                                       station_df: pd.DataFrame) -> pd.DataFrame:
    # Step2-1: 將 Pandas的DataFrame 轉換為 GeoPandas的GeoDataFrame
    gdf_Accident = gpd.GeoDataFrame(accident_df,
                                    # geometry 把經緯度字串轉成pandas看得懂的"點(Point)"，並放在geometry這新的一欄。
                                    geometry=gpd.points_from_xy(
                                        accident_df["longitude (WGS84)"], accident_df["latitude (WGS84)"]),
                                    crs="EPSG:4326",)

    gdf_Obs_stn = gpd.GeoDataFrame(station_df,
                                   geometry=gpd.points_from_xy(
                                       station_df["Longitude (WGS84)"], station_df["Latitude (WGS84)"]),
                                   crs="EPSG:4326")

    # Step2-2: 將geometry那欄的座標系轉換為"EPSG 3826公尺座標系"，代表事故地點在公尺座標系下的位置
    gdf_Accident_3826 = gdf_Accident.to_crs(epsg=3826)
    gdf_Obs_stn_3826 = gdf_Obs_stn.to_crs(epsg=3826)

    # Step2-3: 為事故地點建立 半徑10000 公尺的緩衝區 (Buffer)
    gdf_Accident_3826['buffer'] = gdf_Accident_3826.geometry.buffer(
        distance=10000)  # 會多加一欄buffer，資料型別也是Geometry

    # Step2-4: 空間連接(Spatial Join)、找區域交疊
    # 代表改用buffer的值當作事故地點的定位，而不是剛剛產生的geometry，即:把 點(Point) 換成 面(Polygon)
    gdf_Accident_3826 = gdf_Accident_3826.set_geometry("buffer")

    # 尋找事故地點附近10000公尺的緩衝區內是否出現觀測站，並全部列出。
    nearby_Obs_stn = gpd.sjoin(
        gdf_Accident_3826, gdf_Obs_stn_3826, how="left", predicate="intersects", distance="distances")

    return nearby_Obs_stn

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
        df_A2 = pd.read_sql("""SELECT *
                                FROM Accident_A2""", con=conn)
        df_Obs_stn = pd.read_sql("""SELECT Station_ID, `Longitude (WGS84)`, `Latitude (WGS84)`
                                    FROM Obs_Stations""", conn)
except Exception as e:
    print(f"Error!!{e}")
finally:
    # Step 2: 轉換座標，找尋周圍的氣象觀測站
    find_nearby_Obs_stn_in_buffer_area(df_A1, df_Obs_stn)
    find_nearby_Obs_stn_in_buffer_area(df_A2, df_Obs_stn)
