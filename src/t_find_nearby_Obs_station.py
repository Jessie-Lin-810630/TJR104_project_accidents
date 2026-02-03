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
                                       station_df["Station_longitude_WGS84"], station_df["Station_latitude_WGS84"]),
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

    # Step2-5: 尋找事故地點附近10000公尺的緩衝區內是否出現觀測站，並全部列出。
    nearby_Obs_stn = gpd.sjoin(
        gdf_Accident_3826, gdf_Obs_stn_3826, how="left", predicate="intersects")

    # Step2-6: 把原本算好的觀測站point(at epsg 3826)合併
    nearby_Obs_stn = nearby_Obs_stn.merge(gdf_Obs_stn_3826, how="left",
                                          on="Station_record_id", suffixes=["_left", "_right"])

    # 合併後，為每一筆觀測站與事故地點的point做距離計算，新增到distance欄位
    nearby_Obs_stn["distance"] = nearby_Obs_stn["geometry_left"].distance(
        nearby_Obs_stn["geometry_right"])

    # Step2-7: 踢除不需呈現的欄位
    nearby_Obs_stn = nearby_Obs_stn[["accident_id", "accident_date", "accident_time",
                                    "Station_record_id", "Station_id_left",
                                     "State_valid_from_left", "distance"]]

    # Step2-8: 剔除State_valid_from_left晚於accident_date的資料表，因為此情境會無法順利取得accident_date天氣
    nearby_Obs_stn = nearby_Obs_stn[nearby_Obs_stn["State_valid_from_left"] <
                                    nearby_Obs_stn["accident_date"]]

    # Step2-9: 為每個事故地點排出2個最近的觀測站，並且有標記名次(近者排名1、遠則排名2)

    nearby_two_stn = nearby_Obs_stn.sort_values(
        by=["accident_id", "distance"], axis=0, ascending=[True, True], na_position="first")
    nearby_two_stn["rank_of_distance"] = nearby_two_stn.groupby(
        "accident_id").cumcount()+1
    nearby_two_stn = nearby_two_stn[nearby_two_stn["rank_of_distance"] <= 2]

    # 修改欄位命名
    nearby_two_stn.columns = [
        "accident_id", "accident_date", "accident_time",
        "station_record_id", "station_id",
        "state_valid_from", "distance", "rank_of_distance"]

    # 確保一下data type沒跑掉，尤其是日期時間。
    nearby_two_stn["accident_date"] = pd.to_datetime(
        nearby_two_stn["accident_date"], "coerce", format="mixed").dt.strftime("%Y-%m-%d")
    nearby_two_stn["state_valid_from"] = pd.to_datetime(
        nearby_two_stn["state_valid_from"], "coerce", format="mixed").dt.strftime("%Y-%m-%d")
    nearby_two_stn["accident_time"] = nearby_two_stn["accident_time"].astype(
        str).str.replace("0 days ", "")
    return nearby_two_stn


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
                                FROM Accident_A1_113y""", con=conn)
        df_A2 = pd.read_sql("""SELECT *
                                FROM Accident_A2_113y""", con=conn)
        df_Obs_stn = pd.read_sql("""SELECT `Station_record_id`, `Station_id`, 
                                           `Station_longitude_WGS84`, 
                                           `Station_latitude_WGS84`, `State_valid_from`
                                        FROM Obs_stations
                                            WHERE Station_working_state = 'Running';
                                 """, con=conn)

        # Step2: 轉換座標，找尋周圍的氣象觀測站
        A1_nearby_Obs_stn = find_nearby_Obs_stn_in_buffer_area(
            df_A1, df_Obs_stn)
        A2_nearby_Obs_stn = find_nearby_Obs_stn_in_buffer_area(
            df_A2, df_Obs_stn)
except Exception as e:
    print(f"Error!!{e}")
else:
    nearby_Obs_stn = pd.concat([A1_nearby_Obs_stn, A2_nearby_Obs_stn])

if __name__ == "__main__":
    curr_dir = Path().resolve()
    save_to_dir = curr_dir/"accident_nearby_obs_stn"
    save_to_dir.mkdir(parents=True, exist_ok=True)
    A1_nearby_Obs_stn.to_csv(save_to_dir/"A1_nearby_Obs_station.csv",
                             encoding="utf-8-sig", index=False)
    A2_nearby_Obs_stn.to_csv(save_to_dir/"A2_nearby_Obs_station.csv",
                             encoding="utf-8-sig", index=False)
    nearby_Obs_stn.to_csv(save_to_dir/"nearby_Obs_station.csv",
                          encoding="utf-8-sig", index=False)
