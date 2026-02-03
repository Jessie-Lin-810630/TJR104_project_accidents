from t_find_nearby_Obs_station import nearby_Obs_stn
import pandas as pd
import geopandas as gpd  # poetry add geopandas。GeoPandas輔助判斷什麼叫「附近」、「重疊」。
from sqlalchemy import create_engine, types, text
import os
from dotenv import load_dotenv


load_dotenv()
username = os.getenv("mysqllocal_username")
password = os.getenv("mysqllocal_password")
server = "127.0.0.1:3306"
DB = "TESTDB"
engine = create_engine(f"mysql+pymysql://{username}:{password}@{server}/{DB}",)

# 以新的資料表存入SQL。create or replace
dtype = {"accident_id": types.VARCHAR(100),
         "accident_date": types.DATE,
         "accident_time": types.TIME,
         "state_valid_from": types.DATE,
         "station_record_id": types.INTEGER,
         "station_id": types.VARCHAR(10),
         "distance": types.DECIMAL(10, 6),
         "rank_of_distance": types.INTEGER,
         }
nearby_Obs_stn.to_sql("accident_nearby_obs_stn", con=engine.connect(),
                      if_exists="replace", dtype=dtype, index=False)
