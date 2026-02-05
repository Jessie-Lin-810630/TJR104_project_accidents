from t_find_nearby_Obs_station import nearby_Obs_stn
import pandas as pd
from sqlalchemy import create_engine, types
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Step 1: 準備與本地端MySQL server的連線
# load_dotenv()
# username = quote_plus(os.getenv("mysqllocal_username"))
# password = quote_plus(os.getenv("mysqllocal_password"))
# server = "127.0.0.1:3306"
# DB = "TESTDB"
# engine = create_engine(f"mysql+pymysql://{username}:{password}@{server}/{DB}",)
# writer = quote_plus(os.getenv("mail_address"))

# or, Connect to GCP VM MySQL server
load_dotenv()
username = quote_plus(os.getenv("mysql_username"))
password = quote_plus(os.getenv("mysql_password"))
server = "127.0.0.1:3307"
DB = "test_weather"
engine = create_engine(
    f"mysql+pymysql://{username}:{password}@{server}/{DB}",)
writer = quote_plus(os.getenv("mail_address"))

try:
    with engine.begin() as conn:
        # 存入SQL server。append
        nearby_Obs_stn["Created_by"] = writer
        nearby_Obs_stn.to_sql("Accident_nearby_obs_stn", con=conn, index=False,
                              if_exists="append", method="multi", chunksize=1000)
except Exception as e:
    print(f"Error: {e}")
else:
    print("資料表Accident_nearby_obs_stn成功插入！")
