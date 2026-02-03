from t_Obs_station_info import df_new_stn, df_existing_stn_change
import os
# terminal執行poetry add python-dotenv或pip install python-dotenv
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime

# Step 1: 讀取現在資料表中的既有資料
# Step 1-1: 準備與本地端MySQL server的連線
load_dotenv()
username = quote_plus(os.getenv("mysqllocal_username"))
password = quote_plus(os.getenv("mysqllocal_password"))
server = "127.0.0.1:3306"
db_name = "TESTDB"
engine = create_engine(
    f"mysql+pymysql://{username}:{password}@{server}/{db_name}",)  # 建立engine物件
writer = quote_plus(os.getenv("mail_address"))

# 或Step 1-1: 準備與GCP VM上的MySQL server的連線
# load_dotenv()
# username = quote_plus(os.getenv("mysql_username"))
# password = quote_plus(os.getenv("mysql_password"))
# server = "127.0.0.1:3307"
# db_name = "test_db"
# engine = create_engine(
#     f"mysql+pymysql://{username}:{password}@{server}/{db_name}",
# )  # 建立engine物件
# writer = quote_plus(os.getenv("mail_address"))

# Step 1-2: 建立連線
with engine.connect() as conn:
    df_new_stn_l = df_new_stn.copy()
    df_existing_stn_change_l = df_existing_stn_change.copy()
    row_info_to_insert = []
    # 遍歷每個資料列，把要插入MySQL的欄位名稱的值，做成list。
    for _, row in df_new_stn_l.iterrows():
        row_info_to_insert.append({
            "Station_id": row["Station_id"],
            "Station_name": row["Station_name"],
            "Station_sea_level": row["Station_sea_level"],
            "Station_longitude_WGS84": row["Station_longitude_WGS84"],
            "Station_latitude_WGS84": row["Station_latitude_WGS84"],
            "Station_working_state": row["Station_working_state_new"],
            "State_valid_from": row["State_valid_from"],
            "State_valid_to": datetime(9999, 12, 31).strftime("%Y-%m-%d"),
            "Remark": row["Remark"],
            "Created_by": writer,
            "Updated_by": writer,
        })
        df_to_insert = pd.DataFrame(row_info_to_insert)
        df_to_insert.to_sql("Ob s_stations", conn,
                            index=False, if_exists="append")
        conn.commit()

    for _, row in df_existing_stn_change_l.iterrows():
        if row["Station_working_state_new"] is np.nan:
            dml_text = text("""UPDATE Obs_stations 
                                    SET
                                        Station_working_State = "Previous Run",
                                        State_valid_to = (:closing_date),
                                        Updated_on = CURRENT_TIMESTAMP,
                                        Updated_by = (:writer)
                                    WHERE Station_record_id = (:record_id);
                            """)
            conn.execute(dml_text, {"closing_date": datetime.now().strftime("%Y-%m-%d"),
                                    "writer": writer,
                                    "record_id": row["Station_record_id"],
                                    })
        conn.commit()
