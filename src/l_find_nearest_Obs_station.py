from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from t_find_nearest_Obs_station import df_A1_to_append

load_dotenv()
username = os.getenv("mysqllocal_username")
password = os.getenv("mysqllocal_password")
server = "127.0.0.1:3306"
DB = "TESTDB"
engine = create_engine(f"mysql+pymysql://{username}:{password}@{server}/{DB}",)

# 先把df_to_append暫放到另外的臨時表後再join、update，會比在python直接操作update敘述來得快。
with engine.connect() as conn:
    df_A1_to_append.to_sql("tmp_station_near_accidents", conn, if_exists="replace",
                           chunksize=1024*1024,
                           index=False)

    conn.execute(text("""UPDATE Accident_A1
                            JOIN tmp_station_near_accidents AS Tmp 
                                ON Accident_A1.accident_id = Tmp.accident_id
                            SET Accident_A1.Nearest_station_ID = Tmp.Station_ID;"""))
    conn.execute(text("""UPDATE Accident_A1
                            SET Updated_by = (:user)"""), {"user": "lucky460721@gmail.com;"})

    conn.execute(text("""UPDATE Accident_A1
                            SET Updated_on = NOW();"""))

    conn.execute(text("""DROP TABLE tmp_station_near_accidents;"""))
    conn.commit()

    print("更新完成！")
