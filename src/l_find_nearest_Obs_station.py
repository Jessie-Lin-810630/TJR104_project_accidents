from sqlalchemy import create_engine, text, types
import os
from dotenv import load_dotenv
from t_find_nearest_Obs_station import df_A1_to_append, df_A2_to_append
from urllib.parse import quote_plus
import pandas as pd

load_dotenv()
# (儲存方法一)建立與本地端MySQL server的連線
username = os.getenv("mysqllocal_username")
password = os.getenv("mysqllocal_password")
server = "127.0.0.1:3306"
DB = "TESTDB"

# (儲存方法二)建立與GCP VM上的MySQL server的連線
# username = quote_plus(os.getenv("mysql_username"))
# password = quote_plus(os.getenv("mysql_password"))
# server = "127.0.0.1:3307"
# DB = "test_db"


engine = create_engine(f"mysql+pymysql://{username}:{password}@{server}/{DB}",)


# 做法一（較推薦）：直接將df_to_append存成一個資料表，表述事故與觀測站兩個實體的關係。


def dataframe_first_load_to_mysql(sqlengine, final_df: pd.DataFrame,
                                  table_name: str, accidentID_FK_tableref: str,
                                  StnID_FK_tableref: str) -> None:
    try:
        with sqlengine.connect() as conn:
            final_df.to_sql(f"{table_name}", conn, if_exists="replace",
                            chunksize=1024*1024,
                            index=False,
                            dtype={"accident_id": types.VARCHAR(100),
                                   "Station_ID": types.VARCHAR(10),
                                   "distances": types.DOUBLE,
                                   })

            conn.execute(text(f"""ALTER TABLE {table_name}
                                        ADD CONSTRAINT FK_SNA_ActA2ID FOREIGN KEY (accident_id) 
                                            REFERENCES {accidentID_FK_tableref} (accident_id),
                                        ADD CONSTRAINT FK_SNA_StnID_forA2 FOREIGN KEY (Station_ID) 
                                            REFERENCES {StnID_FK_tableref} (Station_ID);
                            """))
            conn.execute(text(f"""ALTER TABLE {table_name}
                                        ADD COLUMN Sequential_order INT AUTO_INCREMENT PRIMARY KEY FIRST;"""))

            conn.execute(
                text(f"""ALTER TABLE {table_name} ADD COLUMN Created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP; """))

            conn.execute(
                text(f"""ALTER TABLE {table_name} ADD COLUMN Created_by VARCHAR(50) NOT NULL; """))

            conn.execute(
                text(f"""ALTER TABLE {table_name} ADD COLUMN Updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP; """))

            conn.execute(
                text(f"""ALTER TABLE {table_name} ADD COLUMN Updated_by VARCHAR(50) NOT NULL; """))

            conn.execute(
                text(f"UPDATE {table_name} SET Created_by = (:user), Updated_by = (:user);"), {'user': "lucky460721@gmail.com"})

    except RuntimeError as re:
        print(f"錯誤!{re}")
    except Exception as e:
        print(f"發生非預期的錯誤：{e}")
    else:
        print(f"資料表{table_name}建立並寫入成功！")
        return None


# 做法二：先把df_to_append暫放到另外的臨時表後再join、update，會比在python直接操作update敘述來得快。
# with engine.connect() as conn:
#     df_A1_to_append.to_sql("tmp_station_near_accidents", conn, if_exists="replace",
#                            chunksize=1024*1024,
#                            index=False)

#     conn.execute(text("""UPDATE Accident_A1
#                             JOIN tmp_station_near_accidents AS Tmp
#                                 ON Accident_A1.accident_id = Tmp.accident_id
#                             SET Accident_A1.Nearest_station_ID = Tmp.Station_ID;"""))
#     conn.execute(text("""UPDATE Accident_A1
#                             SET Updated_by = (:user)"""), {"user": "lucky460721@gmail.com;"})

#     conn.execute(text("""UPDATE Accident_A1
#                             SET Updated_on = NOW();"""))

#     conn.execute(text("""DROP TABLE tmp_station_near_accidents;"""))

#     print("更新完成！")
if __name__ == "__main__":
    # dataframe_first_load_to_mysql(
    #     engine, df_A1_to_append, "Station_near_accidentsA1", "Accident_A1_113y", "Obs_Stations")
    dataframe_first_load_to_mysql(
        engine, df_A2_to_append, "Station_near_accidentsA2", "Accident_A2_113y", "Obs_Stations")
