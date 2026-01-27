from sqlalchemy import create_engine, text, types
import os
from dotenv import load_dotenv
from t_find_nearest_Obs_station import df_A1_to_append
from urllib.parse import quote_plus

load_dotenv()
# username = os.getenv("mysqllocal_username")
# password = os.getenv("mysqllocal_password")
# server = "127.0.0.1:3306"
# DB = "TESTDB"

# (儲存方法二)建立與GCP VM上的MySQL server的連線
username = quote_plus(os.getenv("mysql_username"))
password = quote_plus(os.getenv("mysql_password"))
server = "127.0.0.1:3307"
DB = "test_db"


engine = create_engine(f"mysql+pymysql://{username}:{password}@{server}/{DB}",)


# 做法一（較推薦）：直接將df_to_append存成一個資料表，表述事故與觀測站兩個實體的關係。
with engine.connect() as conn:
    df_A1_to_append.to_sql("Station_near_accidents", conn, if_exists="replace",
                           chunksize=1024*1024,
                           index=False,
                           dtype={"accident_id": types.VARCHAR(100),
                                  "Station_ID": types.VARCHAR(10),
                                  "distances": types.DOUBLE,
                                  })

    conn.execute(text("""ALTER TABLE Station_near_accidents 
                                ADD CONSTRAINT FK_SNC_ActID FOREIGN KEY (accident_id) 
                                    REFERENCES Accident_A1 (accident_id),
                                ADD CONSTRAINT FK_SNC_StnID FOREIGN KEY (Station_ID) 
                                    REFERENCES Obs_Stations (Station_ID);
                      """))
    conn.execute(text("""ALTER TABLE Station_near_accidents
                                ADD COLUMN Sequential_order INT AUTO_INCREMENT PRIMARY KEY FIRST;"""))

    conn.execute(
        text("""ALTER TABLE Station_near_accidents ADD COLUMN Created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP; """))

    conn.execute(
        text("""ALTER TABLE Station_near_accidents ADD COLUMN Created_by VARCHAR(50) NOT NULL; """))

    conn.execute(
        text("""ALTER TABLE Station_near_accidents ADD COLUMN Updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP; """))

    conn.execute(
        text("""ALTER TABLE Station_near_accidents ADD COLUMN Updated_by VARCHAR(50) NOT NULL; """))

    conn.execute(
        text("UPDATE Station_near_accidents SET Created_by = (:user), Updated_by = (:user);"), {'user': "lucky460721@gmail.com"})

    conn.commit()

    print("創建完成！")


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
#     conn.commit()

#     print("更新完成！")
