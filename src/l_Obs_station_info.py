from t_Obs_station_info import df_weather_obs_stations
import os
# terminal執行poetry add python-dotenv或pip install python-dotenv
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, types, text
from urllib.parse import quote_plus
import pymysql


def dataframe_first_load_to_mysql(sqlengine):
    """Use to create TABLE with transformed data when first loading them
    onto a MySQL database. This function include the create a schema and 
    add neccessary primary key"""
    try:
        with sqlengine.connect() as conn:
            # 將資料存入MySQL
            df.to_sql("Obs_Stations", con=conn, if_exists="replace",
                      dtype={"Station_ID": types.VARCHAR(10),
                             "Station_name": types.VARCHAR(50),
                             "Sea_level": types.DECIMAL(7, 2),
                             "Longitude (WGS84)": types.DECIMAL(10, 6),
                             "Latitude (WGS84)": types.DECIMAL(10, 6),
                             "Date_of_Opening": types.DATE},
                      index=False)  # 不用匯入index，因為站號已經是有識別用了

            # 補上UK、FK，
            # 這是sqlalchemy的痛點，無法在to_sql資料表當下一併設定，或是要在to_sql前做MetaData
            conn.execute(
                text("""ALTER TABLE Obs_Stations ADD PRIMARY KEY (Station_ID);"""))

            conn.execute(
                text("""ALTER TABLE Obs_Stations CHANGE `Longitude (WGS84)` `Longitude (WGS84)` DECIMAL(10, 6) NOT NULL,
                                                CHANGE `Latitude (WGS84)` `Latitude (WGS84)` DECIMAL(10, 6) NOT NULL;"""))

            conn.execute(
                text("""ALTER TABLE Obs_Stations COMMENT "觀測站基本地理資訊" """))

            conn.execute(
                text("""ALTER TABLE Obs_Stations ADD COLUMN Created_on DATETIME DEFAULT (NOW(),"UTC","Asia/Taipei")); """))

            conn.execute(
                text("""ALTER TABLE Obs_Stations ADD COLUMN Created_by VARCHAR(50) NOT NULL; """))

            conn.execute(
                text("""ALTER TABLE Obs_Stations ADD COLUMN Updated_on DATETIME DEFAULT (NOW(),"UTC","Asia/Taipei")); """))

            conn.execute(
                text("""ALTER TABLE Obs_Stations ADD COLUMN Updated_by VARCHAR(50) NOT NULL; """))

            conn.execute(
                text("UPDATE Obs_Stations SET Created_by = (:user), Updated_by = (:user);"), {'user': "lucky460721@gmail.com"})

            conn.commit()  # 手動提交，確保變更生效
    except RuntimeError as re:
        print(f"錯誤!{re}")
    except Exception as e:
        print(f"發生非預期的錯誤：{e}")

    else:
        print("資料表Obs_Stations建立並寫入成功！")


if __name__ == "__main__":
    curr_dir = Path(__file__).resolve().parent
    load_dotenv()

    # Step 1: 呼叫src/t_Obs_station_info.py的weather_obs_stations (a DataFrame)
    df = df_weather_obs_stations
    # 如果不直接呼叫，而是想讀取t-step存下的中間層數據，則改執行下一行：
    # df = pd.read_csv(curr_dir.parent /
    #                  "supplementary_weather_csv_from_CODiS/station_info_table_Eng.csv",
    #                  encoding="utf-8-sig")

    # (儲存方法一)建立與本地端MySQL server的連線
    username = quote_plus(os.getenv("mysqllocal_username"))
    password = quote_plus(os.getenv("mysqllocal_password"))
    server = "127.0.0.1:3306"
    db_name = "TESTDB"

    # (儲存方法二)建立與GCP VM上的MySQL server的連線
    # username = quote_plus(os.getenv("mysql_username"))
    # password = quote_plus(os.getenv("mysql_password"))
    # server = "127.0.0.1:3307"
    # db_name = "test_db"

    # Step 3: 建立engine物件
    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{server}/{db_name}",
    )
    # Step 4: 建立connection物件並連線進入MySQL Server後，建立資料表
    dataframe_first_load_to_mysql(engine)
