import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, types, text  # 用以引入寫進資料庫時
from urllib.parse import quote_plus
from dotenv import load_dotenv
import os


# 定義存入MySQL時的資料型別
sql_col_map = {
    "accident_id": types.VARCHAR(100),
    "accident_year": types.Integer,
    "accident_month": types.Integer,
    "accident_date": types.DATE,
    "accident_time": types.TIME,  # TIMESTAMP?
    "accident_category": types.VARCHAR(2),
    "police_department": types.VARCHAR(20),
    "accident_location": types.VARCHAR(100),
    "weather_condition": types.VARCHAR(1),
    "light_condition": types.VARCHAR(20),
    "road_type_primary_party": types.VARCHAR(10),
    "speed_limit_primary_party": types.SMALLINT,
    "road_form_major": types.VARCHAR(10),
    "road_form_minor": types.VARCHAR(10),
    "accident_position_major": types.VARCHAR(20),
    "accident_position_minor": types.VARCHAR(20),
    "road_surface_pavement": types.VARCHAR(10),
    "road_surface_condition": types.VARCHAR(10),
    "road_surface_defect": types.VARCHAR(10),
    "road_obstacle": types.VARCHAR(10),
    "sight_distance_quality": types.VARCHAR(10),
    "sight_distance": types.VARCHAR(10),
    "traffic_signal_type": types.VARCHAR(200),
    "traffic_signal_action": types.VARCHAR(200),
    "lane_divider_direction_major": types.VARCHAR(20),
    "lane_divider_direction_minor": types.VARCHAR(20),
    "lane_divider_main_general": types.VARCHAR(20),
    "lane_divider_fast_slow": types.VARCHAR(20),
    "lane_edge_marking": types.VARCHAR(20),
    "accident_type_major": types.VARCHAR(20),
    "accident_type_minor": types.VARCHAR(20),
    "cause_analysis_major_primary": types.VARCHAR(200),
    "cause_analysis_minor_primary": types.VARCHAR(200),
    "casualties_count": types.VARCHAR(20),
    "party_sequence": types.SMALLINT,
    "vehicle_type_major": types.VARCHAR(20),
    "vehicle_type_minor": types.VARCHAR(20),
    "gender": types.VARCHAR(20),
    "age": types.SMALLINT,
    "protective_equipment": types.VARCHAR(200),
    "mobile_device_usage": types.VARCHAR(20),
    "party_action_major": types.VARCHAR(20),
    "party_action_minor": types.VARCHAR(20),
    "impact_point_major_initial": types.VARCHAR(20),
    "impact_point_minor_initial": types.VARCHAR(20),
    "impact_point_major_other": types.VARCHAR(20),
    "impact_point_minor_other": types.VARCHAR(20),
    "cause_analysis_major_individual": types.VARCHAR(20),
    "cause_analysis_minor_individual": types.VARCHAR(100),
    "hit_and_run": types.VARCHAR(2),
    "longitude (WGS84)": types.DECIMAL(10, 6),
    "latitude (WGS84)": types.DECIMAL(10, 6),
    "Nearest_station_ID": types.VARCHAR(30),
}


def dataframe_first_load_to_mysql(sqlengine):
    """Use to create TABLE with transformed data when first loading them
    onto a MySQL database. This function include the create a schema and 
    add neccessary primary key"""
    try:
        with sqlengine.connect() as conn:
            final_df_A1.to_sql("Accident_A1", conn, if_exists="replace",
                               chunksize=1024*1024, dtype=sql_col_map,
                               index=False)  # 已經有accident_id了，不用多的index
            conn.execute(
                text("""ALTER TABLE Accident_A1 ADD PRIMARY KEY (accident_id);"""))
            conn.execute(
                text("""ALTER TABLE Accident_A1 CHANGE `longitude (WGS84)` `longitude (WGS84)` DECIMAL(10, 6) NOT NULL,
                                                CHANGE `latitude (WGS84)` `latitude (WGS84)` DECIMAL(10, 6) NOT NULL;"""))

            conn.execute(
                text("""ALTER TABLE Accident_A1 COMMENT "113年(:accident_type)車禍事件記錄表" """), {"accident_type": "A1"})

            conn.execute(
                text("""ALTER TABLE Accident_A1 ADD COLUMN Created_on DATETIME DEFAULT NOW(); """))

            conn.execute(
                text("""ALTER TABLE Accident_A1 ADD COLUMN Created_by VARCHAR(50) NOT NULL; """))

            conn.execute(
                text("""ALTER TABLE Accident_A1 ADD COLUMN Updated_on DATETIME DEFAULT NOW(); """))

            conn.execute(
                text("""ALTER TABLE Accident_A1 ADD COLUMN Updated_by VARCHAR(50) NOT NULL; """))

            conn.execute(
                text("UPDATE Accident_A1 SET Created_by = (:user), Updated_by = (:user);"), {'user': "lucky460721@gmail.com"})

            conn.commit()  # 手動提交，確保變更生效
    except RuntimeError as re:
        print(f"錯誤!{re}")
    except Exception as e:
        print(f"發生非預期的錯誤：{e}")

    else:
        print("資料表Accident_A1建立並寫入成功！")


if __name__ == "__main__":
    # 方法一(耗時30秒)：直接呼叫t_AccidentsA1A2_y113_at_fault_driver.py清理過的dataframe of A1/A2
    # from t_AccidentsA1A2_y113_at_fault_driver import df_A1, df_A2
    # final_df_A1 = df_A1.copy()
    # final_df_A2 = df_A2.copy()

    # 方法二：讀取t_AccidentsA1A2_y113_at_fault_driver.py產出的A1_summary_113(at-fault-driver-only).csv
    curr_dir = Path().resolve()
    source_dir_A1 = curr_dir / \
        "traffic_accient_transformed-csv/A1_summary_113(at-fault-driver-only).csv"
    source_dir_A2 = curr_dir / \
        "traffic_accient_transformed-csv/A2_summary_113(at-fault-driver-only).csv"
    final_df_A1 = pd.read_csv(source_dir_A1, engine='python', encoding="utf-8")
    final_df_A2 = pd.read_csv(source_dir_A2, engine='python', encoding="utf-8")

    # (儲存方法一)建立與本地端MySQL server的連線
    load_dotenv()
    username = quote_plus(os.getenv("mysqllocal_username"))
    password = quote_plus(os.getenv("mysqllocal_password"))
    server = "127.0.0.1:3306"
    db_name = "TESTDB"
    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{server}/{db_name}",)
    dataframe_first_load_to_mysql(engine)

    # (儲存方法二)建立與GCP VM上的MySQL server的連線
    # load_dotenv()
    # username = quote_plus(os.getenv("mysql_username"))
    # password = quote_plus(os.getenv("mysql_password"))
    # server = "127.0.0.1:3307"
    # db_name = "test_db"
    # engine = create_engine(
    #     f"mysql+pymysql://{username}:{password}@{server}/{db_name}",)
    # dataframe_first_load_to_mysql(engine)
