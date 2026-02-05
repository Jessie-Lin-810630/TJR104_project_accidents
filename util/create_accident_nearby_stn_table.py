from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
from pathlib import Path


def create_accident_nearby_stn_table(engine, table_name: str) -> None:
    """Use to create TABLE onto a MySQL database. This function include the create a table and 
    add neccessary primary key"""
    try:
        with engine.connect() as conn:
            ddl_text = text(f"""CREATE TABLE {table_name}(
                                `accident_id` VARCHAR(100),
                                `accident_datetime` DATETIME COMMENT '車禍發生日期時間',
                                `station_record_id` INT AUTO_INCREMENT COMMENT '測站紀錄流水編號',
                                `station_id` VARCHAR(10) COMMENT '附近觀測站別',
                                `state_valid_from` DATE COMMENT '運作狀態起始日',
                                `distance` DECIMAL(10, 6) COMMENT '距離',
                                `rank_of_distance` INT COMMENT '距離排名',
                                `created_on` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建立日期',
                                `created_by` VARCHAR(50) NOT NULL COMMENT '建立者',
                                `updated_on` TIMESTAMP COMMENT '最近修改日期',
                                `updated_by` VARCHAR(50) COMMENT '修改者',
                                PRIMARY KEY (`accident_id`,`rank_of_distance`),
                                CONSTRAINT FK_ANS_StnRId FOREIGN KEY (`Station_record_id`)
                                            REFERENCES Obs_stations(`Station_record_id`))
                                CHARSET=utf8mb4 
                                COMMENT '各觀測站天氣觀測結果';""")

            conn.execute(ddl_text)
    except RuntimeError as re:
        print(f"錯誤!{re}")
    except Exception as e:
        print(f"發生非預期的錯誤：{e}")
    else:
        print(f"{table_name}資料表建立成功！")
    return None


if __name__ == "__main__":
    # curr_wd = Path().resolve()
    # load_dotenv(str(curr_wd/"src"/".env"))
    # username = quote_plus(os.getenv("mysqllocal_username"))
    # password = quote_plus(os.getenv("mysqllocal_password"))
    # server = "127.0.0.1:3306"
    # DB = "TESTDB"
    # engine = create_engine(
    #     f"mysql+pymysql://{username}:{password}@{server}/{DB}",)

    # or, Connect to GCP VM MySQL server
    curr_wd = Path().resolve()
    load_dotenv(str(curr_wd/"src"/".env"))
    username = quote_plus(os.getenv("mysql_username"))
    password = quote_plus(os.getenv("mysql_password"))
    server = "127.0.0.1:3307"
    DB = "test_weather"
    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{server}/{DB}",)
    # 創建資料表
    create_accident_nearby_stn_table(engine, "Accident_nearby_obs_stn")
