from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
from pathlib import Path


def create_obs_stations_table(engine, table_name: str) -> None:
    """Use to create TABLE onto a MySQL database. This function include the create a table and
    add neccessary primary key"""
    try:
        with engine.connect() as conn:
            ddl_text = text(f"""CREATE TABLE {table_name}(
                                `Station_record_id` INT AUTO_INCREMENT COMMENT '測站紀錄流水編號',
                                `Station_id` VARCHAR(10) COMMENT '觀測站別',
                                `Station_name` VARCHAR(50) COMMENT '觀測站名',
                                `Station_sea_level` DECIMAL(7, 2) COMMENT '測站海拔高度',
                                `Station_longitude_WGS84` DECIMAL(10, 6) COMMENT '測站經度',
                                `Station_latitude_WGS84` DECIMAL(10, 6) COMMENT '測站緯度',
                                `Station_working_state` VARCHAR(50) COMMENT '測站運作狀態',
                                `State_valid_from` DATE COMMENT '運作狀態起始日',
                                `State_valid_to` DATE DEFAULT(DATE("9999-12-31")) COMMENT '運作狀態結束日',
                                `Remark` TEXT COMMENT '備註',
                                `Created_on` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建立日期',
                                `Created_by` VARCHAR(50) NOT NULL COMMENT '建立者',
                                `Updated_on` TIMESTAMP COMMENT '最近修改日期',
                                `Updated_by` VARCHAR(50) COMMENT '修改者',
                                PRIMARY KEY (`Station_record_id`),
                                UNIQUE KEY UK_obs_idvfvt(`Station_id`, `State_valid_from`, `Station_working_state`),
                                INDEX inx_ObsStn_StnId (`Station_id`))
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
    create_obs_stations_table(engine, "Obs_stations")
