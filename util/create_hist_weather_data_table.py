from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv


# 建立Weather_data資料表
load_dotenv("./src/.env")
username = os.getenv("mysqllocal_username")
password = os.getenv("mysqllocal_password")
server = "127.0.0.1:3306"
DB = "TESTDB"
engine = create_engine(f"mysql+pymysql://{username}:{password}@{server}/{DB}",)


def create_hist_weahter_data_table(engine, table_name: str) -> None:
    """Create the table 'historical_weather_data' into the assigned database
    in MySQL server (given by engine object) if the table is not exist.
    Raise Error when the table is already exist. '"""
    with engine.connect() as conn:
        ddl_query = text(f"""CREATE TABLE {table_name}(
                                `Station_record_id` INT AUTO_INCREMENT COMMENT '測站紀錄流水編號',
                                `Station_id` VARCHAR(10) COMMENT '觀測站別',
                                `Observation_datetime` DATETIME COMMENT '氣象觀測日期(yyyy/mm/dd HH:MM:00)',
                                `Station_air_pressure` DECIMAL(6,1) COMMENT '測站氣壓(hPa)',
                                `Sea_pressure` DECIMAL(6,1) COMMENT '海平面氣壓(hPa)',
                                `Temperature` DECIMAL(6,1) COMMENT '氣溫(℃)',
                                `Temp_dew_point` DECIMAL(6,1) COMMENT '露點溫度(℃)',
                                `Relative_humidity` DECIMAL(6,1) COMMENT '相對溼度(%)',
                                `Wind_speed` DECIMAL(6,1) COMMENT '風速(m/s)',
                                `Wind_direction` DECIMAL(6,1) COMMENT '風向(360degree)',
                                `Wind_speed_gust` DECIMAL(6,1) COMMENT '最大瞬間風(m/s)',
                                `Wind_distant_gust` DECIMAL(6,1) COMMENT '最大瞬間風風向(360degree)',
                                `Precipitation` DECIMAL(6,1) COMMENT '降水量(mm)',
                                `Precipitation_hour` DECIMAL(6,1) COMMENT '降水時數(h)',
                                `Sun_shine_hour` DECIMAL(6,1) COMMENT '日照時數(h)',
                                `Global_radiation` DECIMAL(6,1) COMMENT '全天空日射量(MJ/㎡)',
                                `Visibility` DECIMAL(6,1) COMMENT '能見度(km)',
                                `Visibility_mean_auto` DECIMAL(6,1) COMMENT '能見度_自動(km)',
                                `UVI` DECIMAL(6,1) COMMENT '紫外線指數',
                                `Cloud_amount` DECIMAL(6,1) COMMENT '總雲量(0~10)',
                                `Cloud_amount_by_satellites` DECIMAL(6,1) COMMENT '總雲量_衛星(0~10)',
                                `Soil_temp_at_0_cm` DECIMAL(6,1) COMMENT '地溫0cm',
                                `Soil_temp_at_5_cm` DECIMAL(6,1) COMMENT '地溫5cm',
                                `Soil_temp_at_10_cm` DECIMAL(6,1) COMMENT '地溫10cm',
                                `Soil_temp_at_20_cm` DECIMAL(6,1) COMMENT '地溫20cm',
                                `Soil_temp_at_30_cm` DECIMAL(6,1) COMMENT '地溫30cm',
                                `Soil_temp_at_50_cm` DECIMAL(6,1) COMMENT '地溫50cm',
                                `Soil_temp_at_100_cm` DECIMAL(6,1) COMMENT '地溫100cm',
                                `Created_on` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '建立日期',
                                `Created_by` VARCHAR(50) COMMENT '建立者',
                                `Updated_on` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '最近修改日期',
                                `Updated_by` VARCHAR(50) COMMENT '修改者',
                                PRIMARY KEY (`Station_ID`, `Observation_datetime`),
                                CONSTRAINT FK_HWD_StnID FOREIGN KEY (`Station_record_id`)
                                            REFERENCES Obs_stations(`Station_record_id`))
                                CHARSET=utf8mb4 COMMENT '各觀測站天氣觀測結果';""")

        conn.execute(ddl_query)
        print(f"{table_name}資料表建立成功！")
        return None


# 創建資料表
create_hist_weahter_data_table(engine, "Historical_weather_data")
