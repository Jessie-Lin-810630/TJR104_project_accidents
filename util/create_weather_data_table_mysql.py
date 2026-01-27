from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv


# 建立Weather_data資料表
load_dotenv()
username = os.getenv("mysqllocal_username")
password = os.getenv("mysqllocal_password")
server = "127.0.0.1:3306"
DB = "TESTDB"
engine = create_engine(f"mysql+pymysql://{username}:{password}@{server}/{DB}",)


def create_histo_weahter_data_schema(engine) -> None:
    """Create the table 'weather_historical_data' into the assigned database
    in MySQL server (given by engine object) if the table is not exist.
    Raise Error when the table is already exist. '"""
    with engine.connect() as conn:
        text_query = text("""CREATE TABLE weather_historical_data(
                                `Station_ID` VARCHAR(10) COMMENT '觀測站別',
                                `Observation_date` DATE DEFAULT('1990-01-01') COMMENT '觀測日期',
                                `Observation_time` TIME DEFAULT('00:00:00') COMMENT '觀測時間(hour)',
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
                                PRIMARY KEY (`Station_ID`, `Observation_date`, `Observation_time`),
                                CONSTRAINT UK_HWD_StationID (`Station_ID`),
                                CONSTRAINT FK_HWD_StationID FOREIGN KEY (`Station_ID`)
                                            REFERENCES Obs_Stations(`Station_ID`))
                                CHARSET=utf8mb4 COMMENT '各觀測站天氣觀測結果';""")

        conn.execute(text_query)
        print("weather_historical_data資料表建立成功！")
        return None

# 創建資料表
# create_histo_weahter_data_schema(engine)
