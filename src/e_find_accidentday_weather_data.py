from e_crawerling_func_weather_data import service, options, crawler_CODis_to_dowload_data
from selenium import webdriver  # 引入瀏覽器驅動程式主控制器webdriver
from selenium.webdriver.support.ui import WebDriverWait  # 等待元素出現/可點擊
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text


# 預期步驟:
# 1. 讀取accident_A1主表，遍歷每個事件ID，找到每個事件ID對應的發生日期、時間(小時)與附近最近的觀測站ID。
# 2. 利用觀測站ID、日期、時間(小時)，組合出索引值，然後到資料庫中的天氣觀測資料表找尋是否有存下天氣資料；
# 3. 若沒有，才需要先到CODis網站進行網路爬蟲，下載該日的全時段(24 hr)天氣數據csv格式。
# 4. 在天氣數據csv檔中的小時找到該小時的天氣數據，相符者，將該列資料列寫入各站點天氣觀測資料表。

# Step 1: 建立Weather_data資料表
load_dotenv()
username = os.getenv("mysqllocal_username")
password = os.getenv("mysqllocal_password")
server = "127.0.0.1:3306"
DB = "TESTDB"
engine = create_engine(f"mysql+pymysql://{username}:{password}@{server}/{DB}",)


def create_histo_weahter_data_schema(conn):
    text_query = text("""CREATE TABLE weather_historical_data(
                            `Station_ID` VARCHAR(10) COMMENT '觀測站ID',
                            `Observation_date` DATE NOT NULL COMMENT '觀測日期',
                            `Observation_time` TIME NOT NULL COMMENT '觀測時間(%H:%M:%S)',
                            `Station_air_pressure` DECIMAL(6,1) COMMENT '測站氣壓',
                            `Sea_pressure` DECIMAL(6,1) COMMENT '海平面氣壓',
                            `Temperature` DECIMAL(6,1) COMMENT '氣溫',
                            `Tempe_dew_point` DECIMAL(6,1) COMMENT '露點溫度',
                            `Relative_humidity` DECIMAL(6,1) COMMENT '相對濕度',
                            `Wind_speed` DECIMAL(6,1) COMMENT '風速',
                            `Wind_direction` DECIMAL(6,1) COMMENT '風向',
                            `Wind_speed_gust` DECIMAL(6,1) COMMENT '瞬間最大風速',
                            `Wind_direction_gust` DECIMAL(6,1) COMMENT '瞬間最大風向',
                            `Precipitation` DECIMAL(6, 1) COMMENT '降水量',
                            `Precipitation_hour` DECIMAL(6, 1) COMMENT '降水時數',
                            `Sun_shine_hour` DECIMAL(6, 1) COMMENT '日照時數',
                            `Global_radiation` DECIMAL(6, 1) COMMENT '全天空日射量',
                            `Visibility_mean_auto` DECIMAL(6, 1) COMMENT '能見度(自動)',
                            `UVI` DECIMAL(6, 1) COMMENT '紫外線指數',
                            `Cloud_amount_by_satellites` DECIMAL(6, 1) COMMENT '總雲量_衛星(0~10)',
                            `Soil_temp_at_0_cm` DECIMAL(6, 1) COMMENT '地溫0cm',
                            `Soil_temp_at_5_cm` DECIMAL(6, 1) COMMENT '地溫5cm',
                            `Soil_temp_at_10_cm` DECIMAL(6, 1) COMMENT '地溫10cm',
                            `Soil_temp_at_20_cm` DECIMAL(6, 1) COMMENT '地溫20cm',
                            `Soil_temp_at_30_cm` DECIMAL(6, 1) COMMENT '地溫30cm',
                            `Soil_temp_at_50_cm` DECIMAL(6, 1) COMMENT '地溫50cm',
                            `Soil_temp_at_100_cm` DECIMAL(6, 1) COMMENT '地溫100cm',
                            PRIMARY KEY (`Station_ID`, `Observation_date`, `Observation_time`),
                            CONSTRAINT FK_HWD_StationID FOREIGN KEY (`Station_ID`)
                                        REFERENCES Obs_Stations(`Station_ID`))
                            CHARSET=utf8mb4 COMMENT '各觀測站天氣觀測結果';""")

    conn.execute(text_query)
    print("weather_historical_data資料表建立成功！")
    return None

# 創建資料表
# with engine.connect() as conn:
#     create_histo_weahter_data_schema(conn)

# Step 2: 讀取車禍事件表主表與天氣觀測資料表，並且比對。


def accident_missing_weather_data():
    try:
        with engine.connect() as conn:

            # 查詢並串接 車禍事故地點附近觀測站 於 車禍當下時間(精細度到小時)觀測到的天氣資料。並留下沒有觀測資料的。
            cursor_result1 = conn.execute(text("""SELECT A.accident_ID, A.accident_date, A.accident_time,
                                                 A.Nearest_station_ID, WHD.*
                                                    FROM Accident_A1 A
                                                        LEFT JOIN weather_historical_data WHD
                                                        ON A.Nearest_station_ID = WHD.Station_ID
                                                        AND A.accident_date = WHD.Observation_date
                                                        AND SEC_TO_TIME(ROUND(TIME_TO_SEC(A.Accident_time) / 3600) * 3600)
                                                        = SEC_TO_TIME(ROUND(TIME_TO_SEC(WHD.Observation_time) / 3600) * 3600)
                                                            WHERE WHD.Observation_date IS NULL;
                                            """))

            result1 = cursor_result1.fetchall()  # 回傳list

            # 查詢觀測站的資料啟用日期
            cursor_result2 = conn.execute(text("""SELECT obs.Station_ID, Date_of_Opening
                                                FROM Obs_Stations obs
                                               """))
            result2 = cursor_result2.fetchall()
            obs_station_dict = {}
            for lst in result2:
                obs_station_dict[lst[0]] = lst[1]

            if not result1:  # 如果回傳空list，代表全部事件都有對應的天氣觀測數據。
                return print(f"全部{len(result1)}筆事件紀錄都在資料庫中有對應的天氣資料。")
            else:
                print(f"共有{len(result1)}筆事件紀錄需補足天氣資料。")
                conditions_to_crawling = []
                for accident_no_w_data in result1:
                    station_ID = accident_no_w_data[3]
                    accident_date = accident_no_w_data[1]

                    # 如果觀測站架設晚於車禍發生日期，則跳過該筆、不用進行爬蟲，否則會爬不到天氣資料。
                    if accident_date >= obs_station_dict[station_ID]:
                        year, month, monthday = accident_date.year, accident_date.month, accident_date.day
                        conditions_to_crawling.append(
                            [station_ID, year, month, monthday])
                print(
                    f"但是{len(result1)-len(conditions_to_crawling)}筆事件附近沒有觀測站可提供資料。")
                return conditions_to_crawling  # list of lists

    except Exception as e:
        print(f"Error: {e}")


conditions_to_crawling = accident_missing_weather_data()

# Step 3: 遍歷待爬取天氣觀測資料的事故案件，呼叫e_Obs_station_weather_data.py寫好的爬蟲程式。
if isinstance(conditions_to_crawling, list):
    driver = webdriver.Chrome(service=service, options=options)
    driver.maximize_window()
    wait = WebDriverWait(driver, 10)

    crawling = crawler_CODis_to_dowload_data
    for condition in conditions_to_crawling:
        target_stn_ID = condition[0]
        target_yr = condition[1]
        target_m = condition[2]
        target_md = condition[3]

        crawling(driver, wait, target_stn_ID, target_yr, target_m, target_md)
