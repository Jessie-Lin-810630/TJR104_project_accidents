from e_crawerling_func_weather_data import service, options, crawler_CODis_to_dowload_data, save_path
from selenium import webdriver  # 引入瀏覽器驅動程式主控制器webdriver
from selenium.webdriver.support.ui import WebDriverWait  # 等待元素出現/可點擊
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from pathlib import Path
import shutil

# 預期步驟:
# 1. 從MySQL server讀取accident_A1主表，遍歷每個事件ID，找到每個事件ID對應的發生日期、時間(小時)
# 2. 從MySQL server讀取Station_near_accidents，找到車禍地點附近最近的觀測站ID。
# 3. 利用觀測站ID、日期、時間(小時)，組合出索引值，然後到資料庫中的天氣觀測資料表找尋是否有存下天氣資料；
# 4. 若沒有，才需要先到CODis網站進行網路爬蟲，下載該日的全時段(24 hr)天氣數據csv格式。
# 5. 在天氣數據csv檔中的小時找到該小時的天氣數據，相符者，將該列資料列寫入各站點天氣觀測資料表。


def accident_missing_weather_data(engine) -> list[list] | None:
    """Read the table of accident records, Station_near_accidents, and weather data record
    from the assigned database in MySQL server (given by engine object).
    Return a list of lists of some accident_ID with its accident_date,
    accident_time at which the weather data is missing."""
    try:
        with engine.connect() as conn:
            # 查詢並串接 車禍事故地點附近觀測站 於 車禍當下時間(精細度到小時)觀測到的天氣資料。並留下沒有觀測資料的。
            conn.execute(text("""CREATE OR REPLACE 
                                VIEW v_A_SNA (accident_ID, accident_date, accident_time, Station_ID)
                                    AS (SELECT A.accident_ID, A.accident_date, A.accident_time, SNA.Station_ID
                                        FROM Accident_A1 A
                                            LEFT JOIN Station_near_accidents SNA
                                            ON A.accident_ID = SNA.accident_ID);
                         """))
            cursor_result1 = conn.execute(text("""SELECT v_A_SNA.accident_ID, v_A_SNA.accident_date, 
                                                        v_A_SNA.accident_time, v_A_SNA.station_ID, WHD.*
                                                    FROM v_A_SNA
                                                        LEFT JOIN weather_historical_data WHD
                                                        ON v_A_SNA.station_ID = WHD.Station_ID
                                                            AND v_A_SNA.accident_date = WHD.Observation_date
                                                            AND SEC_TO_TIME(ROUND(TIME_TO_SEC(v_A_SNA.Accident_time) / 3600) * 3600)
                                                            = SEC_TO_TIME(ROUND(TIME_TO_SEC(WHD.Observation_time) / 3600) * 3600)
                                                                WHERE WHD.Observation_date IS NULL;
                                                """))

            result1 = cursor_result1.fetchall()  # 回傳list of rows

            if not result1:  # 如果回傳空list，代表全部事件都有對應的天氣觀測數據。
                print(f"全部{len(result1)}筆事件紀錄都在資料庫中已有對應的天氣資料。")
                return None
            else:
                print(f"共有{len(result1)}筆事件紀錄需補足天氣資料。")

                # 查詢觀測站的資料啟用日期
                cursor_result2 = conn.execute(text("""SELECT obs.Station_ID, Date_of_Opening
                                                FROM Obs_Stations obs
                                               """))

                result2 = cursor_result2.fetchall()
                obs_station_dict = {}

                for lst in result2:
                    obs_station_dict[lst[0]] = lst[1]

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


def classify_and_move_csvfile_path(orign_dir):
    """""""""
    Classify and organize csv files into date-based directories by parsing filenames.
    Automatically creates date-structured subdirectories (dt=YYYY-MM-DD format) 
    and copies observation station CSV files based on embedded station ID and 
    date information in the filename pattern: "*?-YYYY-MM-DD*.csv"."""

    for csv_file in orign_dir.glob("*?-????-?*-?*.csv"):
        file_name = csv_file.name
        station_id, y, m, md = file_name.replace(
            ".csv", "").split("-")  # C0K400  2024 04  2 22.22
        md = md.split()[0]
        obs_date_dir = orign_dir/f"dt={y}-{m:02}-{md:02}"
        obs_date_dir.mkdir(parents=True, exist_ok=True)
        new_file_name = obs_date_dir/file_name
        shutil.copy(csv_file, new_file_name)
    return None


if __name__ == "__main__":
    load_dotenv()
    # 連到本地的MySQL server
    username = os.getenv("mysqllocal_username")
    password = os.getenv("mysqllocal_password")
    server = "127.0.0.1:3306"
    DB = "TESTDB"
    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{server}/{DB}",)

    # 或是連GCP上的MySQL server
    # username = quote_plus(os.getenv("mysql_username"))
    # password = quote_plus(os.getenv("mysql_password"))
    # server = "127.0.0.1:3307"
    # db_name = "test_db"
    # engine = create_engine(
    #     f"mysql+pymysql://{username}:{password}@{server}/{db_name}",)

    # Step 1: 讀取車禍事件表主表、事故附近觀測站關係表與天氣觀測資料表，並且比對是否存有該觀測站當日的天氣資料。
    conditions_to_crawling = accident_missing_weather_data(engine)

    # Step 2: 遍歷待爬取天氣觀測資料的事故案件，呼叫e_crawerling_func_weather_data.py寫好的爬蟲程式。
    if isinstance(conditions_to_crawling, list):
        driver = webdriver.Chrome(service=service, options=options)
        driver.maximize_window()
        wait = WebDriverWait(driver, 10)

        crawling = crawler_CODis_to_dowload_data
        count = 0
        for condition in conditions_to_crawling:
            target_stn_ID, target_yr, target_m, target_md = condition[
                0], condition[1], condition[2], condition[3]
            file_name_pattern = f"{target_stn_ID}-{target_yr}-{target_m:02}-{target_md:02}.csv"

            if list(save_path.glob(file_name_pattern)):
                count += 1
                print(f"同名檔案已經存在。{count}")
            else:
                crawling(driver, wait, target_stn_ID,
                         target_yr, target_m, target_md)

    # # Step 3: 整理資料夾，依照日期分區dt=......
    # curr_dir = Path().resolve()
    # source_dir = curr_dir/"supplementary_weather_csv_from_CODiS"
    # classify_and_move_csvfile_path(source_dir)
