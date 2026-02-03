from e_crawerling_func_weather_data import service, options, crawler_CODis_to_dowload_data, save_path
from selenium import webdriver  # 引入瀏覽器驅動程式主控制器webdriver
from selenium.webdriver.support.ui import WebDriverWait  # 等待元素出現/可點擊
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from pathlib import Path
import shutil
from datetime import datetime
# 預期步驟 - 針對歷史資料:
# 1. 從MySQL server讀取accident_nearby_obs_stn，每個列都代表事件ID對應的發生日期、時間(小時)與最近觀測站record_id
# 3. 利用事件日期、時間(小時)與最近觀測站record_id，到歷史天氣觀測資料表找尋是否有存下歷史天氣資料；
# 4. 若沒有，才需要先到CODis網站進行網路爬蟲，下載該日的全時段(24 hr)天氣數據csv格式。
# 5. 在天氣數據csv檔中的小時找到該小時的天氣數據，相符者，將該列資料列寫入各站點天氣觀測資料表。(若不選小時了，全數寫入歷史資料表)


def check_missing_weather_data(engine) -> set[tuple] | None:
    """Read the table of accident records, Station_near_accidents, and weather data record
    from the assigned database in MySQL server (given by engine object).
    Return a list of lists of some accident_ID with its accident_date,
    accident_time at which the weather data is missing."""
    try:
        with engine.connect() as conn:
            # 查詢並串接 車禍事故地點附近觀測站 於 車禍當下時間(精細度到小時)觀測到的天氣資料。並留下沒有觀測資料的。
            dql_text_for_geo = text("""SELECT DATE(accident_date), station_record_id, station_id
                                            FROM accident_nearby_obs_stn ANS
                                                WHERE rank_of_distance <= 2;
                                    """)
            cursor_result_for_geo = conn.execute(dql_text_for_geo)
            # 回傳list of rows, eg.[(datetime.date(2024, 4, 13), 17, "467350")]
            relation_btw_stn_acc = cursor_result_for_geo.fetchall()

            # 查詢氣象歷史資料
            dql_text_for_weather = text("""SELECT DATE(Observation_datetime), station_record_id
                                            FROM Historical_weather_data;
                                        """)
            cursor_result_for_weather = conn.execute(dql_text_for_weather)
            stn_weather_data = cursor_result_for_weather.fetchall()

            # 盤點需要爬蟲的觀測站別＋日期有幾組，並用set去掉重複的組合。
            conditions_to_crawling = set()
            for row in relation_btw_stn_acc:
                date_and_record_id = (row[0], row[1])
                if date_and_record_id not in stn_weather_data:
                    crawling_stn_id = str(row[2])
                    crawling_year = row[0].year
                    crawling_month = row[0].month
                    crawling_monthday = row[0].day
                    conditions_to_crawling.add((crawling_stn_id, crawling_year,
                                                crawling_month, crawling_monthday))

            return conditions_to_crawling  # a set of tuples

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

    # # 連到本地的MySQL server
    load_dotenv()
    username = os.getenv("mysqllocal_username")
    password = os.getenv("mysqllocal_password")
    server = "127.0.0.1:3306"
    DB = "TESTDB"
    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{server}/{DB}",)

    # # 或是連GCP上的MySQL server
    # # username = quote_plus(os.getenv("mysql_username"))
    # # password = quote_plus(os.getenv("mysql_password"))
    # # server = "127.0.0.1:3307"
    # # db_name = "test_db"
    # # engine = create_engine(
    # #     f"mysql+pymysql://{username}:{password}@{server}/{db_name}",)

    # # Step 1: 讀取事故附近觀測站關係表與天氣觀測資料表，並且比對是否存有該觀測站當日的天氣資料。
    conditions_to_crawling = check_missing_weather_data(engine)

    # Step 2: 遍歷待爬取天氣觀測資料的事故案件，呼叫e_crawerling_func_weather_data.py寫好的爬蟲程式。
    if conditions_to_crawling:
        driver = webdriver.Chrome(service=service, options=options)
        driver.maximize_window()
        wait = WebDriverWait(driver, 10)

        crawling = crawler_CODis_to_dowload_data
        count = 0
        error_log = []
        for condition in conditions_to_crawling:
            stn_id, yr, m, md = condition[0], condition[1], condition[2], condition[3]
            file_name_pattern = f"{stn_id}-{yr}-{m:02}-{md:02}*.csv"

            if list(save_path.rglob(file_name_pattern)):
                count += 1
                print(f"同名檔案已經存在，無需下載。目前累計找到{count}筆同名檔案")
            else:
                result = crawling(driver, wait, stn_id, yr, m, md, 1)
                if result:
                    error_log.append(result)
    print(error_log)
    print(f"任務結束，本次總共下載了{len(conditions_to_crawling) - count}個檔案。")

    # Step 3: 整理資料夾，依照日期分區dt=......
    curr_dir = Path().resolve()
    source_dir = curr_dir/"supplementary_weather_csv_from_CODiS"
    classify_and_move_csvfile_path(source_dir)
