from e_crawerling_func_weather_data import service, options, save_path, crawler_CODis_to_dowload_data
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


def get_table_from_sqlserver(database: str, dql_text) -> list[list]:
    # 連到本地的MySQL server
    load_dotenv()
    username = quote_plus(os.getenv("mysqllocal_username"))
    password = quote_plus(os.getenv("mysqllocal_password"))
    server = "127.0.0.1:3306"
    DB = database
    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{server}/{DB}",)

    # 或是連GCP上的MySQL server
    # load_dotenv()
    # username = quote_plus(os.getenv("mysql_username"))
    # password = quote_plus(os.getenv("mysql_password"))
    # server = "127.0.0.1:3307"
    # db_name = "test_weather"
    # engine = create_engine(
    #     f"mysql+pymysql://{username}:{password}@{server}/{db_name}",)

    try:
        with engine.connect() as conn:
            cursor_result = conn.execute(dql_text)
            result = cursor_result.fetchall()
            return result  # 回傳list of rows
    except Exception as e:
        print(f"讀取Error: {e}")


def check_missing_weather_data(stn_weather_data: list[list],
                               relation_btw_stn_acc: list[list]) -> set[tuple] | None:
    """Read the table of accident records, Station_near_accidents, and weather data record
    from the assigned database in MySQL server (given by engine object).
    Return a list of lists of some accident_ID with its accident_date,
    accident_time at which the weather data is missing."""

    # 盤點需要爬蟲的觀測站別＋日期有幾組，並用set去掉重複的爬蟲需求。
    conditions_to_crawling = set()
    for row in relation_btw_stn_acc:  # (date(2024,4,13),17,"467350")
        date_and_record_id = (row[0], row[1])  # (date(2024,4,13),17)
        if date_and_record_id != stn_weather_data:  # (date(2024,4,13),17)
            crawling_stn_id = str(row[2])
            crawling_year = row[0].year
            crawling_month = row[0].month
            crawling_monthday = row[0].day
            conditions_to_crawling.add((crawling_stn_id, crawling_year,
                                        crawling_month, crawling_monthday))

    return conditions_to_crawling


def classify_and_move_csvfile_path(orign_dir: str | Path, dest_dir: str | Path) -> None:
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
        dest_sub_dir = dest_dir/f"dt={y}-{m:02}-{md:02}"
        dest_sub_dir.mkdir(parents=True, exist_ok=True)
        new_file_name = dest_sub_dir/file_name
        shutil.copy(csv_file, new_file_name)
    shutil.rmtree(str(orign_dir))

    return None


def e_find_hist_accidentday_weather_data(conditions_to_crawling: list,
                                         service, options) -> None:
    if conditions_to_crawling:
        driver = webdriver.Chrome(service=service, options=options)
        driver.maximize_window()
        wait = WebDriverWait(driver, 10)

        # 呼叫e_crawerling_func_weather_data.py的爬蟲函式: 執行爬蟲任務
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
                if result:  # 如果下載成功會回傳None、反之回傳error string
                    error_log.append(result)
        print(
            f"任務結束，本次總共下載了{len(conditions_to_crawling) - len(error_log)}個檔案。")
        print(error_log)
    else:
        print("無下載任何氣象觀測檔。")
    return None


if __name__ == "__main__":
    # Step 1:查詢 車禍事故日 & 附近觀測站。回傳list of rows, eg.[(datetime.date(2024, 4, 13), 17, "467350")]
    relation_btw_stn_acc = get_table_from_sqlserver("TESTDB",
                                                    text("""SELECT DATE(accident_date), station_record_id, 
                                                                   station_id
                                                                FROM accident_nearby_obs_stn ANS;
                                                         """))

    # Step 2: 查詢氣象歷史資料
    stn_weather_data = get_table_from_sqlserver("TESTDB",
                                                text("""SELECT DATE(Observation_datetime), station_record_id
                                                            FROM Historical_weather_data;
                                                     """))

    # Step 3: 比對step1&step2兩張表，比對是否存有事故附近觀測站當日的天氣資料。
    conditions_to_crawling = check_missing_weather_data(
        stn_weather_data, relation_btw_stn_acc)

    # Step 4: 遍歷待爬取天氣觀測資料的事故案件，呼叫e_crawerling_func_weather_data.py寫好的爬蟲程式。
    e_find_hist_accidentday_weather_data(
        conditions_to_crawling, service, options)

    # Step 5: 整理資料夾，依照日期分區dt=......
    curr_dir = Path().resolve()
    source_dir = curr_dir/"raw_csv"/"daily_Weather_data"
    destination_dir = curr_dir/"processed_csv"/"partitioned_weather_csv"
    classify_and_move_csvfile_path(source_dir, destination_dir)
