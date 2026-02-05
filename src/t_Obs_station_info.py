from e_Obs_station_info import df_raw_obs_stations
from sqlalchemy import create_engine, text
import pandas as pd
from pathlib import Path
from urllib.parse import quote_plus
from dotenv import load_dotenv
import os
from datetime import datetime


def clean_crawled_table(df_raw_obs_stn: pd.DataFrame, col_name: dict | list) -> pd.DataFrame:
    """Clean the raw dataframe obtained from the e_Obs_station_info.py, and return transformed dataframe."""
    # Step 1: import src/e_Obs_station_info.py爬下來的dataframe: df_raw_obs_stations
    df = df_raw_obs_stn.copy()
    df_crawled_obs_stn = df.rename(columns=col_name)

    # Step 1-2: 確保資料起始日期為datetime認可的格式
    df_crawled_obs_stn["State_valid_from"] = pd.to_datetime(
        df_crawled_obs_stn["State_valid_from"], format="ISO8601", errors='coerce').dt.strftime("%Y-%m-%d")

    # Step 1-3: 標示測站工作狀態，由於前面e_Obs_station_info.py指定div id為existing_station，故工作狀態都是running
    df_crawled_obs_stn["Station_working_state"] = "Running"
    return df_crawled_obs_stn


def get_table_from_sqlserver(table_name_in_sql: str) -> pd.DataFrame:
    """Read the existing table of Observation stations in MySQL server.
    Filter and return the latest valid station information in new dataframe."""
    # Step 2: 讀取現在資料表中的既有資料
    # Step 2-1: 準備與本地端MySQL server的連線
    # load_dotenv()
    # username = quote_plus(os.getenv("mysqllocal_username"))
    # password = quote_plus(os.getenv("mysqllocal_password"))
    # server = "127.0.0.1:3306"
    # db_name = "TESTDB"
    # conn = create_engine(
    #     f"mysql+pymysql://{username}:{password}@{server}/{db_name}",).connect()
    # writer = quote_plus(os.getenv("mail_address"))

    # 或Step 2-1: 準備與GCP VM上的MySQL server的連線
    load_dotenv()
    username = quote_plus(os.getenv("mysql_username"))
    password = quote_plus(os.getenv("mysql_password"))
    server = "127.0.0.1:3307"
    db_name = "test_weather"
    conn = create_engine(
        f"mysql+pymysql://{username}:{password}@{server}/{db_name}",).connect()
    writer = quote_plus(os.getenv("mail_address"))

    # Step 2-3: 改讀取既有資料表`Obs_Stations`到dataframe，留下state_valid_to = 9999-12-31的資料(因為9999-12-31代表不知道何時會失效的最新狀態)
    dql_text = text(
        f"SELECT `Station_id`, `Station_working_state`, `Station_record_id`, `State_valid_to` FROM {table_name_in_sql} WHERE `State_valid_to` = DATE('9999-12-31');")
    df_obs_stn_curr_in_sql = pd.read_sql(dql_text, conn)
    df_obs_stn_curr_in_sql["State_valid_to"] = pd.to_datetime(
        df_obs_stn_curr_in_sql["State_valid_to"])

    return df_obs_stn_curr_in_sql


def map_rows_to_be_inserted(df_crawled_obs_stn: pd.DataFrame,
                            df_obs_stn_curr_in_sql: pd.DataFrame) -> pd.DataFrame:
    """Compare the crawled table and table loaded from MySQL server to summarize which rows in
    the crawled table is the new inititiated data. This may occur on a newly built station which
    is consqeuently first posted in the webpage(your data source), or, the change in working state
    of stations from "Previous Run" to "Running" after maintenance. The returned dataframe means
    new data rows to be inserted into MySQL table."""

    # Step 3: 將Step1&Step2做join，讓接下來可以同一列、跨欄比較要以哪幾個資料為主。
    # 目標是過濾出新增的測站
    # 由於爬蟲時可能發現新的測站，所以以爬蟲存下的df為左表、left-join先前存的資料表。
    df_merged1 = df_crawled_obs_stn.merge(df_obs_stn_curr_in_sql, how="left",
                                          left_on="Station_id", right_on="Station_id",
                                          suffixes=("_new", "_in_sqlserver"))

    # 新的測站或一度中斷後又重啟的測站，在Station_working_state_in_sqlserver這個欄位會是null。
    df_new_stn = df_merged1[df_merged1["Station_working_state_in_sqlserver"].isna()]
    return df_new_stn


def map_rows_to_be_updated(df_crawled_obs_stn: pd.DataFrame,
                           df_obs_stn_curr_in_sql: pd.DataFrame) -> pd.DataFrame:
    """Compare the crawled table and table loaded from MySQL server to summarize which rows in
    the crawled table is non-equivalent to those in MySQL table. This may occur on the change 
    in the working status of an existing station from "Running" to "Previous Run" before maintenance. 
    The returned dataframe means the data rows to be updated. """

    # Step 4: 將Step1&Step2做join，讓接下來可以同一列、跨欄比較要以哪幾個資料為主。
    # 目標是過濾出狀態發生變化的既有測站，既有測站若暫停服務或中斷後重啟，則需要小心為資料表做同步更新
    # 所以join時，以先前存在SQL server的資料表為左表、left-join爬蟲存下的df。
    df_merged2 = df_obs_stn_curr_in_sql.merge(df_crawled_obs_stn, how="left",
                                              left_on="Station_id", right_on="Station_id",
                                              suffixes=("_in_sqlserver", "_new"))

    # 既有測站暫停服務 -> Station_working_state_new: null，Station_working_state_in_sqlserver: running
    # 既有測站中斷後重啟 -> Station_working_state_in_sqlserver: Previous Run，但此筆會經過get_tabel_from_sqlserver()函式處理時先被踢除掉，
    # 導致在執行map_rows_to_be_inserted()函式時先被捕捉到，當成以新的一筆資料列插入。
    df_existing_stn_change = df_merged2[df_merged2["Station_working_state_new"]
                                        != df_merged2["Station_working_state_in_sqlserver"]]
    return df_existing_stn_change


def t_Obs_station(df_raw_obs_stations: pd.DataFrame | list) -> tuple[pd.DataFrame]:
    """Execute the functions, 'clean_crawled_table()', 'get_table_from_sqlserver()',
    map_rows_to_be_inserted() and map_rows_to_be_updated() in order.
    If the given df_raw_obs_stations is an empty list, then return None. 
    Otherwise, return two dataframes in tuple of  (df_new_stn_to_be_inserted, 
    df_existing_stn_to_be_updated)."""

    # 定義好讀資料時的資料型態，如果df_raw_obs_stations來自備份檔csv。則按以下dtype做read_csv。
    col_map_csv = {"站號": {"name": "Station_id", "type_in_pd": object},
                   "站名": {"name": "Station_name", "type_in_pd": object},
                   "海拔高度(m)": {"name": "Station_sea_level", "type_in_pd": float},
                   "經度": {"name": "Station_longitude_WGS84", "type_in_pd": float},
                   "緯度": {"name": "Station_latitude_WGS84", "type_in_pd": float},
                   "資料起始日期": {"name": "State_valid_from", "type_in_pd": object},
                   "備註": {"name": "Remark", "type_in_pd": object},
                   }
    dtypes = {k: v["type_in_pd"] for k, v in col_map_csv.items()}
    col_name = {k: v["name"] for k, v in col_map_csv.items()}

    if isinstance(df_raw_obs_stations, pd.DataFrame):
        df_crawled_obs_stn = clean_crawled_table(df_raw_obs_stations, col_name)
        df_obs_stn_curr_in_sql = get_table_from_sqlserver("Obs_stations")
        df_new_stn = map_rows_to_be_inserted(
            df_crawled_obs_stn, df_obs_stn_curr_in_sql)
        df_existing_stn_change = map_rows_to_be_updated(
            df_crawled_obs_stn, df_obs_stn_curr_in_sql)

        return df_new_stn, df_existing_stn_change
    else:
        return pd.DataFrame([[]]), pd.DataFrame([[]])


df_new_stn, df_existing_stn_change = t_Obs_station(df_raw_obs_stations)

if __name__ == "__main__":
    # 以下是本地端測試區：存成csv
    curr_dir = Path().resolve()
    save_path = curr_dir/"processed_csv"/"obs_stn_change_csv"
    save_path.mkdir(parents=True, exist_ok=True)
    filename_e = save_path / \
        f"Existing_stations_status_change_{datetime.now().date()}.csv"
    filename_n = save_path / \
        f"New_stations_to_be_added_{datetime.now().date()}.csv"
    df_existing_stn_change.to_csv(
        filename_e, encoding="utf-8-sig", index=False)
    df_new_stn.to_csv(filename_n, encoding="utf-8-sig", index=False)
