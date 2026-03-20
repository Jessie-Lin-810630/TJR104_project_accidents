import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import text
from datetime import datetime, timedelta, timezone
from src.util.table_column_map import fact_accident_main_col_origin_map
from src.util.get_table_from_sql_server import get_table_from_sqlserver
from src.task.e_crawling_traffic_accident import (e_crawling_historical_traffic_accident,
                                                  e_crawling_latest_traffic_accident)


def t_fact_accident_main(csvfile_paths: list[str]) -> pd.DataFrame:
    """s"""
    all_df = []
    for file_path in csvfile_paths:
        print(f"正在處理csv檔案: {file_path}")
        # 讀取csv檔案到DataFrame
        df = pd.read_csv(file_path, encoding="utf-8", skipfooter=2, engine="python")

        # 擷取需要的欄位
        required_columns = [k for k in fact_accident_main_col_origin_map.keys()]
        df = df.loc[:, required_columns]

        # 重新命名欄位
        renamed_required_columns = [fact_accident_main_col_origin_map[k] for k in required_columns]
        df.columns = renamed_required_columns

        # 清理發生日期
        df["accident_date"] = pd.to_datetime(df["accident_date"], errors="coerce",
                                             format="%Y%m%d")
        df["accident_date"] = df["accident_date"].astype(str)

        # 清理發生時間
        df["accident_time"] = df['accident_time'].astype(str).str.zfill(6)
        df["accident_time"] = df["accident_time"].apply(lambda r: r[0:2] + ":" +
                                                        r[2:4] + ":" + r[4:])
        # 清理死傷人數
        df["death_count"] = df["casualties_count"].apply(lambda r: int(r.split(";")[0].replace("死亡", "")))
        df["injury_count"] = df["casualties_count"].apply(lambda r: int(r.split(";")[1].replace("受傷", "")))

        # 清理經緯度
        df["longitude"] = df["longitude"].astype("float64")
        df["latitude"] = df["latitude"].astype("float64")

        # 去空白
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

        all_df.append(df)
        print(f"成功讀取csv檔案: {file_path}。此輪得到列數: {len(df)}")

    # union
    df = pd.concat(all_df)

    # 找day_id關聯
    query = "SELECT day_id, accident_date FROM dim_accident_day;"
    df_dim_accident_day = get_table_from_sqlserver(query, database="traffic_accidents")
    df_dim_accident_day["accident_date"] = df_dim_accident_day["accident_date"].astype(str)

    df_merged = df.merge(df_dim_accident_day, how="inner", left_on="accident_date",
                         right_on="accident_date")

    # 找accident_type_id關聯
    query = "SELECT * FROM dim_accident_type;"
    df_dim_accident_type = get_table_from_sqlserver(query, database="traffic_accidents")
    df_merged = df_merged.merge(df_dim_accident_type, how="inner",
                                left_on=["accident_category",
                                         "accident_position_major",
                                         "accident_position_minor",
                                         "accident_type_major",
                                         "accident_type_minor"],
                                right_on=["accident_category",
                                          "accident_position_major",
                                          "accident_position_minor",
                                          "accident_type_major",
                                          "accident_type_minor"])

    # 去重
    df_fact_accident_main = df_merged.drop_duplicates(subset=["day_id",
                                                              "accident_time",
                                                              "longitude",
                                                              "latitude"])
    # 排序
    df_fact_accident_main = df_fact_accident_main.sort_values(by=["day_id",
                                                                  "accident_time",
                                                                  "longitude",
                                                                  "latitude"]).reset_index(drop=True)

    # 生成PK (YYYYMMDD + 8位流水號)
    df_fact_accident_main["prefix"] = df_fact_accident_main["accident_date"].astype(str).str.replace("-", "")
    df_fact_accident_main["cumcount"] = df_fact_accident_main.groupby("accident_date").cumcount() + 1
    df_fact_accident_main["accident_id"] = df_fact_accident_main["prefix"] + \
        df_fact_accident_main["cumcount"].astype(str).str.zfill(8)

    # 留下想要的欄位
    df_fact_accident_main = df_fact_accident_main.loc[:, ["accident_id", "accident_type_id",
                                                          "day_id", "accident_time",
                                                          "death_count", "injury_count",
                                                          "longitude", "latitude"]]

    # 填補空值，將NaN轉換成None
    df_fact_accident_main = df_fact_accident_main.replace({np.nan: None})

    return df_fact_accident_main


df_fact_accident_main = t_fact_accident_main(csvfile_paths=["/Users/little_po/Desktop/Project/04_Traffic_accidents/taiwan_traffic_accidents/test/processed_data/114年度A1交通事故資料.csv",
                                                            "/Users/little_po/Desktop/Project/04_Traffic_accidents/taiwan_traffic_accidents/test/processed_data/114年度A2交通事故資料_5.csv"])


if __name__ == "__main__":
    # 測試區
    # # 指定要爬取的網址
    # historical_years_urls = ["https://data.gov.tw/dataset/158865",  # 2021
    #                          "https://data.gov.tw/dataset/177136"]  # 2025
    # this_year_A1_url = ["https://data.gov.tw/dataset/12818"]  # 2026A1
    # this_year_A2_url = ["https://data.gov.tw/dataset/13139"]  # 2026A2

    # # 準備headers
    # headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    #            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"}

    # historical_csvfile_paths = e_crawling_historical_traffic_accident(historical_years_urls,
    #                                                                   headers)
    # this_year_csvfile_paths = e_crawling_latest_traffic_accident(this_year_A1_url,
    #                                                              this_year_A2_url,
    #                                                              headers)
    # print("歷年資料的csv檔案路徑列表: ", historical_csvfile_paths)
    # print("今年資料的csv檔案路徑列表: ", this_year_csvfile_paths)

    df_fact_accident_main = t_fact_accident_main(csvfile_paths=["/Users/little_po/Desktop/Project/04_Traffic_accidents/taiwan_traffic_accidents/test/processed_data/114年度A1交通事故資料.csv",
                                                                "/Users/little_po/Desktop/Project/04_Traffic_accidents/taiwan_traffic_accidents/test/processed_data/114年度A2交通事故資料_5.csv"])
