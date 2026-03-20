import pandas as pd
import numpy as np
import hashlib
from pathlib import Path
from sqlalchemy import text
from datetime import datetime, timedelta, timezone
from src.util.table_column_map import fact_accident_human_col_origin_map
from src.util.get_table_from_sql_server import get_table_from_sqlserver
from src.task.e_crawling_traffic_accident import (e_crawling_historical_traffic_accident,
                                                  e_crawling_latest_traffic_accident)


def t_fact_accident_human(csvfile_paths: list[str]) -> pd.DataFrame:
    """s"""
    all_df = []
    for file_path in csvfile_paths:
        print(f"正在處理csv檔案: {file_path}")
        # 讀取csv檔案到DataFrame
        df = pd.read_csv(file_path, encoding="utf-8", skipfooter=2, engine="python")

        # 擷取需要的欄位
        required_columns = [k for k in fact_accident_human_col_origin_map.keys()]
        df = df.loc[:, required_columns]

        # 重新命名欄位
        renamed_required_columns = [fact_accident_human_col_origin_map[k] for k in required_columns]
        df.columns = renamed_required_columns

        # 清理發生日期
        df["accident_date"] = pd.to_datetime(df["accident_date"], errors="coerce",
                                             format="%Y%m%d")
        df["accident_date"] = df["accident_date"].astype(str)

        # 清理發生時間
        df["accident_time"] = df['accident_time'].astype(str).str.zfill(6)
        df["accident_time"] = df["accident_time"].apply(lambda r: r[0:2] + ":" +
                                                        r[2:4] + ":" + r[4:])
        # 清理經緯度
        df["longitude"] = df["longitude"].astype("float64")
        df["latitude"] = df["latitude"].astype("float64")

        # 清理age
        df["age"] = np.where(df["gender"].isin(["男", "女"]), df["age"], -1)

        # 清理肇逃
        df["hit_and_run"] = df["hit_and_run"].apply(lambda r: 1 if r == "是" else 0)

        # 建立is_primary_party_sequence欄位
        df["is_primary_party_sequence"] = None
        df["party_sequence"] = df["party_sequence"].astype("int64")
        df["is_primary_party_sequence"] = np.where(df["party_sequence"].eq(1), 1, 0)

        # 去字串空白
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

        all_df.append(df)
        print(f"成功讀取csv檔案: {file_path}。此輪得到列數: {len(df)}")

    # union
    df = pd.concat(all_df)

    # 找day_id關聯
    query = "SELECT day_id, accident_date FROM dim_accident_day;"
    df_dim_accident_day = get_table_from_sqlserver(query, database="traffic_accidents")
    df_dim_accident_day["accident_date"] = df_dim_accident_day["accident_date"].astype(str)
    df_merged = df.merge(df_dim_accident_day, how="left", left_on="accident_date",
                         right_on="accident_date")

    # 找accident_id關聯
    query = """SELECT accident_id, day_id, accident_time, longitude, latitude 
                    FROM fact_accident_main;"""
    df_fact_accident_main = get_table_from_sqlserver(query, database="traffic_accidents")
    df_fact_accident_main["accident_time"] = df_fact_accident_main["accident_time"].astype(
        str).str.replace("0 days", "").str.strip()
    df_fact_accident_main["longitude"] = df_fact_accident_main["longitude"].astype("float64")
    df_fact_accident_main["latitude"] = df_fact_accident_main["latitude"].astype("float64")
    df_merged = df_merged.merge(df_fact_accident_main, how="left",
                                left_on=["day_id", "accident_time",
                                         "longitude", "latitude"],
                                right_on=["day_id", "accident_time",
                                          "longitude", "latitude"])

    # 生成row_hash
    uk = (df_merged["accident_id"].astype(str) + "|" +
          df_merged["party_sequence"].astype(str) + "|" +
          df_merged["age"].astype(str) + "|" +
          df_merged["gender"].astype(str) + "|" +
          df_merged["cause_analysis_minor_individual"].astype(str) + "|" +
          df_merged["impact_point_minor_other"].astype(str))
    df_merged["row_hash"] = uk.apply(lambda x: hashlib.sha256(x.encode()).hexdigest()[:32])

    # 留著檢查row_hash設計是否足夠確保業務唯一性
    dup_cnt = df_merged["row_hash"].duplicated().sum()
    print(f"There are {dup_cnt} rows having duplicated combination of accident_id, party_sequence,"
          f"age, gender, cause_analysis_minor_individual and impact_point_minor_other.")

    # 留下想要的欄位
    df_fact_accident_human = df_merged.loc[:, ["accident_id", "party_sequence",
                                               "is_primary_party_sequence", "gender",
                                               "age", "protective_equipment",
                                               "mobile_device_usage", "party_action_major",
                                               "party_action_minor", "vehicle_type_major",
                                               "vehicle_type_minor",
                                               "cause_analysis_major_individual",
                                               "cause_analysis_minor_individual",
                                               "serving_sharing_economy_or_delivery",
                                               "impact_point_major_initial",
                                               "impact_point_minor_initial",
                                               "impact_point_major_other",
                                               "impact_point_minor_other",
                                               "hit_and_run", "row_hash"
                                               ]]

    # 填補空值，將NaN轉換成None
    df_fact_accident_human = df_fact_accident_human.replace({np.nan: None})

    return df_fact_accident_human


df_fact_accident_human = t_fact_accident_human(csvfile_paths=["/Users/little_po/Desktop/Project/04_Traffic_accidents/taiwan_traffic_accidents/test/processed_data/114年度A1交通事故資料.csv",
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

    df_fact_accident_human = t_fact_accident_human(csvfile_paths=["/Users/little_po/Desktop/Project/04_Traffic_accidents/taiwan_traffic_accidents/test/processed_data/114年度A1交通事故資料.csv",
                                                                  "/Users/little_po/Desktop/Project/04_Traffic_accidents/taiwan_traffic_accidents/test/processed_data/114年度A2交通事故資料_5.csv"])
    print(df_fact_accident_human.head())
    print(df_fact_accident_human.info())
