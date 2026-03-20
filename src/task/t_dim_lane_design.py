import pandas as pd
import numpy as np
from pathlib import Path
from src.util.table_column_map import dim_lane_design_col_map
from src.task.e_crawling_traffic_accident import (e_crawling_historical_traffic_accident,
                                                  e_crawling_latest_traffic_accident)


def t_dim_lane_design(csvfile_paths: list[str]) -> pd.DataFrame:
    """
    這個函式的目的是從csv檔案中讀取交通事故資料，並且從中萃取出車道設計的維度表。
    這個維度表會包含每一種車道類型的唯一ID、名稱、描述等資訊，方便後續分析使用。
    :param csvfile_paths: 包含csv檔案路徑的列表，這些csv檔案是從政府資料開放平台爬取的交通事故資料。
    :return: 一個DataFrame，包含車道設計維度表的資料。
    """
    all_df = []
    for file_path in csvfile_paths:
        print(f"正在處理csv檔案: {file_path}")
        # 讀取csv檔案到DataFrame
        df = pd.read_csv(file_path, encoding="utf-8", skipfooter=2, engine="python")

        # 擷取需要的欄位
        required_columns = [k for k in dim_lane_design_col_map.keys()]
        df = df.loc[:, required_columns]

        # 重新命名欄位
        renamed_required_columns = [dim_lane_design_col_map[k] for k in required_columns]
        df.columns = renamed_required_columns

        # 去空白
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

        # 去重
        single_df = df.drop_duplicates(subset=["lane_divider_direction_major",
                                               "lane_divider_direction_minor",
                                               "lane_divider_main_general",
                                               "lane_divider_fast_slow",
                                               "lane_edge_marking"])
        all_df.append(single_df)
        print(f"成功讀取csv檔案: {file_path}。此輪得到列數: {len(single_df)}")

    df_dim_lane_design = pd.concat(all_df).drop_duplicates(subset=["lane_divider_direction_major",
                                                                   "lane_divider_direction_minor",
                                                                   "lane_divider_main_general",
                                                                   "lane_divider_fast_slow",
                                                                   "lane_edge_marking"])
    df_dim_lane_design = df_dim_lane_design.reset_index(drop=True, inplace=False)
    return df_dim_lane_design


df_dim_lane_design = t_dim_lane_design(csvfile_paths=["/Users/little_po/Desktop/Project/04_Traffic_accidents/taiwan_traffic_accidents/test/processed_data/114年度A1交通事故資料.csv",
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
    df_dim_lane_design = t_dim_lane_design(csvfile_paths=["/Users/little_po/Desktop/Project/04_Traffic_accidents/taiwan_traffic_accidents/test/processed_data/114年度A1交通事故資料.csv",
                                                          "/Users/little_po/Desktop/Project/04_Traffic_accidents/taiwan_traffic_accidents/test/processed_data/114年度A2交通事故資料_5.csv"])
