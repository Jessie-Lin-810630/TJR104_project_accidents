from pathlib import Path
import requests
from bs4 import BeautifulSoup
import pandas as pd
# import sqlalchemy


def crawler_stn_info(stdIDurl: str):
    """Scraping from the government website CODis to obtain the 
    weather observation station list whicn describes the geometry data of
    current stations they are still working, including stations name, 
    station ID, longtitude, latitugde and sea level."""

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"}
    response = requests.get(stdIDurl, headers=headers,
                            stream=True, verify=False)
    mysoup = BeautifulSoup(response.text, "html.parser")

    # 尋找<div> tag以及id=existing_station的標籤
    existing_stn = mysoup.find("div", {"id": "existing_station"})

    # 尋找表頭標籤
    tableheader = existing_stn.find("tr", class_="active")
    columns = tableheader.find_all("th")

    # 尋找表頭以下的資料
    tabledata_rows = existing_stn.find_all("tr")[1:]  # [1:]代表跳過tableheader這列

    # 在表頭標籤中尋找表的欄位名稱，確認站號、站名、海拔高度(m)、經度、緯度落在第幾欄
    station_info = {}
    for i in range(len(columns)):
        columns_text = columns[i].text  # 由左至右逐一讀取表頭的欄位名稱，並標示它在第幾欄
        if columns_text in ["站號", "站名", "海拔高度(m)", "海拔高度（m）", "經度", "緯度"]:
            station_info[columns_text] = []
            for rows in tabledata_rows:  # 找到特定欄位名稱後，往下遍歷每一列
                data = rows.find_all("td")
                station_info[columns_text].append(data[i].text)  # 尋找屬於該欄的資料
        else:
            print(f"第{i}欄，不是站號、站名、海拔高度(m)、經度、緯度")
    return station_info


if __name__ == '__main__':

    # 設定未來存檔資料夾路徑
    curr_dir = Path().resolve()
    save_path = curr_dir/"supplementary_weather_csv_from_CODiS"
    save_path.mkdir(parents=True, exist_ok=True)

    # 從CODis取得現有運作的測站清單，並存成csv。
    stn_url = "https://hdps.cwa.gov.tw/static/state.html"
    weather_obs_stations = crawler_stn_info(stn_url)  # 爬蟲執行

    # 轉成pandas dataframe物件
    # dtypes = {"站號": object, "站名": object,
    #           "海拔高度": float, "經度": float, "緯度": float} #不知道要不要寫
    df_weather_obs_stations = pd.DataFrame(weather_obs_stations)  # 863個測站

    # # 存成csv
    # df_weather_obs_stations.to_csv(
    #     save_path/"station_info_table.csv", encoding="utf-8", index=False)

    # 存入資料庫
