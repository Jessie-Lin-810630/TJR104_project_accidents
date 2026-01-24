import pandas as pd
from pathlib import Path
import hashlib  # 產生PK (SHA256雜湊法)

# 讀檔前先定義資料型別
col_map = {"發生年度": {"name": "accident_year", "type": int},
           "發生月份": {"name": "accident_month", "type": int},
           "發生日期": {"name": "accident_date", "type": object},
           "發生時間": {"name": "accident_time", "type": object},
           "事故類別名稱": {"name": "accident_category", "type": object},
           "處理單位名稱警局層": {"name": "police_department", "type": object},
           "發生地點": {"name": "accident_location", "type": object},
           "天候名稱": {"name": "weather_condition", "type": object},
           "光線名稱": {"name": "light_condition", "type": object},
           "道路類別-第1當事者-名稱": {"name": "road_type_primary_party", "type": object},
           "速限-第1當事者": {"name": "speed_limit_primary_party", "type": object},
           "道路型態大類別名稱": {"name": "road_form_major", "type": object},
           "道路型態子類別名稱": {"name": "road_form_minor", "type": object},
           "事故位置大類別名稱": {"name": "accident_position_major", "type": object},
           "事故位置子類別名稱": {"name": "accident_position_minor", "type": object},
           "路面狀況-路面鋪裝名稱": {"name": "road_surface_pavement", "type": object},
           "路面狀況-路面狀態名稱": {"name": "road_surface_condition", "type": object},
           "路面狀況-路面缺陷名稱": {"name": "road_surface_defect", "type": object},
           "道路障礙-障礙物名稱": {"name": "road_obstacle", "type": object},
           "道路障礙-視距品質名稱": {"name": "sight_distance_quality", "type": object},
           "道路障礙-視距名稱":  {"name": "sight_distance", "type": object},
           "號誌-號誌種類名稱": {"name": "traffic_signal_type", "type": object},
           "號誌-號誌動作名稱": {"name": "traffic_signal_action", "type": object},
           "車道劃分設施-分向設施大類別名稱": {"name": "lane_divider_direction_major", "type": object},
           "車道劃分設施-分向設施子類別名稱": {"name": "lane_divider_direction_minor", "type": object},
           "車道劃分設施-快慢車道間名稱": {"name": "lane_divider_fast_slow", "type": object},
           "車道劃分設施-主車道線名稱": {"name": "lane_divider_main_general", "type": object},
           "車道劃分設施-路邊邊線名稱": {"name": "lane_edge_marking", "type": object},
           "事故類型及型態大類別名稱": {"name": "accident_type_major", "type": object},
           "事故類型及型態子類別名稱": {"name": "accident_type_minor", "type": object},
           "肇因研判大類別名稱-主要": {"name": "cause_analysis_major_primary", "type": object},
           "肇因研判子類別名稱-主要": {"name": "cause_analysis_minor_primary", "type": object},
           "死亡受傷人數": {"name": "casualties_count", "type": object},
           "當事者順位": {"name": "party_sequence", "type": int},
           "當事者區分-類別-大類別名稱-車種": {"name": "vehicle_type_major", "type": object},
           "當事者區分-類別-子類別名稱-車種": {"name": "vehicle_type_minor", "type": object},
           "當事者屬-性-別名稱": {"name": "gender", "type": object},
           "當事者事故發生時年齡": {"name": "age", "type": object},
           "保護裝備名稱": {"name": "protective_equipment", "type": object},
           "行動電話或電腦或其他相類功能裝置名稱": {"name": "mobile_device_usage", "type": object},
           "當事者動作大類別名稱": {"name": "party_action_major", "type": object},
           "當事者動作子類別名稱": {"name": "party_action_minor", "type": object},
           "碰撞部位-最初大類別名稱": {"name": "impact_point_major_initial", "type": object},
           "碰撞部位-最初子類別名稱": {"name": "impact_point_minor_initial", "type": object},
           "碰撞部位-其他大類別名稱": {"name": "impact_point_major_other", "type": object},
           "碰撞部位-其他子類別名稱": {"name": "impact_point_minor_other", "type": object},
           "肇因研判大類別名稱-個別": {"name": "cause_analysis_major_individual", "type": object},
           "肇因研判子類別名稱-個別": {"name": "cause_analysis_minor_individual", "type": object},
           "肇事逃逸類別名稱-是否肇逃": {"name": "hit_and_run", "type": object},
           "經度": {"name": "longitude (WGS84)", "type": float},
           "緯度": {"name": "latitude (WGS84)", "type": float},
           }
# print(len(col_map))


def read_and_concat_csv(file_dir: Path, keyword_of_file_name: str,
                        dp: dict, column_name: list) -> pd.DataFrame:
    """Read the files about the A1 or A2 accidents in the 'file_dir' folder and then
    concat, load into a new DataFrame.Keyword_of_file_name: A1/A2"""

    all_df = []
    target_file_name = f'*{keyword_of_file_name}*.csv'
    count = 0
    # 用glob遍歷來源資料夾，然後逐一讀取csv直至存進all_df list暫存。
    for a_csv in file_dir.glob(target_file_name):
        print('正在讀取:', a_csv.name)
        count += 1
        # 每個csv檔的尾巴有兩筆無法正常按照dtype轉換，故跳過這2列 #skipfooter只有engine設為python時才有支援。
        df = pd.read_csv(a_csv, dtype=dp,
                         skipfooter=2, engine='python')

        # 將每一個df物件暫存到list中
        all_df.append(df)

    # 遍歷all_df中的df，逐一串接。
    concat_df = pd.concat(all_df)
    concat_df = concat_df.drop_duplicates()  # 去除重複項if exist

    # 資料清理
    concat_df = concat_df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # 清理日期欄位的格式
    # 20260101這種格式pandas自己可以識別清楚且保證與SQL的日期格式相容。
    concat_df['發生日期'] = pd.to_datetime(concat_df['發生日期'])

    # 清理時間欄位的格式
    # 雷點：不支援有冒號的寫法%HH:%MM:%SS。也不支援%HH%MM%SS
    concat_df['發生時間'] = pd.to_datetime(
        concat_df['發生時間'], format='%H%M%S').dt.time
    # 再轉成字串，因為MySQL認不得pandas的time object這個物件。
    concat_df['發生時間'] = concat_df['發生時間'].astype(str)

    # 將欄位名稱從中文轉成英文
    concat_df.columns = column_name

    # 生成主鍵PK
    concat_df['accident_id'] = concat_df.apply(lambda row: int(hashlib.sha256(
        f"{row['accident_date']}{row['accident_time']}{row['accident_location']}{row['longitude (WGS84)']}{row['latitude (WGS84)']}".encode()).hexdigest(), 16) % (10**15), axis=1)

    concat_df.set_index("accident_id", inplace=True, drop=True)
    concat_df["Nearest_station_ID"] = "TBD"

    print(f"總共處理了{count}個csv檔.")
    return concat_df


curr_dir = Path().resolve()

# 以下是範例，應視自己把車禍資料源放在哪個資料夾改。
src_dir = Path(
    "/Users/little_po/Desktop/Project/04_Traffic_accidents/00_data_sources/113年傷亡道路交通事故資料")

dtypes = {k: v['type'] for k, v in col_map.items()}
col_name = [v['name'] for v in col_map.values()]
concat_df_A1 = read_and_concat_csv(
    src_dir, keyword_of_file_name='A1', dp=dtypes, column_name=col_name)
concat_df_A2 = read_and_concat_csv(
    src_dir, keyword_of_file_name='A2', dp=dtypes, column_name=col_name)


if __name__ == "__main__":
    # 存成csv
    save_to_dir = curr_dir/"traffic_accient_transformed-csv"
    save_to_dir.mkdir(parents=True, exist_ok=True)
    concat_df_A1.to_csv(save_to_dir/"A1_summary_113.csv",
                        # 用utf-8-sig，只是以利excel decoding時不會出現亂碼，但實際上其他語言來說-sig就是utf-8
                        index=True, encoding="utf-8-sig")
    concat_df_A2.to_csv(save_to_dir/"A2_summary_113.csv",
                        index=True, encoding="utf-8-sig")
    print("存檔成功!")
