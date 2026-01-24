from t_AccidentsA1A2_y113 import concat_df_A1, concat_df_A2
from pathlib import Path
import hashlib  # 產生PK (SHA256雜湊法)

# 呼叫t_AccidentsA1A2_y113.py清理過的final_dataframe of A1/A2
# 只留下順位1的資料，因為多少個順位1就代表多少筆事件。
df_A1 = concat_df_A1.copy()
df_A2 = concat_df_A2.copy()
df_A1 = df_A1[df_A1["party_sequence"] == 1]
df_A2 = df_A2[df_A2["party_sequence"] == 1]

# 生成主鍵PK
df_A1['accident_id'] = df_A1.apply(lambda row: int(hashlib.sha256(
    f"{row['accident_date']}{row['accident_time']}{row['accident_location']}{row['longitude (WGS84)']}{row['latitude (WGS84)']}".encode()).hexdigest(), 16) % (10**15), axis=1)
df_A2['accident_id'] = df_A2.apply(lambda row: int(hashlib.sha256(
    f"{row['accident_date']}{row['accident_time']}{row['accident_location']}{row['longitude (WGS84)']}{row['latitude (WGS84)']}".encode()).hexdigest(), 16) % (10**15), axis=1)

df_A1.set_index("accident_id", inplace=True, drop=True)
df_A2.set_index("accident_id", inplace=True, drop=True)

if __name__ == "__main__":
    curr_dir = Path(__file__).resolve().parent  # src
    save_to_dir = curr_dir.parent/"traffic_accient_transformed-csv"
    df_A1.to_csv(save_to_dir/"A1_summary_113(at-fault-driver-only).csv",
                 index=True, encoding="utf-8-sig")  # index要改成True，因為index = 進schema後的PK
    df_A2.to_csv(save_to_dir/"A2_summary_113(at-fault-driver-only).csv",
                 index=True, encoding="utf-8-sig")
