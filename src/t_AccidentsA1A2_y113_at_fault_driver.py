from t_AccidentsA1A2_y113 import concat_df_A1, concat_df_A2
from pathlib import Path

# 呼叫t_AccidentsA1A2_y113.py清理過的final_dataframe of A1/A2
# 只留下順位1的資料，因為多少個順位1就代表多少筆事件。
df_A1 = concat_df_A1.copy()
df_A2 = concat_df_A2.copy()
df_A1 = df_A1[df_A1["party_sequence"] == 1]
df_A2 = df_A2[df_A2["party_sequence"] == 1]


if __name__ == "__main__":
    curr_dir = Path(__file__).resolve().parent  # src
    save_to_dir = curr_dir.parent/"traffic_accient_transformed-csv"
    df_A1.to_csv(save_to_dir/"A1_summary_113(at-fault-driver-only).csv",
                 index=True, encoding="utf-8-sig")  # index要改成True，因為index = 進schema後的PK
    df_A2.to_csv(save_to_dir/"A2_summary_113(at-fault-driver-only).csv",
                 index=True, encoding="utf-8-sig")
