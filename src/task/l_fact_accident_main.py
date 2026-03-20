import pandas as pd
from sqlalchemy import text
from src.util.create_engine_to_mysql import get_pymysql_conn_to_mysql
from src.task.t_fact_accident_main import df_fact_accident_main


def l_fact_accident_main(df_fact_accident_main: pd.DataFrame,
                         database: str | None = None) -> None:
    """"""
    # 準備INSERT資料表時需要的SQL語句，採用UPSERT
    # 先準備INSERT部分
    columns = ', '.join(df_fact_accident_main.columns)
    placeholders = ', '.join(['%s'] * len(df_fact_accident_main.columns))

    # 準備UPDATE的部分：故意只更新accident_time。
    update_part = "accident_time=VALUES(accident_time)"

    # 組合出完整SQL語句
    dml_str = f"""INSERT INTO fact_accident_main ({columns})
                  VALUES ({placeholders})
                  ON DUPLICATE KEY UPDATE {update_part};
                """

    # 6. 寫入資料表
    print(f"====Inserting into table `fact_accident_main`....====")
    try:
        conn = get_pymysql_conn_to_mysql(database)
        cursor = conn.cursor()
        cursor.executemany(dml_str, df_fact_accident_main.values.tolist())
        conn.commit()
    except Exception as e:
        print(f"Error on inserting into table, Error msg: {e}")
        conn.rollback()
    else:
        print(f"====Successfully inserting into table `fact_accident_main`====")
    finally:
        cursor.close()
        conn.close()
    return None


if __name__ == "__main__":
    # 測試區
    l_fact_accident_main(df_fact_accident_main, "traffic_accidents")
