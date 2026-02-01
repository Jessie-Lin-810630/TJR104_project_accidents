# t_AccidentsA1A2_y113.py
1. 時間(時:分:秒)欄位的清理
    1. 概念：讀取原生資料後，利用string concat(+)語法或補字(.zfill())方法拼成pandas看得懂的時間格式。然後要存入SQL server時(例如：使用dataframe.to_sql()方法)，明確告知sql這個欄位的格式是sql中的time型別。
    2. 以183054、74230這樣的時間格式為例：183054應該表達為18:30:54，所以要補冒號；而74230應該要表達為07:42:30，所以要補數字與補冒號。存入SQL才不容易報錯。
    3. 承上述情境，歸納有兩種組合技可以嘗試：
        ```
        # 組合技一
        time_str = concat_df['發生時間'].astype(str).str.zfill(6) # 補字
        pd.to_datetime(time_str, format='%H%M%S').dt.strftime('%H:%M:%S') # 轉成datetime物件，讓pandas了解時間格式依序為 時分秒。然後再reparsing回字串順便補冒號。
        ```

        ```
        # 組合技二
        time_str = concat_df['發生時間'].astype(str).str.zfill(6) # 補字
        concat_df['發生時間'] = time_str.str[0:2] + ':' + time_str.str[2:4] + ':' + time_str.str[4:6] # 也是補字
        ```

    4. 補充，直接輸出datetime、time物件其實也可以，但穩定度稍微不如以string輸出給SQL server，但這種做法的好處是物件導向的設計讓人類比較好閱讀、開發難度不高。範例如下：
        ```
        concat_df['發生時間'] = pd.to_datetime(concat_df['發生時間'], format='%H%M%S').dt.time
        ```
        
    5. 補充：撰寫雷點，pd.to_datetime(format="????")，????不支援有冒號的寫法%HH:%MM:%SS、%H:%M:%S，處理這行時要小心不要跟strftime() method的要求搞混。

# t_AccidentsA1A2_y113_at_fault_driver.py
1. `Path().resolve() vs. Path(__file__).resolve()`
    ```
    Path().resolve() # 回傳目前工作根目錄之絕對路徑，型態為Path物件
    Path(__file__).resolve() # 回傳目前腳本之絕對路徑，型態為Path物件
    ```
2. 補充 Jupyter notebook中， `__file__ ` 變數不存在，因為Notebook不是像.py腳本那樣從檔案載入執行，而是透過互動式kernel逐cell運行，所以呼叫`Path(__file__)` 時，會拋出`NameError: name '__file__' is not defined`。為什麼會這樣？
    1. .py 檔案： 當Python執行一個檔案時，它知道這個檔案在硬碟的哪個位置，所以會自動建立`__file__`變數，儲存該檔案的路徑。
    2. Jupyter Notebook： 代碼是在一個互動式的內核(Kernel)中執行的，代碼塊是臨時傳送給內核的「字串」，並沒有對應到硬碟上的某個.py檔案路徑，因此` __file__` 變數不存在。
    3. 在.ipynb檔中可以用這個方式找到
3. 承2.，`if __name__ == "__main__"` in .py vs. in .ipynb
    1. 在Jupyter Notebook中是有`__name__`變數的。但它的行為與一般的.py檔不同。
    2. 在 Jupyter Notebook 的環境下，`__name__` 這個變數永遠會被賦值為 `"__main__"`。
    3. 但在Jupyter notebook環境中還是可以保持習慣寫`if __name__ == "__main"`，方便移植到.py檔後，且不會被其他.py檔import時，不會自動執行測試區程式碼或主程式邏輯。如果不移植，在Jupyter notebook中維持寫這行也可以提醒自己、code viewers說這段是測試用的。

# l_AccidentsA1A2_y113.py
1. As mentioned in the paragraph for t_AccidentsA1A2_y113.py, must carefully assign the data types of each columns when using pandas.dataframe.to_sql() in the parameter 'dtype'. This make sure the stability of type conversion from file system to DBMS.

2. When using .to_sql('table_name') method to load to SQL server, if the table does not exist then the sql server will automatically create a new table as the table_name given in to_sql().

3. engine.begin vs. engine.connect()
    1. engine.begin(): 搭配with begin() as conn 上下文管理器時，可以自動commit或rolling back。 
    2. while connect() 手動commit或rolling back每筆交易。
    3. with engine.begin() as conn: starts a transaction, executes code (like INSERTs, UPDATEs), and commits automatically, making it great for simple, single-transaction tasks, whereas with engine.connect() as conn: gives more control, allowing for multiple statements within a single transaction or different transaction management.

# e_Obs_station_info.py
1. verify=False用意禁用SSL憑證驗證，繞過certificate verify failed錯誤，常因伺服器自簽憑證或CA問題發生。該頁面是 HTTPS，台灣政府網站偶有憑證不符或過期問題，使用此設定確保連線成功。但生產環境避免使用，以防中間人攻擊；可試更新 certifi 套件或用 verify=’/path/to/ca_bundle’ 替代。兩個做法：
    ```
    # 下載中央氣象署憑證或政府 CA：
    response = requests.get(url, headers=headers, verify='/path/to/cwa_ca_bundle.crt')
    ```
    ```
    import certifi
    import requests
    response = requests.get(url, headers=headers, verify=certifi.where())
    ```
2. stream=True 用意啟用串流模式，逐塊下載內容而不一次載入記憶體。頁面是大型 Markdown 表格（數百氣象站資料），適合大檔案爬取，避免記憶體溢位。之後用 response.iter_content() 處理資料。

# l_Obs_station_info.py
1. quote_plus用於將字串中的特殊字符(如: 空格、&、=) 轉換成url安全的百分比編碼格式，特別適合查詢參數(query string)。
2. Insert into existing Table敘述，有以sqlalchemy+SQL statement為主體的寫法與python-list+pandas的寫法。
    2.1 以sqlalchemy為主，用比較多SQL statement
    ```
        # source_df: 打算寫入Table的資料表
        engine = create_engine(f"mysql+pymysql://{username}:{password}@{server}/{db_name}")
        with engine.connect() as conn:
            for _, row in source_df.iterrows():
                dml_text = text("""INSERT INTO Obs_stations
                                        (Station_id, Station_name, Station_sea_level)
                                    VALUES
                                        ((:Station_id), (:Station_name), (:Station_sea_level));
                                """)
                conn.execute(dml_text, {"Station_id": row["Station_id"],
                                        "Station_name": row["Station_name"],
                                        "Station_sea_level": row["Station_sea_level"],
                                        ...,
                                        ...,
                                        })
                conn.commit()
    ```
    2.2 以python-list+pandas寫
    ```
        # source_df: 打算寫入Table的資料表
        row_info_to_insert = []
        for _, row in source_df.iterrows():
            row_info_to_insert.append({
                "Station_id": row["Station_id"],
                "Station_name": row["Station_name"],
                "Station_sea_level": row["Station_sea_level"],
                ....,
                ....
                })
        df_to_insert = pd.DataFrame(row_info_to_insert)
        conn = create_engine(
                f"mysql+pymysql://{username}:{password}@{server}/{db_name}").connect()
        df_to_insert.to_sql("Obs_stations", conn, index=False, if_exists="append")
        conn.commit()
    ```
    2.3 兩者比較
        | 面向     | 2.1逐列sqlalchemy                         | 2.2 pandas+to_sql批次寫入           |
        |----------|-------------------------------------------|-------------------------------------|
        | 操作層級 | per‑row SQL 語句                          | per‑DataFrame 批次寫入              |
        | 效能     | 慢，N筆 = N次execute+commit               | 快，批次送出，driver 可再調優       |
        | 控制粒度 | 高：可per‑row查詢、條件邏輯、複雜SQL      | 中：適合單純 insert / append        |
        | 可讀性   | 程式碼較長，SQL 模板＋欄位 mapping 要維護 | 程式簡短，欄位對齊 DataFrame 即可   |
        | 交易行為 | 可自己決定何時 commit                     | 多半整批一個 transaction            |
        | 適用情境 | 少量資料、複雜邏輯、需高度客製 SQL        | 大量資料、單純 append、ETL pipeline |      

# t_find_nearest_Obs_station.py
1. 尋找最近觀測站
    ```
    # gpd.GeoDataFrame()可將Pandas的DataFrame 轉換為 GeoPandas的GeoDataFrame
    # gpd.points_from_xy()把經緯度字串轉成pandas看得懂的"點(Point)"。
    # gpd.to_crs()可為Point所屬座標系做轉換，例如轉成"EPSG 3826公尺座標系"，代表在公尺座標系下的位置
    # gpd.set_geometry()用以定義做空間連接時，哪一欄中的Point物件要作為參照物。
    # gpd.sjoin_nearest()用以尋找兩個geodataframe中的參照物的距離，並回傳距離最短的組合(仍然是GeoDataFrame)。
    ```
2. 尋找附近觀測站
    ```
    # gpd.GeoDataFrame()可將Pandas的DataFrame 轉換為 GeoPandas的GeoDataFrame
    # gpd.points_from_xy()把經緯度字串轉成pandas看得懂的"點(Point)"。
    # gpd.to_crs()可為Point所屬座標系做轉換，例如轉成"EPSG 3826公尺座標系"，代表在公尺座標系下的位置
    # gdf.geometry.buffer(distance=10000)可為參照物建立 半徑10000 公尺的緩衝區 (Buffer)
    # gpd.set_geometry()用以定義做空間連接時，哪一欄中的Point物件要作為參照物。即:把 點(Point) 換成 面(Polygon)
    # gpd.sjoin() 尋找緩衝區內是否出現目標建物，並全部列出，包含距離，回傳值仍然是GeoDataFrame。
    # 
    ```

# l_find_nearest_Obs_station.py
1. 使用 Pandas 的 to_sql()方法，且設定 if_exists="replace" 時，Pandas 會幫忙自動偵測資料型態並重新建表。在 Pandas 中，str 類型（Object在寫入 MySQL時，如果不特別指定，Pandas 預設會將其轉換為TEXT型態。TEXT型態長度跨度很大，如果需要將TEXT設為約束，需要對該欄位定義修改前綴長度，限制索引使用的位元組數避免記憶體過載。最直接避險方式就是to_sql()中明確寫好欄位的dtype，例如：VARCHAR(50)。

# e_crawerling_func_weather_data.py
1. 如果遇到Webdriver-manager下載的Chromedriver沒有跟上chrome版本，會報錯 selenium.common.exceptions.WebDriverException。
    1. 解決方式：
        ```
        rm -rf ~/.wdm # 清除WebDriverManager快取、重新下載
        poetry run python src/e_crawerling_func_weather_data.py
        ```
    2. 使用穿透層級語法：//*[contains(text(), '期望該標籤內容帶有什麼關鍵字')]，這會向內掃描標籤下所有內容，即便按鈕被包得太深找得到。
        ``` 
        # //tr[...]//div[...] # 代表在該列底下的任何層級尋找 div 
        ```

# e_find_accidentday_weather_data.py
1. 如果遇到Webdriver-manager下載的Chromedriver沒有跟上chrome版本，會報錯 selenium.common.exceptions.WebDriverException。
    1. 解決方式：
        ```
        rm -rf ~/.wdm # 清除WebDriverManager快取、重新下載
        poetry run python src/e_find_accidentday_weather_data.py
        ```