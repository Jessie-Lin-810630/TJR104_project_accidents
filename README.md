# TJR104_project_accidents
For practice on the traffic accidents

# The scripts in dir ./src/ could be executed by following the order:
1. Download the csv files that recording the Taiwan traffic accidents from (https://data.gov.tw/dataset/12818) and (https://data.gov.tw/dataset/13139). Save in your local file system.

2. Run [t_AccidentsA1A2_y113.py](src/t_AccidentsA1A2_y113.py) to transform the dirty data to the type fit Schema requirement in MySQL. You can also save the transformed data as new csv file before push to MySQL server optionally if server is not ready yet. When doing this, please directly run the script alone (i.e., make sure `__name__` = `__main__`").
    1. [My learning notes for reminders](./learning_notes/learning_notes.md)

3. Optionally, run [t_AccidentsA1A2_y113_at_fault_driver.py](src/t_AccidentsA1A2_y113_at_fault_driver.py) to further filtering the data, leaving the data of at-fault driver ONLY(只留下肇事順位一的最大責任者的資料). By doing this, we can quickly generate new csv file to report how many tracffic accidents, when to happen, location of accident.
    1. [My learning notes for reminders](/learing_notes/learning_notes.md)

4. Run [l_AccidentsA1A2_y113.py](src/l_AccidentsA1A2_y113.py) to load the transformed data after running the script mentioned in 2. or 3. SQL server can be deployed in GCP or local site. Connection methods for these two scenarios are included in this .py.
    1. [My learning notes for reminders](/learing_notes/learning_notes.md)

5. Run [e_Obs_station_info.py](src/e_Obs_station_info.py) to extract the geometry data of weather observation stations in Taiwan. The observation info will be saved as a new csv file.
    1. [My learning notes for reminders](/learing_notes/learning_notes.md)

6. Run [t_Obs_station_info.py](src/t_Obs_station_info.py) to clean the wanted data and columns. You can save the transformed data as new csv file before push to MySQL server optionally if server is not ready yet. When doing this, please directly run the script alone (i.e., make sure `__name__` = `__main__`").

7. Run [l_Obs_station_info.py](src/l_Obs_station_info.py) to load the transformed geometry info of observation stations to MySQL server.
    1. [My learning notes for reminders](/learing_notes/learning_notes.md)

8. Run [t_find_nearest_Obs_station.py](src/t_find_nearest_Obs_station.py) to find the nearest observation station at each accident location. In this srcipt, Geopandas module is imported to perform the spatial join. It finally returns a new pd.DataFrame with two columns:
        (1) 'accident_id': Matched accident identifiers.
        (2) 'Station_ID': Nearest observation station ID for each accident.
        (3) 'distances': Distances between the station and the accident location.
    1. [My learning notes for reminders](/learing_notes/learning_notes.md)

9. Import the variable defined in t_find_nearest_Obs_station.py , and then run [l_find_nearest_Obs_station.py](src/l_find_nearest_Obs_station.py) to load the table describing relation btw station_Id and nearest_Obs_id to the SQL server (either Local or GCP ).
    1. [My learning notes for reminders](/learing_notes/learning_notes.md)

10. Run [e_find_accidentday_weather_data.py](src/e_find_accidentday_weather_data.py) to check how many weather data should be extract from the GOV-CODis website a weather data open platform (https://codis.cwa.gov.tw/StationData). In this script, must import the self-definded crawerling function on GOV-CODis website a weather data open platform, from the srcipt, [e_crawerling_func_weather_data.py](src/e_crawerling_func_weather_data.py) where use Selenium module.
    ## 待解決、討論問題：
    1. 實作結果爬取1500個csv (前提是：一個觀測站一天有自己一份csv，1500場車禍幾乎需要下載1100-1500個csv)，需要耗時3.5小時。需要仰賴自動排程與雲端服務。
    2. 這裡特別在爬蟲完成時會自動呼叫一個整理資料夾的函式來收尾，把收集了大量csv的資料夾進行整理，整理後下載地點的資料夾結構會轉變為csvpool/dt=某年某月某日/.站別-某年某月某日.csv。 此舉用意留為評估是否要用Bigquery的分區概念存放這些csv，將查詢速度達到提高，而dt=....的資料夾架構是Bigquery的要求。 此外也可以開始思考數據血緣的管理工具。
    3. 如果遇到Webdriver-manager下載的Chromedriver沒有跟上chrome版本 -> [My learning notes for reminders](/learing_notes/learning_notes.md)

11. Run [l_find_accidentday_weather_data.py](src/l_find_accidentday_weather_data.py) to concatenate the dataframe of all observation stations with the same obervation date. You can optionally save as CSV file (just recommended for development and test). At the end of the script, the concatenated result will be load into MySQL server (or Bigqurey data warehouse see the note in 第10點)。