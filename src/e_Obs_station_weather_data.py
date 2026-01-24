from selenium import webdriver  # 引入瀏覽器驅動程式主控制器webdriver
from selenium.webdriver.chrome.service import Service  # ChromeDriver服務管理器
# 自動下載與 Chrome 版本匹配的 ChromeDriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options  # Chrome 瀏覽器啟動參數設定
from selenium.webdriver.common.by import By  # 網頁元素定位
from selenium.webdriver.support.ui import WebDriverWait  # 等待元素出現/可點擊
from selenium.webdriver.support import expected_conditions as EC  # 等待條件判斷
import time  # 延遲執行，確保網頁載入與元素穩定
from pathlib import Path  # 設定檔案路徑為Path物件

import calendar  # 生成合法日曆日期，避免選到不存在的日期


def crawler_CODis_to_dowload_data(station_id: str, target_year: int, target_month: int,
                                  target_monthday: int, max_retry_time=3) -> None:
    """This function featuring crawlering from the website CODis by the 
    following actions: Get in the web page, 
    select the default observation station classes(自動雨量站、自動氣象站、署屬有人站), 
    find and download the weather data of certain station ID and datetime that should be given
    by the arguments. 
    If any exception is raised during the process, you can choose the maximal
    retry times. Otherwise, the default is THREE times."""

    print(
        f"---正在找尋並下載{target_year}-{target_month:02}-{target_monthday:02}的天氣資料---")
    for attempt in range(max_retry_time):
        print(f"\t第{attempt+1}次嘗試中.....")
        try:
            # Step 5-1: 進入網站
            driver.get("https://codis.cwa.gov.tw/StationData")

            # Step 5-2: 勾選自動雨量站
            # 用find_element()+click()也可以，但是精準度稍低，經實測結果大約150次點擊中有機率會失敗5次。
            stationC1_input = wait.until(
                EC.element_to_be_clickable((By.ID, "auto_C1")))
            driver.execute_script("arguments[0].click();", stationC1_input)

            # Step 5-3: 勾選自動氣象站
            stationC2_input = driver.find_element(By.ID, "auto_C0")
            stationC2_input.click()

            # Step 5-4: 點開測站清單
            station_lst_btn = driver.find_element(
                By.CSS_SELECTOR, "#switch_display > button:nth-child(2)")
            station_lst_btn.click()
            time.sleep(2)

            # Step 5-5: 找到測站坐落在哪一個資料列，如果找到了，用Selenium driver定位該列最右側趨勢圖icon，並點擊

            # 使用穿透層級語法：//*[contains(text(), '期望該標籤內容帶有什麼關鍵字')]，這會向內掃描標籤下所有內容，即便按鈕被包得太深找得到。
            # //tr[...]//div[...] 代表在該列底下的任何層級尋找 div
            xpath = f"//tr[.//div[contains(text(), {station_id})]]//div[i[contains(@class, 'fa-chart-line')]]"
            chartbtn = wait.until(
                EC.element_to_be_clickable((By.XPATH, xpath)))
            driver.execute_script("arguments[0].click();", chartbtn)
            print("\t點擊chart鈕成功")
            time.sleep(2)

            crawler_CODis_select_download(
                target_year, target_month, target_monthday)
            time.sleep(3)
            return None

        except Exception as e:
            print(f"Error發生: {e}")
            print(
                f"\t下載失敗，無法下載到{target_year}-{target_month:02}-{target_monthday:02}。")
            if attempt == max_retry_time - 1:
                print(f"---嘗試{max_retry_time}次仍下載失敗，請做問題排解。---")
            else:
                print(f"\t將嘗試第{attempt+2}次.......")
    return None


def crawler_CODis_select_download(target_year: int, target_month: int, target_monthday: int) -> None:
    """This function is response for the download action following the function 
    crawler_CODis_to_dowload_data() which focuses on visiting the specific station chart
    and controlling the attempt times if exception occurs."""

    # Step 5-6: 點開日期選單
    date_input_btn = wait.until(EC.element_to_be_clickable(
        (By.XPATH, '//*[@id="main_content"]/section[2]/div/div/section/div[5]/div[1]/div[1]/label/div/div[2]/div[1]/input')))
    driver.execute_script("arguments[0].click()", date_input_btn)
    print("\t成功點開日期選單")

    # Step 5-7: 點開年份下拉式選單
    y_menu_xpath = "//div[contains(@class, 'vdatetime-popup__year')]"
    y_menu_btn = wait.until(
        EC.element_to_be_clickable((By.XPATH, y_menu_xpath)))
    driver.execute_script("arguments[0].click()", y_menu_btn)
    print("\t成功點開年份下拉式選單")

    # Step 5-8: 選擇年份
    y_xpath = f"//div[contains(@class, 'vdatetime-year-picker__item') and contains(text(), '{target_year}')]"
    year_select = wait.until(
        EC.element_to_be_clickable((By.XPATH, y_xpath)))
    driver.execute_script("arguments[0].click()", year_select)
    print(f"\t成功選到{target_year}年")

    # Step 5-9: 點開月日下拉式選單
    # 以下兩種xpath都定位得到。
    # submenu_xpath = '//*[@id="main_content"]/section[2]/div/div/section/div[5]/div[1]/div[1]/label/div/div[2]/div[1]/div/div[2]/div[1]/div[2]'
    month_date_xpath = "//div[contains(@class, 'vdatetime-popup__date')]"
    month_date_btn = wait.until(EC.element_to_be_clickable(
        (By.XPATH, month_date_xpath)))
    driver.execute_script("arguments[0].click()", month_date_btn)
    print(f"\t成功點開月日下拉式選單")

    # Step 5-10: 選擇月份
    m_xpath = f"//div[contains(@class, 'vdatetime-month-picker__item') and contains(text(), '{target_month}月')]"
    month_select = wait.until(
        EC.element_to_be_clickable((By.XPATH, m_xpath)))
    driver.execute_script("arguments[0].click()", month_select)
    print(f"\t成功選到{target_month}月")

    # Step 5-11: 選擇日期
    md_xpath = f"//div[contains(@class, 'vdatetime-calendar__month__day')]//span[contains(text(), '{target_monthday}')]"
    monthday_select = wait.until(
        EC.element_to_be_clickable((By.XPATH, md_xpath)))
    driver.execute_script("arguments[0].click()", monthday_select)
    print(f"\t成功選到{target_monthday}日")
    time.sleep(1)

    # step 5-12: 觸發下載
    csv_btn_xpath = '//*[@id="main_content"]/section[2]/div/div/section/div[5]/div[1]/div[2]/div'
    csv_btn = wait.until(
        EC.element_to_be_clickable((By.XPATH, csv_btn_xpath)))
    time.sleep(1)
    driver.execute_script("arguments[0].click()", csv_btn)
    print(f"\t下載成功!")


if __name__ == '__main__':

    #  Step 1: 設定未來存檔資料夾路徑
    curr_dir = Path().resolve()
    save_path = curr_dir/"supplementary_weather_csv_from_CODiS"
    save_path.mkdir(parents=True, exist_ok=True)

    #  Step 2: 建立service & options物件
    service = Service(ChromeDriverManager().install())
    options = Options()

    #  Step 3: 建立下載路徑
    prefs = {"download.default_directory": str(save_path)}
    options.add_experimental_option("prefs", prefs)

    #  Step 4: options備妥，可建立瀏覽器控制物件，並啟動瀏覽器。
    driver = webdriver.Chrome(service=service, options=options)
    driver.maximize_window()
    wait = WebDriverWait(driver, 10)

    # Step 5: 指定觀測站與觀測年月日，先以一個測站的一年的數據試運轉
    station_id = "467410"  # 臺南觀測站
    target_year = 2025
    start_month = 12
    end_month = 12

    for m in range(start_month, end_month+1):
        calendar_a_month = calendar.monthcalendar(
            target_year, m)  # list of lists
        for week in calendar_a_month:
            for d in week:
                if d != 0:
                    crawler_CODis_to_dowload_data(
                        station_id, target_year, m, d)
    driver.close()
