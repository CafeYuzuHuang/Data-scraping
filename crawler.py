# -*- coding: utf-8 -*-
# Date: Jun. 16, 2021
# Author: YuzuHuang


from shutil import copy
import os
from time import sleep
import datetime as dt
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import pandas as pd
from plotly.offline import plot # 使用Spyder執行需要使用offline模式
from plotly.subplots import make_subplots
import plotly.graph_objects as go


# ---< Global variables >---
if __name__ == "__main__": WD_Path = os.getcwd() # executed
else: WD_Path = os.path.dirname(__file__) # imported
WD_Path += "\\chromedriver.exe" # 使用的Webdriver所在路徑


# ---< Utility functions >---
def ValidateDate(d):
    """ 驗證日期格式，並擷取出年月日後以列表回傳 """
    try:
        isod = dt.date.fromisoformat(d)
        yyyy, mm, dd = str(isod.year), str(isod.month).zfill(2), \
            str(isod.day).zfill(2) # padding by zeros
    except ValueError as ve:
        print(ve)
        return []
    except TypeError as te:
        print(te)
        return []
    else:
        return [yyyy, mm, dd]


def ValidateUrl(url):
    """ 驗證url有效性，url中必須包含指定的域名與關鍵字，回傳的布林值為驗證結果 """
    p = urlparse(url)
    print(p)
    # 確認域名，以及路徑是否包含關鍵字詞
    # 在此路徑開頭必須是/indices，且包含historical-data關鍵字
    if p.netloc == "www.investing.com" and p.path.find("/indices") == 0 and \
        p.path.find("historical-data") != -1:
        return True
    else:
        return False


def GetName(url):
    """ 從網址中取得標的名稱後回傳，之後用於檔案命名 """
    n1 = url.find("/indices/") + len("/indices/")
    n2 = url.find("-historical-data")    
    return url[n1:n2]


# ---< Functions called by Main() >---
def FetchData(url, d1, d2):
    """
    從Investing抓取資料，不同的標的對應提供不同url，日期範圍介於d1與d2之間
    抓取結果寫入DataFrame變數data，再存成csv檔並回傳
    """
    ymd_1 = ValidateDate(d1)
    ymd_2 = ValidateDate(d2)
    try:
        assert len(ymd_1) > 0 and len(ymd_2) > 0
    except Exception:
        print("Invalid date(s)!")
        return None
    try:
        assert ValidateUrl(url)
    except Exception:
        print("Invalid url!")
        return None
    try:
        # PhantomJS support has been depreciated, so we use Chrome instead
        options = webdriver.ChromeOptions()
        options.add_argument('--headless') # Chrome瀏覽器開啟無頭模式
        options.add_argument("--disable-extensions")
        driver = webdriver.Chrome(options = options, 
                                  executable_path = WD_Path)
        driver.implicitly_wait(3)
        driver.get(url)
    except Exception as e:
        print(e)
        driver.close()
        return None
    try:
        wait = WebDriverWait(driver, 10, 0.5)
        target = wait.until(EC.presence_of_element_located\
                            ((By.ID, "widgetFieldDateRange")))
        print("Page is ready!")
        # 選擇日期範圍：所需格視為 mm/dd/yyyy
        input_d1 = ymd_1[1] + '/' + ymd_1[2] + '/' + ymd_1[0]
        input_d2 = ymd_2[1] + '/' + ymd_2[2] + '/' + ymd_2[0] 
        # 頁面捲到適當位置後點擊打開日曆
        driver.execute_script('arguments[0].scrollIntoView(true);', target)
        driver.execute_script('scrollBy(0, -200);') # Scroll up
        sleep(2)
        target.click() # expand the wrapped calender
        sleep(2)
        # 清除預設值後填入新值
        s_d = driver.find_element_by_id("startDate")
        e_d = driver.find_element_by_id("endDate")
        sleep(1)
        s_d.clear()
        e_d.clear()
        sleep(1)
        s_d.send_keys(input_d1)
        e_d.send_keys(input_d2)
        sleep(1)
        # 按下apply按鍵送出
        appbtn = driver.find_element_by_id("applyBtn")
        sleep(1)
        appbtn.click()
        sleep(5) # 若搜尋範圍很大(例如10年)，則需要等待較長的秒數
        # 抓取資料：最快的方法是使用pandas讀取網頁中的表格，並轉換成DataFrame
        # 因已毋須操作，之後只需要讀取靜態資料
        table = pd.read_html(driver.page_source, 
                             attrs = {"id": "curr_table"}, 
                             parse_dates = True, 
                             index_col = 0)
        print("Show # of rows: ", table[0].shape[0])
        print("Show # of columns: ", table[0].shape[1])
        print("Show first 5 rows:")
        print(table[0].head())
    except TimeoutException as toe:
        print(toe)
    except Exception as e:
        print(e)
        return None
    finally:
        driver.close()
    return table[0]


def PrepData(df0, dpath, dname):
    """ 預處理數據，包含資料排序/格式整理，以及與本地資料合併。回傳預處理後的DataFrame """
    df = df0.copy()
    df.index = pd.to_datetime(df.index) # 日期資料格式轉換
    df = df.iloc[::-1] # reversed order
    # 將漲跌幅原本的文字格式'x.x%'轉換成浮點數x.x
    df.iloc[:,5] = df.iloc[:,5].apply(lambda x: float(x[:-2]))
    print("Show first 5 rows:")
    print(df.head())
    
    flist = os.listdir(dpath)
    for ff in flist:
        ffext = os.path.splitext(ff)[-1]
        ffname = os.path.splitext(ff)[0]
        if ffext.lower() == '.csv' and ffname.lower() == dname.lower():
            print("%s is existed in the current dir. Merge them:" % dname)
            copy(dpath + '\\' + ff, 
                 dpath + '\\' + dname + "_bk.csv") # backup
            df_his = pd.read_csv(dpath + '\\' + ff, 
                                 delimiter = ',', 
                                 index_col = 0, 
                                 header = 0, 
                                 skiprows = None)
            df_his.index = pd.to_datetime(df_his.index) # 日期資料格式轉換
            # 將舊資料整併：以下簡單比較日期範圍
            if df.index[0] < df_his.index[-1]: # overlapped
                df = pd.concat([df_his, df], axis = 0)
                df.drop_duplicates(inplace = True, keep = "last")
                # 檔案覆寫
                df.to_csv(dpath + '\\' + ff, 
                          sep = ',', header = True, index = True)
                return df
    print("%s is a new file in the current dir." % dname)
    df.to_csv(dpath + '\\' + dname + ".csv", 
              sep = ',', header = True, index = True)
    return df

        
def VisData(data, dname):
    """ 使用Plotly建立互動式視窗 """
    fig = make_subplots(rows = 2, cols = 1, shared_xaxes = True, 
                        vertical_spacing = 0.04, row_heights = [3, 1])
    # 繪製K線圖
    fig.add_trace(go.Candlestick(x = data.index, 
                                 open = data.iloc[:, 1], 
                                 high = data.iloc[:, 2], 
                                 low = data.iloc[:, 3], 
                                 close = data.iloc[:, 0], 
                                 increasing_line_color = "red", 
                                 decreasing_line_color = 'green', 
                                 name = "K線圖"), 
                  row = 1, col = 1)
    # 繪製漲跌幅：上漲和下跌使用不同顏色
    xup = data.index[data.iloc[:, 5] >= 0].copy()
    yup = data.iloc[:, 5][data.iloc[:, 5] >= 0].copy()
    xdown = data.index[data.iloc[:, 5] < 0].copy()
    ydown = data.iloc[:, 5][data.iloc[:, 5] < 0].copy()
    fig.add_trace(go.Bar(x = xup, y = yup, marker_color = "red", 
                         name = "漲幅"), 
                  row = 2, col = 1)
    fig.add_trace(go.Bar(x = xdown, y = ydown, marker_color = "green", 
                         name = "跌幅"), 
                  row = 2, col = 1)
    # 設定標題、圖片尺寸、樣板、以及定義縮放範圍的按鈕
    fig.update_layout(title_text = dname, 
                      height = 600, 
                      width = 800, 
                      template = "plotly", 
                      xaxis = dict(
                          rangeselector = dict(
                              buttons = [
                                  dict(count = 3,
                                       label = "3m", 
                                       step = "month",
                                       stepmode = "backward"),
                                  dict(count = 6,
                                       label = "6m",
                                       step = "month", 
                                       stepmode = "backward"),
                                  dict(count = 1,
                                       label = "YTD",
                                       step = "year",
                                       stepmode = "todate"),
                                  dict(count = 1,
                                       label = "1y",
                                       step = "year",
                                       stepmode = "backward"),
                                  dict(step = "all", label = "MAX")]), 
                          type = "date"), 
                      xaxis_rangeslider_visible = False, 
                      xaxis2 = dict(type = "date", title = "日期"), 
                      xaxis2_rangeslider_visible = False, 
                      yaxis = dict(title = "數值或價格(USD)"), 
                      yaxis2 = dict(title = "漲跌幅(%)"))
    plot(fig)
    return None


# ---< Main >---
def Main(url, dst_path, start_date, \
         end_date = dt.date.isoformat(dt.date.today())):
    """ 主函式：擷取資料→資料前處理→作圖 """
    rawdata = FetchData(url, start_date, end_date)
    name = GetName(url)
    prepdata = PrepData(rawdata, dst_path, name)
    VisData(prepdata, name)
    # return None


# ---< Integrated function test >---
if __name__ == "__main__":
    t_start = dt.datetime.now()
    url_vxn = "https://www.investing.com/indices/cboe-nasdaq-100-voltility-historical-data"
    sd = dt.date.isoformat(dt.date.today() - dt.timedelta(days = 601))
    ed = dt.date.isoformat(dt.date.today() - dt.timedelta(days = 1))
    Main(url = url_vxn, dst_path = os.getcwd(), start_date = sd, end_date = ed)
    
    # url_dxy = "https://www.investing.com/indices/usdollar-historical-data"
    # end_date 使用預設值，即今日
    # Main(url = url_dxy, dst_path = os.getcwd(), start_date = sd,)
    
    t_end = dt.datetime.now()
    print("Total ellapsed time is: ", t_end - t_start) # 約需25至30秒
# Done
