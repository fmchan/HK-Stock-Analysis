import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import math
import time

from configs.settings import ALLOW_OVERWRITE, SECOND_TIME_OUT, options, WEBDRIVER_PATH, SCRAP_PAUSE_TIME, DATA_PATH, DB_PATH
# from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium import webdriver
import os
import sys

try:
    from urllib import FancyURLopener
except:
    from urllib.request import FancyURLopener
from models.income import Income
from dbhelper import DBHelper

from datetime import datetime, timedelta
import sqlite3
import logging
from datautils import data_mapper
from datautils import pattern_utils

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def getIndexes(dfObj, value): 
    listOfPos = [] # Empty list 

    # isin() method will return a dataframe with boolean values, True at the positions where element exists 
    result = dfObj.isin([value]) #df.isin(['Total Revenue'])

    # any() method will return a boolean series 
    seriesObj = result.any() #if there is a True anywhere in the dataframe this returns True

    # Get list of column names where element exists 
    columnNames = list(seriesObj[seriesObj == True].index) #index gives me the column name where there is a True

    # Iterate over the list of columns and extract the row index where element exists 
    for col in columnNames: 
        rows = list(result[col][result[col] == True].index) 

        for row in rows: 
            listOfPos.append((row, col)) 
  
    # This list contains a list tuples with the index of element in the dataframe 
    return listOfPos

def scrap_aa_fundamentals(sid, is_annual):
    logger = logging.getLogger('MainLogger')
    result_df = pd.DataFrame()
    aacode = sid.split(".")[0].zfill(5)
    content = None
    no_error = True
    full_url = f"http://www.aastocks.com/en/stocks/analysis/company-fundamental/earnings-summary?symbol={aacode}"

    if is_annual:
        full_url = f"http://www.aastocks.com/en/stocks/analysis/company-fundamental/earnings-summary?symbol={aacode}&period=4"
    else:
        full_url = f"http://www.aastocks.com/en/stocks/analysis/company-fundamental/earnings-summary?symbol={aacode}&period=2"
    try:
        response = requests.get(full_url)
        if response.status_code != 200:
            logger.error("response is not 200")
            no_error = False
        content = response.text

        # driver = webdriver.Chrome(executable_path = WEBDRIVER_PATH, chrome_options = options)
        # driver.get(full_url)
        # try:
        #     WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, f"//select/option[text()='Interim']")))
        #     if not is_annual:
        #         driver.find_element_by_xpath("//select/option[text()='Interim']").click()
        # except Exception as e:
        #     exc_type, exc_obj, exc_tb = sys.exc_info()
        #     fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        #     logger.exception(exc_type, fname, exc_tb.tb_lineno)
        #     no_error = False
        # content = driver.page_source
        # driver.close()

        logger.info(full_url)
        if content and no_error:
            soup = BeautifulSoup(content, "html.parser")
            result_statements = soup.find(id = "cnhk-list")
            # print(result_statements.prettify()) # to show html result
            result_statements = result_statements.find_all(attrs={"ref-2nd" : ["date", "0"]})
            # print(result_statements)

            work = []
            for result_statement in result_statements:
                for string in result_statement.strings:
                    if string.strip():
                        work.append((string.strip()))
            #    print(repr(string))

            work = pd.DataFrame(work, columns=["info"]) #list to pandas
            # print(len(work+"rows"))
            df = work #renaming it for function below
            logger.info(len(df))
            if len(df) > 1:
                # print(df)
                df = df.replace(r'\n', '', regex=True)
                df = df.replace(r'\r', '', regex=True)
                # print(df)
                first_period = df.iloc[1].values[0]
                logger.info(f"first_period: {first_period}")
                currency_index = df[df["info"].str.contains("Currency")].index.values[0] + 1
                currency = df.iloc[currency_index].values[0]
                logger.info(f"currency: {currency}")
                df1 = df.astype(str).apply(pd.to_datetime, format='%Y/%m', errors='coerce')
                idx = df.index[df["info"]==first_period]
                start_index = idx.values[0]
                logger.info(f"start_index: {start_index}")
                # period_group = df1[~df1["info"].isnull()].groupby("info").groups
                # logger.info(period_group)
                # logger.info(type(period_group))
                # total_rows = len(period_group) + 1
                total_rows = len(df1[~df1["info"].isnull()]) + 1
                logger.info(f"total_rows: {total_rows}")

                # listOfPositions = getIndexes(df, "\nNet Profit (Mn)\n")
                listOfPositions = getIndexes(df, "Net Profit (Mn)")

                pos = listOfPositions[0][:1]
                pos = pos[0]

                work2 = work.drop(work.index[0:pos]).reset_index(drop=True) #drop the first several rows of useless data
                # print(work2)

                work3 = work2[work2.index % total_rows == 0].reset_index(drop=True) #return every per value and start a zero
                work3.rename(columns={work.columns[0]:"Title"}, inplace=True) #rename the first and only column 
                # print(work3)

                for i in range(1, total_rows):
                    work3[df.iloc[start_index]] = work2[work2.index % total_rows == i].reset_index(drop=True)
                    start_index += 1
                # print(work3)
                # print(type(work3))
                result_df = work3.T
                # print(result_df)
                if len(result_df) > 0:
                    result_df = result_df.rename(columns=result_df.iloc[0]).drop(result_df.index[0])
                    result_df = result_df.rename_axis("period").reset_index()
                    result_df["sid"] = sid
                    result_df["currency"] = currency
                    result_df["year"] = result_df["period"].str[0:4]
                    if is_annual:
                        result_df["frequency"] = "annual"
                    else:
                        result_df["frequency"] = "quarterly"
                result_df["Net Profit (Mn)"] = result_df["Net Profit (Mn)"].str.replace(',', '').astype(float)
                result_df["Net Profit Growth (%)"] = result_df["Net Profit Growth (%)"].astype(str)
                result_df["EPS Growth (%)"] = result_df["EPS Growth (%)"].astype(str)
                result_df["Earnings Per Share"] = result_df["Earnings Per Share"].str.replace(',', '').astype(float)
                result_df["Dividend Per Share"] = result_df["Dividend Per Share"].str.replace(',', '').astype(float)
                # logger.info(result_df)
            else:
                logger.error("no table is found")
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.exception(exc_type, fname, exc_tb.tb_lineno)
        logger.exception(e)
        no_error = False
        result_df = pd.DataFrame() # to rollback to empty dataframe once error found
    return result_df

def update_all_stocks(provider, market):
    logger = logging.getLogger('MainLogger')
    annual_fail_stock_list = []
    quarterly_fail_stock_list = []
    cnx = sqlite3.connect(DB_PATH)
    cnx.execute("PRAGMA journal_mode=WAL")

    present_ticker_ids = pd.read_sql("SELECT DISTINCT sid FROM stocks WHERE provider = '{}' and market = '{}'".format(provider, market), cnx)
    logger.info(present_ticker_ids)
    for sid in present_ticker_ids['sid']:
        annual_df = scrap_aa_fundamentals(sid, is_annual=True)
        if len(annual_df) < 1:
            annual_fail_stock_list.append(sid)
        else:
            data_mapper.map_aadf_to_model(annual_df)

        quarterly_df = scrap_aa_fundamentals(sid, is_annual=False)
        if len(quarterly_df) < 1:
            quarterly_fail_stock_list.append(sid)
        else:
            data_mapper.map_aadf_to_model(quarterly_df)
    logger.info(annual_fail_stock_list)
    logger.info(quarterly_fail_stock_list)
    _retry_fails(annual_fail_stock_list, quarterly_fail_stock_list)

def _retry_fails(annual_fail_stock_list, quarterly_fail_stock_list):
    start = time.time()
    consuming_time = 0

    for sid in annual_fail_stock_list:
        annual_df = scrap_aa_fundamentals(sid, is_annual=True)
        if len(annual_df) > 1:
            data_mapper.map_aadf_to_model(annual_df)
            annual_fail_stock_list.remove(sid)
    if len(annual_fail_stock_list) > 0:
        annual_fail_stock_list_df = pd.DataFrame(annual_fail_stock_list, columns=['sid'])
        annual_fail_stock_list_df.to_csv(DATA_PATH + 'fail_annual_fundamentals_list.csv', index=False)

    for sid in quarterly_fail_stock_list:
        quarterly_df = scrap_aa_fundamentals(sid, is_annual=False)
        if len(quarterly_df) > 1:
            data_mapper.map_aadf_to_model(quarterly_df)
            quarterly_fail_stock_list.remove(sid)
    if len(quarterly_fail_stock_list) > 0:
        quarterly_fail_stock_list_df = pd.DataFrame(quarterly_fail_stock_list, columns=['sid'])
        quarterly_fail_stock_list_df.to_csv(DATA_PATH + 'fail_quarterly_fundamentals_list.csv', index=False)

    export_fundamentals_from_db()
    print("annual_fail_stock_list_df")
    print(annual_fail_stock_list_df)
    print("quarterly_fail_stock_list_df")
    print(quarterly_fail_stock_list_df)
    current_time = time.time()
    consuming_time = current_time - start
    print("consuming_time")
    print(consuming_time)

def export_fundamentals_from_db():
    logger = logging.getLogger('MainLogger')
    cnx = sqlite3.connect(DB_PATH)
    cnx.execute("PRAGMA journal_mode=WAL")
    incomes_df = pd.read_sql("SELECT * FROM incomes where id is not NULL order by sid, frequency, year", cnx)
    # print(incomes_df)
    logger.info(incomes_df.info())
    incomes_df.to_csv(DATA_PATH + 'raw_incomes_df.csv', index=False)

def import_fundamentals_to_db():
    logger = logging.getLogger('MainLogger')
    cnx = sqlite3.connect(DB_PATH)
    cnx.execute("PRAGMA journal_mode=WAL")
    incomes_df = pd.read_csv(DATA_PATH + 'incomes_df.csv', index_col=False)
    logger.info(incomes_df.info())
    incomes_df.to_sql('incomes', cnx, if_exists='replace', index=False)

def export_adjust_fundamentals():
    logger = logging.getLogger('MainLogger')
    incomes_df = pd.DataFrame()
    cnx = sqlite3.connect(DB_PATH)
    cnx.execute("PRAGMA journal_mode=WAL")
    present_ticker_ids = pd.read_sql("SELECT DISTINCT sid FROM stocks WHERE provider = '{}' and market = '{}'".format("YAHOO", "HK"), cnx)
    logger.info(present_ticker_ids)
    raw_incomes_df = pd.read_csv(DATA_PATH + 'raw_incomes_df.csv', index_col=False)
    for sid in present_ticker_ids['sid']:
        income_df = raw_incomes_df[raw_incomes_df["sid"] == sid]
        if len(income_df) > 0:
            annual_income_df = income_df[income_df["frequency"]=="annual"].sort_values(by="year", ascending=True)
            quarterly_income_df = income_df[income_df["frequency"]=="quarterly"].sort_values(by="year", ascending=True)
            if len(annual_income_df) > 0:
                annual_income_df["net_profit_growth"] = np.where(annual_income_df["net_profit_growth"] == '-', pattern_utils.compute_growth(annual_income_df, "net_profit"), annual_income_df["net_profit_growth"])
                annual_income_df["eps_growth"] = np.where(annual_income_df["eps_growth"] == '-', pattern_utils.compute_growth(annual_income_df, "basic_eps"), annual_income_df["eps_growth"])
            if len(quarterly_income_df) > 0:
                quarterly_income_df["net_profit_growth"] = np.where(quarterly_income_df["net_profit_growth"] == '-', pattern_utils.compute_growth(quarterly_income_df, "net_profit"), quarterly_income_df["net_profit_growth"])
                quarterly_income_df["eps_growth"] = np.where(quarterly_income_df["eps_growth"] == '-', pattern_utils.compute_growth(quarterly_income_df, "basic_eps"), quarterly_income_df["eps_growth"])
            incomes_df = pd.concat([incomes_df, annual_income_df, quarterly_income_df], ignore_index=True)
            # logger.info(incomes_df)
    incomes_df["net_profit_growth"] = incomes_df["net_profit_growth"].apply(pattern_utils.value_to_float)
    incomes_df["eps_growth"] = incomes_df["eps_growth"].apply(pattern_utils.value_to_float)
    incomes_df["month"] = incomes_df["period"].str[5:7]
    incomes_df["net_profit_growth"] = incomes_df["net_profit_growth"].astype(float)
    incomes_df["eps_growth"] = incomes_df["eps_growth"].astype(float)
    incomes_df["net_profit"] = incomes_df["net_profit"].astype(float)
    incomes_df.fillna(0, inplace=True)
    logger.info(incomes_df)
    incomes_df.to_csv(DATA_PATH + 'incomes_df.csv', index=False)
