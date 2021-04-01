from pandas_datareader import data
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys
from configs.settings import DATA_PATH, DB_PATH, FAIL_STOCK_FILE, CPU_COUNT, DB_TIMEOUT
from dbhelper import DBHelper
import sqlite3
from configs.base import Session, engine, Base
import logging
import time
import multiprocessing as mp

def get_hk_stocks_df():
    stocks_df = pd.read_csv(DATA_PATH + 'stock_list.csv', index_col=False)
    return get_stocks_core(stocks_df)

def get_failed_hk_stocks_df():
    stocks_df = pd.read_csv(DATA_PATH + 'fail_stock_list.csv', index_col=False)
    return get_stocks_core(stocks_df)

def get_stocks_core(stocks_df):
    stocks_df['sid'] = stocks_df['sid'].astype(str)
    stocks_df['sid'] = stocks_df['sid'].apply(lambda x: x.zfill(4))
    stocks_df.drop_duplicates(subset='sid', keep="last", inplace=True)
    return stocks_df

def get_hist_stock_price(sid, start_date='2001-07-15', provider='YAHOO', market='HK'):
    try:
        logger = logging.getLogger('MainLogger')
        today_date = datetime.strftime(datetime.now(), '%Y-%m-%d')
        df = pd.DataFrame()
        temp_table_name = "temp_" + sid.replace('.', '_') + "_table"
        if provider == 'YAHOO':
            df = data.get_data_yahoo(sid, start=start_date, end=today_date, retry_count=3, pause=1)
        if len(df) > 0:
            df['date'] = pd.to_datetime(df.index)
            df.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume', 'Adj Close': 'adj_close'}, inplace=True)
            df['provider'] = provider
            df['market'] = market
            df['sid'] = sid
            with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as cnx:
                cnx.execute("PRAGMA journal_mode=WAL")
                # logger.info('before inserting to temp {}'.format(sid))
                df.to_sql(temp_table_name, cnx, if_exists='replace', index=False)
                logger.info('before inserting to stocks {}'.format(sid))
                insert_sql = f"""
                    INSERT OR IGNORE INTO stocks (provider, market, sid, date, open, high, low, close, volume, adj_close) 
                        SELECT provider, market, sid, date, open, high, low, close, volume, adj_close 
                        FROM {temp_table_name} t
                        WHERE NOT EXISTS 
                            (SELECT 1 FROM stocks f
                            WHERE t.sid = f.sid
                            AND t.provider = f.provider
                            AND t.date = f.date)
                """
                cnx.execute(insert_sql)
                cnx.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.exception(exc_type, fname, exc_tb.tb_lineno)
        logger.exception(e)
        logger.error('failed: {}'.format(sid))
        try:
            with sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT) as cnx:
                cnx.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
        except Exception as e:
            return pd.DataFrame()
        return pd.DataFrame()
    logger.info('success: {}'.format(sid))
    return df

def update_all_stocks(provider, market, delta=1000, lower_limit=1, expire_mins=5):
    logger = logging.getLogger('MainLogger')
    fail_stock_list = []
    present_ticker_ids = []

    with sqlite3.connect(DB_PATH) as cnx:
        cnx.execute("PRAGMA journal_mode=WAL")
        present_ticker_ids = pd.read_sql("SELECT DISTINCT sid FROM stocks WHERE provider = '{}' and market = '{}'".format(provider, market), cnx)
    logger.info(present_ticker_ids)

    logger.info("start multiprocessing")
    with mp.Pool(processes=CPU_COUNT) as pool:
        results = [pool.apply_async(_get_delta_stock_price, args=[sid, delta, lower_limit]) for sid in present_ticker_ids['sid']]
        for result in results:
            fail_sid = result.get()
            if fail_sid:
                fail_stock_list.append(fail_sid)
    logger.info(fail_stock_list)
    _retry_fails(fail_stock_list, expire_mins, delta)

def insert_all_stocks(delta=1000, lower_limit=1, expire_mins=5):
    logger = logging.getLogger('MainLogger')
    stocks_df = get_hk_stocks_df()
    fail_stock_list = []

    logger.info("start multiprocessing")
    with mp.Pool(processes=CPU_COUNT) as pool:
        results = [pool.apply_async(_get_delta_stock_price, args=[str(stock_df['sid']).zfill(4) + '.HK', delta, lower_limit]) for index, stock_df in stocks_df.iterrows()]
        for result in results:
            fail_sid = result.get()
            if fail_sid:
                fail_stock_list.append(fail_sid)
    logger.info(fail_stock_list)
    _retry_fails(fail_stock_list, expire_mins, delta)

def _get_delta_stock_price(sid, delta, lower_limit):
    logger = logging.getLogger('MainLogger')
    dates = ""
    with sqlite3.connect(DB_PATH) as cnx:
        cnx.execute("PRAGMA journal_mode=WAL")
        sql = "SELECT date FROM stocks WHERE sid='{}'".format(sid)
        dates = pd.read_sql(sql, cnx)

    if dates.empty:
        last_date = datetime.strftime(datetime.now() - timedelta(delta), '%Y-%m-%d')
    else:
        last_date = dates.iloc[-1, 0]

    if last_date[0:10] == datetime.strftime(datetime.now(), '%Y-%m-%d'):
        logger.info('bypassing updating: {} from {}'.format(sid, str(last_date)))    
    else:
        logger.info('processing updating: {} from {}'.format(sid, str(last_date)))
        df = get_hist_stock_price(sid, start_date=last_date)
        if len(df) < lower_limit:
            return sid
    return None

def _retry_fails(fail_stock_list, expire_mins, delta):
    logger = logging.getLogger('MainLogger')
    start = time.time()
    consuming_time = 0
    expire_seconds = expire_mins * 60
    if len(fail_stock_list) > 0:
        fail_stock_list_df = pd.DataFrame(fail_stock_list, columns=['sid'])
        fail_stock_list_df.to_csv(DATA_PATH + FAIL_STOCK_FILE, index=False)
    while consuming_time < expire_seconds:
        for sid in fail_stock_list:
            with sqlite3.connect(DB_PATH) as cnx:
                cnx.execute("PRAGMA journal_mode=WAL")
                sql = "SELECT date FROM stocks WHERE sid='{}'".format(sid)
                dates = pd.read_sql(sql, cnx)
            if dates.empty:
                start_date = datetime.strftime(datetime.now() - timedelta(delta), '%Y-%m-%d')
            else:
                start_date = dates.iloc[-1, 0]

            df = get_hist_stock_price(sid, start_date=start_date)
            if len(df) > 10:
                fail_stock_list.remove(sid)
        time.sleep(10)
        logger.info(fail_stock_list)
        if len(fail_stock_list) > 0:
            fail_stock_list_df = pd.DataFrame(fail_stock_list, columns=['sid'])
            fail_stock_list_df.to_csv(DATA_PATH + FAIL_STOCK_FILE, index=False)
        else:
            break
        consuming_time = time.time() - start
        logger.info(consuming_time)
        logger.info(expire_seconds)

def update_failed_stocks(provider, market, delta=1000, expire_mins=5):
    logger = logging.getLogger('MainLogger')
    fail_stock_list = []

    stocks_df = get_failed_hk_stocks_df()
    for index, stock_df in stocks_df.iterrows():
        sid = str(stock_df['sid'])
        fail_stock_list.append(sid)

    logger.info(fail_stock_list)
    _retry_fails(fail_stock_list, expire_mins, delta)