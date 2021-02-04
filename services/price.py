from pandas_datareader import data
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys
from configs.settings import DATA_PATH
from dbhelper import DBHelper
from configs.settings import DB_PATH
import sqlite3
from configs.base import Session, engine, Base
import logging
import time

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
        if provider == 'YAHOO':
            df = data.get_data_yahoo(sid, start=start_date, end=today_date, retry_count=3, pause=1)
        if len(df) > 0:
            df['date'] = pd.to_datetime(df.index)
            df.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume', 'Adj Close': 'adj_close'}, inplace=True)
            cnx = sqlite3.connect(DB_PATH)
            cnx.execute("PRAGMA journal_mode=WAL")
            df['provider'] = provider
            df['market'] = market
            df['sid'] = sid
            # logger.info('before inserting to temp {}'.format(sid))
            df.to_sql('temporary_stocks', cnx, if_exists='replace', index=False)
            logger.info('before inserting to stocks {}'.format(sid))
            # df.to_sql('stocks', cnx, if_exists='append', index=False)
            with engine.begin() as cnx:
                insert_sql = f"""
                    INSERT OR IGNORE INTO stocks (provider, market, sid, date, open, high, low, close, volume, adj_close) 
                        SELECT provider, market, sid, date, open, high, low, close, volume, adj_close 
                        FROM temporary_stocks t
                        WHERE NOT EXISTS 
                            (SELECT 1 FROM stocks f
                            WHERE t.sid = f.sid
                            AND t.provider = f.provider
                            AND t.date = f.date)
                """
                cnx.execute(insert_sql)
            # logger.info('after inserting to stocks {}'.format(sid))
            # db = DBHelper()
            # for i, row in df.iterrows():
            #     date = row['date']
            #     open = row['open']
            #     high = row['high']
            #     low = row['low']
            #     close = row['close']
            #     volume = row['volume']
            #     adj_close = row['adj_close']
            #     db.insert_stock(provider, market, sid, date, open, high, low, close, volume, adj_close)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(exc_type, fname, exc_tb.tb_lineno)
        logger.error('failed: {}'.format(sid))
        return pd.DataFrame()
    return df

def update_failed_stocks(provider, market, lower_limit=1, expire_mins=120):
    logger = logging.getLogger('MainLogger')
    fail_stock_list = []
    cnx = sqlite3.connect(DB_PATH)

    stocks_df = get_failed_hk_stocks_df()
    for index, stock_df in stocks_df.iterrows():
        sid = str(stock_df['sid'])
        sql = "SELECT date FROM stocks WHERE sid='{}'".format(sid)
        dates = pd.read_sql(sql, cnx)
        last_date = dates.iloc[-1, 0]
        logger.info('processing: {} from {}'.format(sid, str(last_date)))
        df = get_hist_stock_price(sid, start_date=last_date)
        if len(df) < lower_limit:
            fail_stock_list.append(sid)
    logger.info(fail_stock_list)
    _retry_fails(fail_stock_list, expire_mins, cnx=cnx)

def update_all_stocks(provider, market, lower_limit=1, expire_mins=120):
    logger = logging.getLogger('MainLogger')
    fail_stock_list = []
    cnx = sqlite3.connect(DB_PATH)
    cnx.execute("PRAGMA journal_mode=WAL")

    present_ticker_ids = pd.read_sql("SELECT DISTINCT sid FROM stocks WHERE provider = '{}' and market = '{}'".format(provider, market), cnx)
    logger.info(present_ticker_ids)
    for sid in present_ticker_ids['sid']:
        sql = "SELECT date FROM stocks WHERE sid='{}'".format(sid)
        dates = pd.read_sql(sql, cnx)
        if dates.empty:
            last_date = datetime.strftime(datetime.now() - timedelta(200), '%Y-%m-%d')
        else:
            last_date = dates.iloc[-1, 0]
        if last_date[0:10] == datetime.strftime(datetime.now(), '%Y-%m-%d'):
            logger.info('bypassing updating: {} from {}'.format(sid, str(last_date)))    
        else:
            logger.info('processing updating: {} from {}'.format(sid, str(last_date)))
        df = get_hist_stock_price(sid, start_date=last_date)
        if len(df) < lower_limit:
            fail_stock_list.append(sid)
    logger.info(fail_stock_list)
    _retry_fails(fail_stock_list, expire_mins, cnx=cnx)

def insert_all_stocks(day_delta=2000, lower_limit=1, expire_mins=120):
    logger = logging.getLogger('MainLogger')
    start_date = datetime.strftime(datetime.now() - timedelta(day_delta), '%Y-%m-%d')
    stocks_df = get_hk_stocks_df()
    fail_stock_list = []
    for index, stock_df in stocks_df.iterrows():
        sid = str(stock_df['sid']).zfill(4) + '.HK'
        logger.info('processing: {}'.format(sid))
        df = get_hist_stock_price(sid, start_date=start_date)
        if len(df) < lower_limit:
            fail_stock_list.append(sid)
    logger.info(fail_stock_list)
    _retry_fails(fail_stock_list, expire_mins, start_date=start_date)

def _retry_fails(fail_stock_list, expire_mins, start_date=None, cnx=None):
    start = time.time()
    consuming_time = 0
    expire_seconds = expire_mins * 60
    if len(fail_stock_list) > 0:
        fail_stock_list_df = pd.DataFrame(fail_stock_list, columns=['sid'])
        fail_stock_list_df.to_csv(DATA_PATH + 'fail_stock_list.csv', index=False)
    while consuming_time < expire_seconds:
        for sid in fail_stock_list:
            if start_date is None and cnx is not None:
                sql = "SELECT date FROM stocks WHERE sid='{}'".format(sid)
                dates = pd.read_sql(sql, cnx)
                start_date = dates.iloc[-1, 0]
            df = get_hist_stock_price(sid, start_date=start_date)
            if len(df) > 0:
                fail_stock_list.remove(sid)
        if len(fail_stock_list) > 40:
            time.sleep(120)
        else:
            time.sleep(10)
        print(fail_stock_list)
        if len(fail_stock_list) > 0:
            fail_stock_list_df = pd.DataFrame(fail_stock_list, columns=['sid'])
            fail_stock_list_df.to_csv(DATA_PATH + 'fail_stock_list.csv', index=False)
        else:
            break
        current_time = time.time()
        consuming_time = current_time - start
        print(consuming_time)
        print(expire_seconds)