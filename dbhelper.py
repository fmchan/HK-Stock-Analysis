#-*- coding: utf-8 -*-
from sqlalchemy import create_engine, and_, or_, any_, DDL
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy import exc
from models.stock import Stock
from datetime import date, timedelta
import datetime as dt
from configs.settings import DB_PATH
from configs.base import Session, engine, Base
from configs.logger import Logger
import logging
import json
import os
from datetime import datetime
import sqlite3
import csv
import pandas as pd
from datetime import timedelta

class DBHelper:
    def __init__(self):
        self.encoding = 'utf-8'
        self.logger = logging.getLogger('MainLogger')

    def insert_stock(self, provider, market, sid, date, open, high, low, close, volume, adj_close):
        try:
            self.logger.info("inserting stock for sid: {}".format(sid))
            session = Session()
            result = session.query(Stock).filter_by(provider = provider, market = market, sid = sid, date = date).first()
            if result is None:
                stock = Stock(provider = provider, market = market, sid = sid, date = date, open = open, high = high, low = low, close = close, volume = volume, adj_close = adj_close, created_date = datetime(datetime.today().year, datetime.today().month, datetime.today().day), updated_date = datetime(datetime.today().year, datetime.today().month, datetime.today().day))
                session.add(stock)
            # else:
            #     self.logger.info("record existed, updating stock for sid: {}".format(sid))
            #     result.updated_date = datetime.now()
            session.commit()
            return "done"
        except Exception as e:
            message = "Exception in insert_stock: %s" % e
            self.logger.exception(message + str(e))
            return message
        finally:
            session.close()

    def query_stock(self, provider, market, sid, start=datetime.strftime(datetime.now()-timedelta(200), '%Y-%m-%d'), end=datetime.strftime(datetime.now(), '%Y-%m-%d'), letter_case=True):
        try:
            cnx = sqlite3.connect(DB_PATH.replace('sqlite:', '').replace('/', ''))
            query = f"""
                SELECT * FROM stocks
                WHERE sid = '{sid}'
                AND provider = '{provider}'
                AND market = '{market}'
                AND date BETWEEN '{start}' AND '{end}'
            """
            df = pd.read_sql_query(query, cnx)
            if letter_case:
                df.rename(columns={'date': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
                df.set_index('Date', inplace=True)
            else:
                df.set_index('date', inplace=True)
            return df       
        except Exception as e:
            message = "Exception in query_stock: {}".format(e)
            self.logger.exception(message)
            return message

    def query_stock_by_volume(self, top, volume):
        try:
            cnx = sqlite3.connect(DB_PATH.replace('sqlite:', '').replace('/', ''))
            query = f"""
                SELECT sid
                FROM (
                    SELECT *
                    FROM (
                        SELECT
                            sid, volume, date,
                            ROW_NUMBER() OVER (
                                PARTITION BY sid
                                ORDER BY date DESC
                            ) rn
                        FROM stocks)
                    WHERE
                        rn <= {top}
                    GROUP BY sid
                    HAVING AVG(volume) > {volume})
            """
            df = pd.read_sql_query(query, cnx)
            return df       
        except Exception as e:
            message = "Exception in query_stock_by_volume: {}".format(e)
            self.logger.exception(message)
            return message

    def custom_alter(self):
        add_column = DDL('ALTER TABLE stocks ADD COLUMN newcol VARCHAR(300)')
        engine.execute(add_column)

if __name__ == "__main__":
    # print(sqlite3.sqlite_version)
    # print(sqlite3.version)
    logger = Logger('MainLogger').setup_system_logger()
    db = DBHelper()
    cnx = sqlite3.connect(DB_PATH.replace('sqlite:', '').replace('/', ''))
    provider = 'YAHOO'
    sid = '0700.HK'
    market = 'HK'
    df = db.query_stock(provider, market, sid, start='2019-04-01')
    print(df)