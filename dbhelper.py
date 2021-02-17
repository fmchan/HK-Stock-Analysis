#-*- coding: utf-8 -*-
from sqlalchemy import create_engine, and_, or_, any_, DDL
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy import exc
from models.stock import Stock
from models.pattern import Pattern
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

    def insert_pattern(self, start_date, name, sid, end_date=None):
        try:
            session = Session()
            result = session.query(Pattern).filter_by(start_date = start_date, end_date = end_date, name = name, sid = sid).first()
            if result is None:
                self.logger.info("inserting {} pattern for sid {} at {}".format(name, sid, start_date))
                pattern = Pattern(start_date = start_date, end_date = end_date, name = name, sid = sid, created_date = datetime(datetime.today().year, datetime.today().month, datetime.today().day), updated_date = datetime(datetime.today().year, datetime.today().month, datetime.today().day))
                session.add(pattern)
            session.commit()
            return "done"
        except Exception as e:
            message = "Exception in insert_pattern: %s" % e
            self.logger.exception(message + str(e))
            return message
        finally:
            session.close()

    def query_stock(self, provider, market, sid, start=None, end=None, letter_case=True):
        try:
            if start is None:
                start = datetime.strftime(datetime.now()-timedelta(200), '%Y-%m-%d')
            if end is None:
                end = datetime.strftime(datetime.now()+timedelta(2), '%Y-%m-%d')
            cnx = sqlite3.connect(DB_PATH)
            query = f"""
                SELECT * FROM stocks
                WHERE sid = '{sid}'
                AND provider = '{provider}'
                AND market = '{market}'
                AND date BETWEEN '{start}' AND '{end}'
            """
            self.logger.info(query)
            df = pd.read_sql_query(query, cnx)
            if letter_case:
                df.rename(columns={'date': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
                df.set_index('Date', inplace=True, drop=False)
                df.index.names = ['DateTime']
                df['Date'] = pd.to_datetime(df["Date"]).dt.strftime('%Y-%m-%d')
            else:
                df.set_index('date', inplace=True, drop=False)
                df.index.names = ['datetime']
                df['date'] = pd.to_datetime(df["date"]).dt.strftime('%Y-%m-%d')
            df.index = pd.to_datetime(df.index)
            df = df.sort_index()
            self.logger.info("result returned for %s".format(sid))
            return df
        except Exception as e:
            message = "Exception in query_stock: {}".format(e)
            self.logger.exception(message)
            return message

    def query_stock_by_volume(self, top, volume):
        try:
            cnx = sqlite3.connect(DB_PATH)
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
            self.logger.info(query)
            df = pd.read_sql_query(query, cnx)
            self.logger.info("query_stock_by_volume result returned")
            return df
        except Exception as e:
            message = "Exception in query_stock_by_volume: {}".format(e)
            self.logger.exception(message)
            return message

    def query_pattern(self, start_date, end_date=None, name=None):
        try:
            cnx = sqlite3.connect(DB_PATH)
            query = f"""
                SELECT start_date, end_date, name, sid FROM patterns
                WHERE date(start_date) = '{start_date}' 
            """
            if name is not None:
                query += f" and name = '{name}'"
            query += " ORDER BY name desc, sid"
            self.logger.info(query)
            df = pd.read_sql_query(query, cnx)
            self.logger.info("query_pattern result returned")
            df['start_date'] = pd.to_datetime(df["start_date"]).dt.strftime('%Y-%m-%d')
            # df['end_date'] = pd.to_datetime(df["end_date"]).dt.strftime('%Y-%m-%d')
            return df
        except Exception as e:
            message = "Exception in query_stock: {}".format(e)
            self.logger.exception(message)
            return message

    def query_stock_pattern(self, sid, name=None):
        try:
            cnx = sqlite3.connect(DB_PATH)
            query = f"""
                SELECT start_date, end_date, name, sid FROM patterns
                WHERE sid = '{sid}'
            """
            if name is not None:
                query += f" and name = '{name}'"
            query += " ORDER BY name desc"
            self.logger.info(query)
            df = pd.read_sql_query(query, cnx)
            self.logger.info("result returned for %s".format(sid))
            df['start_date'] = pd.to_datetime(df["start_date"]).dt.strftime('%Y-%m-%d')
            # df['end_date'] = pd.to_datetime(df["end_date"]).dt.strftime('%Y-%m-%d')
            return df
        except Exception as e:
            message = "Exception in query_stock: {}".format(e)
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
    cnx = sqlite3.connect(DB_PATH)
    provider = 'YAHOO'
    sid = '3618.HK'
    market = 'HK'
    # df = db.query_stock(provider, market, sid, start='2019-04-01')
    # df = db.query_pattern(start_date='2021-01-15')
    df = db.query_stock_pattern(sid)
    print(df)