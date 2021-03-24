#-*- coding: utf-8 -*-
from sqlalchemy import create_engine, and_, or_, any_, DDL
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy import exc
from models.stock import Stock
from models.pattern import Pattern
from models.income import Income
from models.watchlist import Watchlist
import datetime as dt
from configs.settings import DB_PATH
from configs.base import Session, engine, Base
from configs.logger import Logger
import logging
import json
import os
from datetime import datetime, date, timedelta
import sqlite3
import csv
import pandas as pd

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
            self.logger.info("result returned for {}".format(sid))
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
            message = "Exception in query_pattern: {}".format(e)
            self.logger.exception(message)
            return message

    def query_pattern_w_pct_chg(self, start_date, name, min_volume, max_volume):
        try:
            cnx = sqlite3.connect(DB_PATH)
            # select * from
            # (SELECT start_date, end_date, name, sid FROM patterns WHERE start_date = "2021-02-10 00:00:00.000000" and name = "SEPA3") as p
            # join (
            # SELECT 100.0*(curr.close - prev.close) / prev.close As pct_diff, curr.sid
            # FROM stocks As curr
            # JOIN stocks As prev
            # ON curr.sid = prev.sid
            # where prev.date = '2021-02-10 00:00:00'
            # AND curr.date = (SELECT max(date) FROM stocks where sid = prev.sid)) as c
            # on p.sid = c.sid
            # order by pct_diff desc
            query = f"""
                select * from
                    (SELECT start_date, end_date, name, sid FROM patterns WHERE date(start_date) = '{start_date}' and name = '{name}') as p
                join (SELECT 100.0*(curr.close - prev.close) / prev.close As pct_diff, prev.sid, prev.volume
                    FROM stocks As curr
                    JOIN stocks As prev
                    ON curr.sid = prev.sid
                    where date(prev.date) = '{start_date}' AND prev.volume between '{min_volume}' AND '{max_volume}' 
                    AND curr.date = (SELECT max(date) FROM stocks where sid = prev.sid)) as c
                on p.sid = c.sid
                order by pct_diff desc
            """
            self.logger.info(query)
            df = pd.read_sql_query(query, cnx)
            self.logger.info("query_pattern_w_pct_chg result returned")
            df['start_date'] = pd.to_datetime(df["start_date"]).dt.strftime('%Y-%m-%d')
            # df['end_date'] = pd.to_datetime(df["end_date"]).dt.strftime('%Y-%m-%d')
            return df
        except Exception as e:
            message = "Exception in query_pattern_w_pct_chg: {}".format(e)
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
            self.logger.info("result returned for {}".format(sid))
            df['start_date'] = pd.to_datetime(df["start_date"]).dt.strftime('%Y-%m-%d')
            # df['end_date'] = pd.to_datetime(df["end_date"]).dt.strftime('%Y-%m-%d')
            return df
        except Exception as e:
            message = "Exception in query_stock_pattern: {}".format(e)
            self.logger.exception(message)
            return message

    def insert_income(self, income):
        try:
            session = Session()
            result = session.query(Income).filter_by(period = income.period, sid = income.sid).first()
            if result is None:
                self.logger.info("inserting {} income for sid {}".format(income.period, income.sid))
                session.add(income)
            else:
                self.logger.info("db found {} income for sid {}".format(income.period, income.sid))
            session.commit()
            return "done"
        except Exception as e:
            message = "Exception in insert_income: %s" % e
            self.logger.exception(message + str(e))
            return message
        finally:
            session.close()

    def query_income(self, sid):
        try:
            cnx = sqlite3.connect(DB_PATH)
            query = f"""
                SELECT * FROM incomes
                WHERE sid = '{sid}'
            """
            self.logger.info(query)
            df = pd.read_sql_query(query, cnx)
            self.logger.info("result returned for {}".format(sid))
            return df
        except Exception as e:
            message = "Exception in query_income: {}".format(e)
            self.logger.exception(message)
            return message

    def insert_watchlist(self, watchlist):
        try:
            watchlist.start_date = datetime.strptime(watchlist.start_date, "%Y-%m-%d")
            session = Session()
            result = session.query(Watchlist).filter_by(sid = watchlist.sid, pattern = watchlist.pattern, end_date = None).first() # allow to recreate if end_date is null
            if result is None:
                self.logger.info("inserting {} watchlist for sid {}".format(watchlist.pattern, watchlist.sid))
                session.add(watchlist)
            else:
                self.logger.info("db found {} watchlist for sid {}".format(watchlist.pattern, watchlist.sid))
            session.commit()
            return "done"
        except Exception as e:
            message = "Exception in insert_watchlist: %s" % e
            self.logger.exception(message + str(e))
            return message
        finally:
            session.close()

    def update_watchlist_enddate(self, sid, pattern, end_date, uid="admin"):
        try:
            self.logger.info("updating {} watchlist for sid {}".format(pattern, sid))
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
            session = Session()
            session.query(Watchlist).filter_by(sid = sid, pattern = pattern, uid=uid).update({"end_date": end_date, "status": "I"})
            session.commit()
        except Exception as e:
            message = "Exception in update_watchlist_enddate: %s" % e
            self.logger.exception(message + str(e))
            return message
        finally:
            session.close()

    def delete_watchlist(self, sid, pattern, start_date, uid="admin"):
        try:
            self.logger.info("deleting {} watchlist for sid {} on {}".format(pattern, sid, start_date))
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            session = Session()
            session.query(Watchlist).filter_by(sid = sid, pattern = pattern, uid = uid, start_date = start_date).delete()
            session.commit()
        except Exception as e:
            message = "Exception in delete_watchlist: %s" % e
            self.logger.exception(message + str(e))
            return message
        finally:
            session.close()

    def query_watchlist_by_sid(self, sid, pattern, uid="admin"):
        try:
            cnx = sqlite3.connect(DB_PATH)
            query = f"""
                SELECT count(1) as cnt FROM watchlist WHERE sid = '{sid}' AND status = 'A' AND pattern = '{pattern}' AND uid = '{uid}' AND end_date IS NULL
            """
            self.logger.info(query)
            df = pd.read_sql_query(query, cnx)
            self.logger.info("watchlist result returned for {} for sid {}".format(pattern, sid))
            return df
        except Exception as e:
            message = "Exception in query_watchlist_by_sid: {}".format(e)
            self.logger.exception(message)
            return message

    def query_watchlist(self, pattern, status, uid="admin"):
        try:
            cnx = sqlite3.connect(DB_PATH)
            # query = f"""
            #     SELECT 100.0*(curr.close - prev.close) / prev.close As pct_diff, w.sid, w.pattern, w.start_date
            #     FROM stocks As curr
            #     JOIN stocks As prev
            #     ON curr.sid = prev.sid
            #     JOIN watchlist as w
            #     ON curr.sid = w.sid
            #     WHERE w.pattern = '{pattern}'
            #     AND w.status = '{status}'
            #     AND date(prev.date) = date(w.start_date)
            #     AND curr.date = (SELECT max(date) FROM stocks where sid = prev.sid)
            # """

            # query for return only one sid in watchlist
            # query = f"""
            #     SELECT
            #     one.sid, one.status, one.pattern, one.start_date,
            #     CASE WHEN two.pct_diff IS NOT NULL THEN two.pct_diff ELSE 0.0 END pct_diff
            #     FROM (SELECT min(start_date) as start_date, sid, status, pattern FROM watchlist WHERE status = 'I' AND pattern = 'SEPA' AND uid = 'admin' GROUP BY sid) as one
            #     LEFT JOIN
            #     (SELECT 100.0*(curr.close - prev.close) / prev.close As pct_diff, w.sid, w.pattern, min(w.start_date) as start_date
            #     FROM stocks As curr
            #     JOIN stocks As prev
            #     ON curr.sid = prev.sid
            #     JOIN watchlist as w
            #     ON curr.sid = w.sid
            #     WHERE w.pattern = 'SEPA'
            #     AND w.status = 'I'
            #     AND pattern = 'SEPA'
            #     AND date(prev.date) = date(w.start_date)
            #     AND curr.date = (SELECT max(date) FROM stocks where sid = prev.sid) 
            #     GROUP BY prev.sid) as two
            #     ON two.sid = one.sid 
            #     AND two.start_date = one.start_date
            #     order by one.start_date
            # """

            query = f"""
                SELECT 
                    one.sid, one.status, one.pattern, one.start_date, 
                    CASE WHEN two.pct_diff IS NOT NULL THEN two.pct_diff ELSE 0.0 END pct_diff
                    FROM (SELECT * FROM watchlist WHERE status = '{status}' AND pattern = '{pattern}' AND uid = '{uid}') as one
                LEFT JOIN 
                    (SELECT 100.0*(curr.close - prev.close) / prev.close As pct_diff, w.sid, w.pattern, w.start_date 
                    FROM stocks As curr
                    JOIN stocks As prev
                    ON curr.sid = prev.sid
                    JOIN watchlist as w
                    ON curr.sid = w.sid
                    WHERE w.pattern = '{pattern}'
                    AND w.status = '{status}'
                    AND pattern = '{pattern}'
                    AND date(prev.date) = date(w.start_date)
                    AND curr.date = (SELECT max(date) FROM stocks where sid = prev.sid)) as two
                ON two.sid = one.sid
                AND two.start_date = one.start_date
                ORDER BY one.start_date
            """
            self.logger.info(query)
            df = pd.read_sql_query(query, cnx)
            self.logger.info("watchlist result returned for {} in status {}".format(pattern, status))
            return df
        except Exception as e:
            message = "Exception in query_watchlist: {}".format(e)
            self.logger.exception(message)
            return message

    def custom_alter(self):
        add_column = DDL('ALTER TABLE stocks ADD COLUMN newcol VARCHAR(300)')
        engine.execute(add_column)

    def delete_expired_stocks(self, sid):
        try:
            self.logger.info("deleting all stocks for sid {}".format(sid))
            session = Session()
            session.query(Stock).filter_by(sid = sid).delete()
            session.commit()
            return "done"
        except Exception as e:
            message = "Exception in delete_expired_stocks: %s" % e
            self.logger.exception(message)
            return message
        finally:
            session.close()

if __name__ == "__main__":
    # print(sqlite3.sqlite_version)
    # print(sqlite3.version)
    logger = Logger('MainLogger').setup_system_logger()
    db = DBHelper()
    cnx = sqlite3.connect(DB_PATH)
    provider = 'YAHOO'
    market = 'HK'
    sid = '0700.HK'
    # sid = '1155.HK'
    # sid = '1698.HK'
    # sid = '8171.HK'
    # sid = '8256.HK'
    # db.delete_expired_stocks(sid)
    df = db.query_stock(provider, market, sid, start='2019-04-01')
    # df = db.query_pattern(start_date='2021-01-15')
    print(df)
    # watchlist = Watchlist(sid, "SEPA", "A", "2021-02-23")
    # db.insert_watchlist(watchlist)
    # # db.update_watchlist_enddate(sid, "SEPA", "2021-03-23")
    db.delete_watchlist("3800.HK", "SEPA", "2021-02-04")
    df = db.query_watchlist("SEPA", "A")
    print(df)