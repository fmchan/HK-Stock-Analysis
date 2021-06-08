#-*- coding: utf-8 -*-
import os
from flask import Flask, request, jsonify, render_template
from configs.settings import HOST, PORT, DB_QUERY_START_DATE, DATA_PATH, CPU_COUNT
import datetime as dt
from configs.logger import Logger
import pandas as pd
from dbhelper import DBHelper
import matplotlib
# matplotlib.use('Svg')
matplotlib.use('AGG')
import matplotlib.pyplot as plt
from patterns import sepa
from patterns import vcp
from patterns.all_patterns import Patterns
import numpy as np
import io
import base64
import json
import json as json_parser
from collections import OrderedDict
from flask_caching import Cache
from dateutil.relativedelta import *
from datetime import date, timedelta, datetime
from datautils import pattern_utils
from configs.settings import DB_PATH
import sqlite3
from models.watchlist import Watchlist
from configs.table_types import Tables
import multiprocessing as mp
import time
import asyncio

db = DBHelper()
pd.set_option('display.max_columns', None)
app = Flask(__name__, template_folder="static")
app.config["JSON_AS_ASCII"] = False
cache = Cache(app, config={"CACHE_TYPE": "simple"})
logger = Logger("MainLogger").setup_system_logger()

def make_cache_key(*args, **kwargs):
    return request.args.get("trading_date"), request.args.get("pattern_name"), request.args.get("min_volume"), request.args.get("max_volume"), request.args.get("status")

@app.route("/")
def index():
    patterns = db.query_distinct_pattern()
    return render_template('index.html', patterns = patterns["pattern"])

@app.route("/compareSEPAStockPatterns", methods=["GET"])
def compareSEPAStockPatterns():
    trading_date = request.args.get("trading_date")
    logger.info("compare SEPA patterns which dated on [%s]"%(trading_date))

    SEPA_df = db.query_pattern(start_date=trading_date, name="SEPA")
    SEPA2_df = db.query_pattern(start_date=trading_date, name="SEPA2")
    SEPA3_df = db.query_pattern(start_date=trading_date, name="SEPA3")

    frames = [SEPA_df["sid"], SEPA2_df["sid"], SEPA3_df["sid"]]
    report = pd.concat(frames, keys=["sid", "sid", "sid"], ignore_index=True).drop_duplicates()
    report_df = pd.DataFrame({"sid":report.values})
    report_df.sort_values("sid", inplace=True)
    report_df["SEPA"], report_df["SEPA2"], report_df["SEPA3"] = report_df["sid"].isin(SEPA_df["sid"]).astype(bool), report_df["sid"].isin(SEPA2_df["sid"]).astype(bool), report_df["sid"].isin(SEPA3_df["sid"]).astype(bool)
    for col in report_df.columns:
        report_df[col] = report_df[col].replace({True:"✓", False:""})
    report_df.style.set_properties(**{"text-align": "center"})
    report_df = report_df.reset_index(drop=True)
    converted_output = report_df.to_html(index=False)
    logger.info("end of compareSEPAStockPatterns()")
    return render_template('compare.html', trading_date = trading_date, content = converted_output)

@app.route("/compareVCPStockPatterns", methods=["GET"])
def compareVCPStockPatterns():
    trading_date = request.args.get("trading_date")
    logger.info("compare VCP patterns which dated on [%s]"%(trading_date))

    vcp_df = db.query_pattern(start_date=trading_date, name="VCP")
    vcp_ta1_df = db.query_pattern(start_date=trading_date, name="VCP_TA1")
    vcp_ta2_df = db.query_pattern(start_date=trading_date, name="VCP_TA2")
    vcp_ta3_df = db.query_pattern(start_date=trading_date, name="VCP_TA3")

    frames = [vcp_df["sid"], vcp_ta1_df["sid"], vcp_ta2_df["sid"], vcp_ta3_df["sid"]]
    report = pd.concat(frames, keys=["sid", "sid", "sid", "sid"], ignore_index=True).drop_duplicates()
    report_df = pd.DataFrame({"sid":report.values})
    report_df.sort_values("sid", inplace=True)
    report_df["VCP"], report_df["VCP_TA1"], report_df["VCP_TA2"], report_df["VCP_TA3"] = report_df["sid"].isin(vcp_df["sid"]).astype(bool), report_df["sid"].isin(vcp_ta1_df["sid"]).astype(bool), report_df["sid"].isin(vcp_ta2_df["sid"]).astype(bool), report_df["sid"].isin(vcp_ta3_df["sid"]).astype(bool)
    for col in report_df.columns:
        report_df[col] = report_df[col].replace({True:"✓", False:""})
    report_df.style.set_properties(**{"text-align": "center"})
    report_df = report_df.reset_index(drop=True)
    converted_output = report_df.to_html(index=False)
    logger.info("end of compareVCPStockPatterns()")
    return render_template('compare.html', trading_date = trading_date, content = converted_output)

@app.route("/getStockPatterns", methods=["GET"])
@cache.cached(timeout=3600, key_prefix=make_cache_key)
def getStockPatterns():
    start_time = time.time()
    trading_date = request.args.get("trading_date")
    pattern_name = request.args.get("pattern_name")
    min_volume = float(request.args.get("min_volume")) * 1_000_000
    max_volume = float(request.args.get("max_volume")) * 1_000_000
    logger.info("start finding [%s] patterns in volume range [%s] to [%s] which dated on [%s]"%(pattern_name, min_volume, max_volume, trading_date))
    sid_list = []

    patterns_df = db.query_pattern_w_pct_chg(start_date=trading_date, name=pattern_name, min_volume=min_volume, max_volume=max_volume)
    logger.debug("total stocks: [%s] "%(len(patterns_df)))
    if len(patterns_df) > 0:
        # converted_output = "<div><nav class='sidediv'><ul>"
        converted_output = "<div><nav class='sidediv'> \
            <button class='return_btn sort_btn' sort='desc' onclick=\"sortList('float', 'return')\">return ↓</button> \
            <button class='volume_btn sort_btn' sort='desc' onclick=\"sortList('float', 'volume')\">volume ↓</button> \
            <ul id='sideul'>"
            # <button onclick=\"sortList('float', 'return', 'asc')\">Sort By Return ASC</button> \
            # <button onclick=\"sortList('float', 'return', 'desc')\">Sort By Return Desc</button> \
            # <button onclick=\"sortList('float', 'volume', 'asc')\">Sort By Volume ASC</button> \
            # <button onclick=\"sortList('float', 'volume', 'desc')\">Sort By Volume Desc</button> \
        for index, row in patterns_df.iterrows():
            sid = row["sid"]
            sid_list.append(sid)
            pct_chg = round(row["pct_diff"], 2)
            volume = row["volume"]
            converted_output += "<li volume='%s' return='%s'><a href='#%s.%s'>%s</a> (<span class='price_movement'>%s</span>)<br><span>(vol: %s)</span></li>" %(volume, pct_chg, sid, pattern_name, sid, pct_chg, volume)
        converted_output += "</ul></nav>"

        converted_output += "<div class='content'>"
        num_processes = CPU_COUNT
        if len(patterns_df) > num_processes*2:
            logger.info("start multiprocessing")
            with mp.Pool(processes=num_processes) as pool:
                results = [pool.apply_async(_get_stock_output, args=[trading_date, sid, pattern_name, False, Tables.PATTERN_DETAILS.name, None]) for sid in sid_list]
                for output in results:
                    converted_output += output.get()
        else:
            for sid in sid_list:
                converted_output += _get_stock_output(trading_date, sid, pattern_name, False, Tables.PATTERN_DETAILS.name, None)
        converted_output += "</div></div><br>"

        logger.info("getStockPatterns() takes %s seconds", time.time() - start_time)
        return render_template('result.html', trading_date = trading_date, content = converted_output)
    else:
        return "no pattern found"

def _get_stock_output(trading_date, sid, pattern_name, bin_volume=False, table_type=Tables.PATTERN_DETAILS.name, start_date=None):
    logger.info("getting output for %s "%(sid))
    aastock_code = sid.split(".")[0].zfill(5)
    aastock_dynamic_chart_image_url = "http://www.aastocks.com/tc/stocks/quote/dynamic-chart.aspx?symbol={}".format(aastock_code)
    aastock_chart_image_url = "http://charts.aastocks.com/servlet/Charts?fontsize=12&15MinDelay=T&lang=1&titlestyle=1&vol=1&Indicator=1&indpara1=10&indpara2=20&indpara3=50&indpara4=150&indpara5=200&subChart1=2&ref1para1=14&ref1para2=0&ref1para3=0&subChart2=3&ref2para1=12&ref2para2=26&ref2para3=9&scheme=6&com=100&chartwidth=870&chartheight=700&stockid={}.HK&period=6&type=1".format(aastock_code)

    df = db.query_stock("YAHOO", "HK", sid, start=DB_QUERY_START_DATE, letter_case=True)
    logger.debug("[%s] returns len(%s) "%(sid, len(df)))
    df = _compute_pattern_features(pattern_name, df, bin_volume)
    df.rename(columns={"Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)

    plot_df = df.copy()
    plot_df["date"] = plot_df["date"].astype(str)
    pattern_df = db.query_stock_pattern(sid, pattern_name) # to avoid too much patterns to be plot
    logger.debug("[%s] returns len(%s) "%(pattern_name, len(pattern_df)))
    # stock_pattern_df = db.query_stock_pattern(sid) # to display all kinds of patterns for single stock
    stock_pattern_df = pd.merge(pattern_df, plot_df, left_on="start_date", right_on="date", how="right")
    stock_pattern_df["start_date"] = stock_pattern_df["start_date"].astype(str)
    stock_pattern_df["pattern_point"] = np.where(stock_pattern_df["start_date"] == stock_pattern_df["date"], stock_pattern_df["close"]*1.02, None)
    past_6_month = (datetime.now() - relativedelta(months=6))
    stock_pattern_df["date"] = pd.to_datetime(stock_pattern_df["date"])
    stock_pattern_df = stock_pattern_df[stock_pattern_df["date"] > past_6_month]
    # stock_pattern_df = stock_pattern_df.iloc[-120:] # show latest 6 months (20 days * 6)
    logger.debug("plotting start for [%s]"%(sid))
    fig, ax = plt.subplots(figsize=(10, 4))
    if pattern_name in ["SEPA", "SEPA2", "SEPA3"]:
        stock_pattern_df = stock_pattern_df[(stock_pattern_df["sma_200"] > 0)]
        stock_pattern_df.plot(x="date", y=["close", "sma_50", "ema_50", "sma_150", "ema_150", "sma_200", "ema_200", "52w_low", "52w_high"], ax=ax)
    elif pattern_name in ["VCP_TA1"]:
        stock_pattern_df = stock_pattern_df[(stock_pattern_df["sma_200"] > 0)]
        stock_pattern_df.plot(x="date", y=["close", "sma_200", "30w_avg", "40w_avg"], ax=ax)
    elif pattern_name in ["VCP_TA2"]:
        stock_pattern_df = stock_pattern_df[(stock_pattern_df["50w_sma"] > 0)]
        stock_pattern_df.plot(x="date", y=["close", "13w_ema", "26w_ema", "50w_sma", "40w_sma", "50w_low", "50w_high", "13d_ema", "26d_ema", "40d_sma", "50d_sma"], ax=ax)
    elif pattern_name in ["VCP"]:
        stock_pattern_df.plot(x="date", y=["close", "52w_high"], ax=ax)
    else:
        stock_pattern_df.plot(x="date", y=["close"], ax=ax)

    if bin_volume:
        ax2 = ax.twiny()
        ax2.hist(stock_pattern_df["close"], bins=30, weights=stock_pattern_df["cum_volume_pect"], orientation="horizontal", alpha=0.3, label="volume", color="orange")
        # ax2.hist(stock_pattern_df['close'], bins=100, weights=stock_pattern_df['volume'], orientation='horizontal', alpha=0.3, label="volume", color="darkblue")[0:2]
        ax2.set_xlim(ax2.get_xlim()[::-1])
        ax2.yaxis.tick_right()
        ax2.set(xlabel="cum_volume_pect")

    if trading_date:
        ax = stock_pattern_df.plot(x="date", y="pattern_point", ax=ax, kind="scatter", label=pattern_name, marker="v", color="red")
        ax = ax.scatter(stock_pattern_df[stock_pattern_df["date"]==trading_date]["date"], stock_pattern_df[stock_pattern_df["date"]==trading_date]["pattern_point"], label="as at", marker="v", color="darkred")

    logger.debug("saving plotting for [%s]"%(sid))
    img = io.BytesIO()
    plt.savefig(img, format="jpg", bbox_inches="tight") # also support format="svg", format="png"
    img.seek(0)
    pattern_obj = base64.b64encode(img.getvalue()).decode("utf8")
    img.close()
    plt.clf()
    plt.close("all")
    logger.debug("plotting done for [%s]"%(sid))

    data_df = pd.merge(pattern_df, plot_df, left_on="start_date", right_on="date", how="inner")
    data_df = _post_process_features(data_df, pattern_name)
    data_df.insert(0, "pattern", pattern_name) # loc at first
    data_df.drop(["pattern", "start_date", "end_date", "name", "sid_x", "sid_y"], axis=1, inplace=True)

    is_watchlist_record = True if db.query_watchlist_by_sid(sid, pattern_name).iloc[0].values[0] > 0 else False
    bookmark_button_html = "<button type='button' sid='%s' pattern='%s' class='bookmark'>Bookmark</button>" %(sid, pattern_name)
    if is_watchlist_record:
        bookmark_button_html = "<button type='button' sid='%s' pattern='%s' disabled class='bookmark'>Bookmark</button>" %(sid, pattern_name)
    if table_type == Tables.ARCHIVED_WATCHLIST.name:
        # converted_output = "<section id='%s.%s'><div class='row'><a target='_blank' href='%s'>%s</a></div><br>" %(sid, pattern_name, aastock_dynamic_chart_image_url, sid)
        converted_output = "<section id='%s.%s'> \
            <div class='row'> \
                <a target='_blank' href='%s'>%s</a> \
                <button type='button' sid='%s' pattern='%s' start_date='%s' class='delete'>Delete</button> \
            </div><br>" %(sid, pattern_name, aastock_dynamic_chart_image_url, sid, sid, pattern_name, start_date)
    elif table_type == Tables.PATTERN_DETAILS.name:
        converted_output = "<section id='%s.%s'> \
            <div class='row'> \
                <a target='_blank' href='%s'>%s</a>" %(sid, pattern_name, aastock_dynamic_chart_image_url, sid)
        converted_output += bookmark_button_html
        converted_output += "</div><br>"
    elif table_type == Tables.WATCHLIST.name:
        converted_output = "<section id='%s.%s'> \
            <div class='row'> \
                <a target='_blank' href='%s'>%s</a> \
                <button type='button' sid='%s' pattern='%s' class='archive'>Archive</button> \
            </div><br>" %(sid, pattern_name, aastock_dynamic_chart_image_url, sid, sid, pattern_name)
    converted_output += "<div class='row'>%s</div><br>" %(data_df.to_html(index=False))
    converted_output += "<div class='row'><img src='data:image/png;base64,%s'></div><br>" %(pattern_obj)
    converted_output += "<div class='row'><img src='%s'></div><br>" %(aastock_chart_image_url)
    converted_output += "<div style='height: 50px !important;'></div></section><br>"
    logger.info("total patterns for [%s]: [%s] "%(sid, len(data_df)))
    return converted_output

def _post_process_features(df, pattern_name):
    if pattern_name in ["SEPA", "SEPA2", "SEPA3"]:
        df["52w_low_pct_chg"] = df["52w_low_pct_chg"].round().astype(int).astype(str) + "%"
        df["52w_high_pct_chg"] = df["52w_high_pct_chg"].round().astype(int).astype(str) + "%"
        df["avg_volume_5d"] = (df['avg_volume_5d'].astype(float)/1000000).round(2).astype(str) + "M"
        df["ema_volume_20d"] = (df['ema_volume_20d'].astype(float)/1000000).round(2).astype(str) + "M"
    elif pattern_name in ["VCP_TA1"]:
        df["obv"] = (df['obv'].astype(float)/1000000).round(2).astype(str) + "M"
    elif pattern_name in ["VCP_TA3"]:
        df["5d_volume_sma"] = (df['5d_volume_sma'].astype(float)/1000000).round(2).astype(str) + "M"
    df["volume"] = (df['volume'].astype(float)/1000000).round(2).astype(str) + "M"
    df = df.round(2)
    return df

def _compute_pattern_features(pattern_name, df, bin_volume=False):
    if bin_volume:
        price_list = df["Close"].value_counts()
        price_list = pd.DataFrame(price_list)
        for prc in price_list.index:
            cnt_q = df[df["Close"]==prc]["Volume"].sum()
            price_list.loc[prc, "cum_volume"] = cnt_q
        cyq_sum = price_list["cum_volume"].sum()
        price_list["cum_volume_pect"] = price_list["cum_volume"] / cyq_sum * 100
        price_list.reset_index(inplace=True)
        price_list.rename(columns={"Close": "count"}, inplace=True)
        # print(price_list)
        df = pd.merge(df, price_list, left_on="Close", right_on="index", how="left")
        df.set_index('Date', inplace=True, drop=False)
        df.index.names = ['DateTime']
        df['Date'] = pd.to_datetime(df["Date"]).dt.strftime('%Y-%m-%d')
        df.index = pd.to_datetime(df.index)
        # print(df[data_df["count"] > 1].tail(10))

    if pattern_name in ["SEPA", "SEPA2", "SEPA3"]:
        df = sepa.compute_sepa_features(df)
        if bin_volume:
            df = df[["sid", "Date", "Open", "High", "Low", "Close", "sma_10", "sma_50", "sma_150", "sma_200", "ema_50", "ema_150", "ema_200", "52w_low", "52w_low_pct_chg", "52w_high", "52w_high_pct_chg", "Volume", "avg_volume_5d", "ema_volume_20d", "cum_volume", "cum_volume_pect"]]
        else:
            df = df[["sid", "Date", "Open", "High", "Low", "Close", "sma_10", "sma_50", "sma_150", "sma_200", "ema_50", "ema_150", "ema_200", "52w_low", "52w_low_pct_chg", "52w_high", "52w_high_pct_chg", "Volume", "avg_volume_5d", "ema_volume_20d"]]
    elif pattern_name in ["VCP_TA1"]:
        df = vcp.compute_vcp_features(df)
        df = df[["sid", "Date", "Open", "High", "Low", "Close", "Volume", "obv", "rsi", "sma_200", "30w_avg", "40w_avg"]]
    elif pattern_name in ["VCP_TA2"]:
        df = vcp.compute_vcp_features(df)
        df = df[["sid", "Date", "Open", "High", "Low", "Close", "Volume", "13w_ema", "26w_ema", "50w_sma", "40w_sma", "50w_low", "50w_high", "13d_ema", "26d_ema", "40d_sma", "50d_sma"]]
    elif pattern_name in ["VCP_TA3"]:
        df = vcp.compute_vcp_features(df)
        df = df[["sid", "Date", "Open", "High", "Low", "Close", "Volume", "5d_volume_sma", "52w_high"]]
    else:
        df = df[["sid", "Date", "Open", "High", "Low", "Close", "Volume"]]

    return df

# def _append_features_data(sid, trading_date):
#     end_date = (datetime.strptime(trading_date, "%Y-%m-%d") + timedelta(1)).strftime("%Y-%m-%d")
#     df = db.query_stock("YAHOO", "HK", sid, start=DB_QUERY_START_DATE, end=end_date, letter_case=True)
#     df = _compute_pattern_features("SEPA", df)
#     df.rename(columns={"Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
#     df = _post_process_features(df)
#     # print(df.tail(1))
#     return df.tail(1)

@app.route("/stockScreener")
def stockScreener():
    return app.send_static_file("screener.html")

@app.route("/getIncomeSummary", methods=["GET"])
def getIncomeSummary():
    min_eps_growth = request.args.get("min_eps_growth")
    min_net_profit_growth = request.args.get("min_net_profit_growth")
    is_increase = request.args.get("is_increase")
    if is_increase and is_increase == "True":
        is_increase = True
    else:
        is_increase = False
    is_found = False
    logger.info("start finding stocks with min eps growth [%s] and min net profit growth [%s], [%s]"%(min_eps_growth, min_net_profit_growth, is_increase))
    cnx = sqlite3.connect(DB_PATH)
    cnx.execute("PRAGMA journal_mode=WAL")
    MIN_EPS_GROWTH = min_eps_growth
    MIN_NET_PROFIT_GROWTH = min_net_profit_growth

    present_ticker_ids = pd.read_sql("SELECT DISTINCT sid FROM stocks WHERE provider = '{}' and market = '{}'".format("YAHOO", "HK"), cnx)
    logger.info(present_ticker_ids)
    incomes_df = pd.read_csv(DATA_PATH + 'incomes_df.csv', index_col=False)
    incomes_df = incomes_df.sort_values(by="year", ascending=True)
    sub_converted_output = ""
    converted_output = "<div><nav class='sidediv'><ul>"
    for sid in present_ticker_ids['sid']:
        # print("processing :", sid)
        # sql = "SELECT basic_eps, eps_growth, net_profit, net_profit_growth, frequency, year FROM incomes WHERE sid='{}'".format(sid)
        # income_df = pd.read_sql(sql, cnx)
        income_df = incomes_df[incomes_df["sid"] == sid]
        if len(income_df) > 0 and len(income_df[income_df["frequency"]=="annual"]) > 0 and len(income_df[income_df["frequency"]=="quarterly"]) > 0:
            if income_df[income_df["frequency"]=="quarterly"]["eps_growth"].iloc[-1] >= float(MIN_EPS_GROWTH) \
            and income_df[income_df["frequency"]=="annual"]["net_profit_growth"].iloc[-1] >= float(MIN_NET_PROFIT_GROWTH) \
            and income_df[income_df["frequency"]=="annual"]["net_profit_growth"].iloc[-2] >= float(MIN_NET_PROFIT_GROWTH) \
            and income_df[income_df["frequency"]=="annual"]["net_profit_growth"].iloc[-3] >= float(MIN_NET_PROFIT_GROWTH) \
            and (is_increase and income_df[income_df["frequency"]=="annual"]["net_profit_growth"].iloc[-1] > income_df[income_df["frequency"]=="annual"]["net_profit_growth"].iloc[-2] > income_df[income_df["frequency"]=="annual"]["net_profit_growth"].iloc[-3]
                or not is_increase):
                logger.info("found: " + sid)
                converted_output += "<a href='#%s'>%s</a><br>" %(sid, sid)
                is_found = True
                sub_converted_output += _get_summary_output(sid, income_df)
    converted_output += "</ul></nav>"
    converted_output += "<div class='content'>"
    converted_output += sub_converted_output
    converted_output += "</div></div><br>"
    logger.info("end of getIncomeSummary()")
    if is_found > 0:
        return render_template('result.html', content = converted_output)
    else:
        return "no stocks found"

def _get_summary_output(sid, data_df):
    logger.info("getting output for %s "%(sid))
    aastock_code = sid.split(".")[0].zfill(5)
    aastock_earnings_summary_url = "http://www.aastocks.com/en/stocks/analysis/company-fundamental/earnings-summary?symbol={}".format(aastock_code)
    aastock_chart_image_url = "http://charts.aastocks.com/servlet/Charts?fontsize=12&15MinDelay=T&lang=1&titlestyle=1&vol=1&Indicator=1&indpara1=10&indpara2=20&indpara3=50&indpara4=150&indpara5=200&subChart1=2&ref1para1=14&ref1para2=0&ref1para3=0&subChart2=3&ref2para1=12&ref2para2=26&ref2para3=9&scheme=6&com=100&chartwidth=870&chartheight=700&stockid={}.HK&period=6&type=1".format(aastock_code)

    data_df = data_df[["period", "year", "month", "frequency", "basic_eps", "eps_growth", "net_profit", "net_profit_growth"]]
    # data_df["net_profit_million"] = (data_df["net_profit"].astype(float)/1000000).round(2)
    # data_df["net_profit"] = data_df["net_profit_million"].astype(str) + "M"
    data_df["net_profit"] = (data_df["net_profit"].astype(float)/1000000).round(2).astype(str) + "M"
    data_df[["eps_growth", "net_profit_growth"]] = data_df[["eps_growth", "net_profit_growth"]].round(2)

    annual_income_df = data_df[data_df["frequency"]=="annual"].sort_values(by=["year", "month"], ascending=[True, True])
    quarterly_income_df = data_df[data_df["frequency"]=="quarterly"].sort_values(by=["year", "month"], ascending=[True, True])
    annual_income_df = annual_income_df.set_index(["year", "month", "frequency"]).unstack("frequency")
    quarterly_income_df = quarterly_income_df.set_index(["year", "month", "frequency"]).unstack("frequency")

    # option 1 row not share the same year
    # annual_income_df.reset_index(drop=True, inplace=True)
    # quarterly_income_df.reset_index(drop=True, inplace=True)
    # result_df = pd.concat([annual_income_df, quarterly_income_df], axis=1, join="outer")
    # result_df.columns = result_df.columns.swaplevel(0,1)
    # result_df.reset_index(drop=True, inplace=True)
    # result_df.fillna("", inplace=True)
    # result_df.columns.names = (None, None)

    # # option 2 row share the same year
    annual_income_df.insert(loc=0, column="year", value=annual_income_df.index.get_level_values("year"))
    quarterly_income_df.insert(loc=0, column="year", value=quarterly_income_df.index.get_level_values("year"))
    result_df = annual_income_df.merge(quarterly_income_df, how='outer')
    # annual_income_df = annual_income_df.set_index("year") # on .join()
    # quarterly_income_df = quarterly_income_df.set_index("year") # on .join()
    # result_df = annual_income_df.join(quarterly_income_df, how='outer')
    result_df.columns = result_df.columns.swaplevel(0,1)
    result_df.reset_index(drop=True, inplace=True)
    result_df.fillna("", inplace=True)
    result_df.columns.names = (None, None)
    result_df.loc[result_df["quarterly"].duplicated(), "quarterly"] = ""
    result_df = result_df.drop("year", axis=1, level=1)

    # print(result_df)
    data_df.dropna(subset=["eps_growth", "net_profit_growth"], how='all', inplace=True)
    data_df["freq"] = np.where(data_df["frequency"]=="annual", "", "Q")
    data_df["new_period"] = data_df["period"] + data_df["freq"]
    data_df = data_df.set_index(["new_period"])
    # data_df = data_df.set_index(["period"])
    data_df = data_df.sort_values(by=["frequency", "year", "month"], ascending=[True, True, True])
    # print(data_df)
    # fig, ax = plt.subplots()
    data_df[["eps_growth", "net_profit_growth"]].plot(kind='bar', rot=30)
    # fig.autofmt_xdate()
    # ax.set_xticklabels(ax.get_xticklabels(), fontsize=7)
    # plt.show()
    logger.info("saving plotting for [%s]"%(sid))
    img = io.BytesIO()
    plt.savefig(img, format="png", bbox_inches="tight")
    img.seek(0)
    pattern_obj = base64.b64encode(img.getvalue()).decode()
    plt.clf()
    plt.close("all")
    logger.info("plotting done for [%s]"%(sid))

    sub_converted_output = "<section id='%s'><div class='row'><a target='_blank' href='%s'>%s</a></div><br>" %(sid, aastock_earnings_summary_url, sid)
    sub_converted_output += "<div class='row'>%s</div><br>" %(result_df.to_html(index=False))
    sub_converted_output += "<div class='row'><img src='data:image/png;base64,%s'></div><br>" %(pattern_obj)
    sub_converted_output += "<div class='row'><img src='%s'></div><br>" %(aastock_chart_image_url)
    sub_converted_output += "<div style='height: 50px !important;'></div></section><br>"
    logger.info("total patterns for [%s]: [%s] "%(sid, len(result_df)))
    return sub_converted_output

@app.route("/watchlist")
def watchlist():
    return app.send_static_file("watchlist.html")

@app.route("/getWatchlist", methods=["GET"])
def getWatchlist():
    pattern_name = request.args.get("pattern_name")
    status = request.args.get("status")
    logger.info("start finding [%s] patterns for [%s] status"%(pattern_name, status))
    sid_list = []
    start_date_list = []
    if status == "A":
        table_type = table_type=Tables.WATCHLIST.name
    elif status == "I":
        table_type = table_type=Tables.ARCHIVED_WATCHLIST.name
    watchlist_df = db.query_watchlist(pattern_name, status)
    logger.info("total stocks: [%s] "%(len(watchlist_df)))
    if len(watchlist_df) > 0:
        converted_output = "<div><nav class='sidediv'> \
            <button class='start_date_btn sort_btn' sort='desc' onclick=\"sortList('date', 'start_date')\">start_date ↓</button> \
            <button class='return_btn sort_btn' sort='desc' onclick=\"sortList('float', 'return')\">return ↓</button> \
            <ul id='sideul'>"
            # <button onclick=\"sortList('date', 'start_date', 'asc')\">Sort By Date ASC</button> \
            # <button onclick=\"sortList('date', 'start_date', 'desc')\">Sort By Date Desc</button> \
            # <button onclick=\"sortList('float', 'return', 'asc')\">Sort By Return ASC</button> \
            # <button onclick=\"sortList('float', 'return', 'desc')\">Sort By Return Desc</button> \
        for index, row in watchlist_df.iterrows():
            sid = row["sid"]
            sid_list.append(sid)
            start_date = row["start_date"][:10]
            start_date_list.append(start_date)
            pct_chg = round(row["pct_diff"], 2)
            converted_output += "<li start_date='%s' return='%s'><a href='#%s.%s'>%s</a> (<span class='price_movement'>%s</span>)<br><span>(%s)</span></li>" %(start_date, pct_chg, sid, pattern_name, sid, pct_chg, start_date)
        converted_output += "</ul></nav>"

        converted_output += "<div class='content'>"
        num_processes = CPU_COUNT
        if len(watchlist_df) > num_processes*2:
            logger.info("start multiprocessing")
            with mp.Pool(processes=num_processes) as pool:
                results = [pool.apply_async(_get_stock_output, args=[None, sid, pattern_name, False, table_type, start_date]) for sid, start_date in zip(sid_list, start_date_list)]
                for output in results:
                    converted_output += output.get()
        else:
            for sid, start_date in zip(sid_list, start_date_list):
                converted_output += _get_stock_output(None, sid, pattern_name, table_type=table_type, start_date=start_date)
        converted_output += "</div></div><br>"

        logger.info("end of getWatchlist()")
        return render_template('result.html', content = converted_output)
    else:
        return "no watchlist found"

@app.route('/bookmark', methods=['POST'])
def bookmark():
    logger.info("start bookmark")
    sid = request.json["sid"]
    pattern = request.json["pattern"]
    logger.info('%s %s bookmark' %(sid, pattern))
    uid = "admin" # TODO
    watchlist = Watchlist(uid, sid, pattern, "A", datetime.today().strftime('%Y-%m-%d'))
    DBHelper().insert_watchlist(watchlist)
    return "bookmark successfully for " + sid

@app.route('/archive', methods=['POST'])
def archive_bookmark():
    logger.info("start archive")
    sid = request.json["sid"]
    pattern = request.json["pattern"]
    logger.info('%s %s archive' %(sid, pattern))
    DBHelper().update_watchlist_enddate(sid, pattern, datetime.today().strftime('%Y-%m-%d'))
    return "archive successfully for " + sid

@app.route('/delete', methods=['POST'])
def delete_archive_bookmark():
    logger.info("start delete")
    sid = request.json["sid"]
    pattern = request.json["pattern"]
    start_date = request.json["start_date"]
    logger.info('%s %s delete' %(sid, pattern))
    DBHelper().delete_watchlist(sid, pattern, start_date)
    return "delete successfully for " + sid

if __name__ == "__main__":
    port = int(os.environ.get("PORT", PORT))
    app.run(host = HOST, port = port, debug = False) # flask is in threaded mode by default

	# from tornado.wsgi import WSGIContainer
	# from tornado.httpserver import HTTPServer
	# from tornado.ioloop import IOLoop

	# http_server = HTTPServer(WSGIContainer(app))
	# http_server.listen(port)
	# IOLoop.instance().start() 
