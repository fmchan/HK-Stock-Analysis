#-*- coding: utf-8 -*-
import os
from flask import Flask, request, jsonify, render_template
from datetime import datetime
from configs.settings import HOST, PORT, DB_QUERY_START_DATE, DATA_PATH
import datetime as dt
from configs.logger import Logger
import pandas as pd
from dbhelper import DBHelper
import matplotlib.pyplot as plt
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
from datetime import date, timedelta
from datautils import pattern_utils
from configs.settings import DB_PATH
import sqlite3

pd.set_option('display.max_columns', None)
app = Flask(__name__, template_folder="static")
app.config["JSON_AS_ASCII"] = False
cache = Cache(app, config={"CACHE_TYPE": "simple"})
logger = Logger("MainLogger").setup_system_logger()

def make_cache_key(*args, **kwargs):
    return request.form.get("trading_date"), request.form.get("pattern_name"), request.form.get("min_volume"), request.form.get("max_volume")

@app.route("/")
def index():
    return app.send_static_file("index.html")

@app.route("/compareStockPatterns", methods=["GET", "POST"])
def compareStockPatterns():
    db = DBHelper()
    trading_date = request.form.get("trading_date")
    logger.info("compare patterns which dated on [%s]"%(trading_date))

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
    # data_df = pd.DataFrame()
    # for index, stock_df in report_df.iterrows():
    #     sid = stock_df["sid"]
    #     df = _append_features_data(db, sid, trading_date)
    #     df.drop("date", axis=1, inplace=True)
    #     data_df = data_df.append(df)
    # data_df = data_df.reset_index(drop=True)
    # report_df = pd.concat([report_df, data_df], keys=["sid", "sid"], ignore_index=False, axis=1).T.drop_duplicates().T
    # report_df = report_df.reset_index(drop=True)
    # report_df.columns = report_df.columns.droplevel(0)
    # print(report_df)
    converted_output = report_df.to_html(index=False)
    return render_template('compare.html', trading_date = trading_date, content = converted_output)

@app.route("/getStockPatterns", methods=["GET", "POST"])
@cache.cached(timeout=3600, key_prefix=make_cache_key)
def getStockPatterns():
    db = DBHelper()
    trading_date = request.form.get("trading_date")
    pattern_name = request.form.get("pattern_name")
    min_volume = float(request.form.get("min_volume")) * 1_000_000
    max_volume = float(request.form.get("max_volume")) * 1_000_000
    logger.info("start finding [%s] patterns in volume range [%s] to [%s] which dated on [%s]"%(pattern_name, min_volume, max_volume, trading_date))

    patterns_df = db.query_pattern_w_pct_chg(start_date=trading_date, name=pattern_name, min_volume=min_volume, max_volume=max_volume)
    logger.info("total stocks: [%s] "%(len(patterns_df)))
    if len(patterns_df) > 0:
        print(patterns_df)
        converted_output = "<div><nav class='sidediv'><ul>"
        for index, row in patterns_df.iterrows():
            # logger.info(row["sid"])
            sid = row["sid"].values[0]
            pct_chg = round(row["pct_diff"], 2)
            converted_output += "<li><a href='#%s.%s'>%s</a> (<span class='price_movement'>%s</span>)</li>" %(sid, pattern_name, sid, pct_chg)
        converted_output += "</ul></nav>"

        converted_output += "<div class='content'>"
        for index, row in patterns_df.iterrows():
            sid = row["sid"].values[0]
            converted_output += _get_stock_output(db, trading_date, sid, pattern_name)
        converted_output += "</div></div><br>"

        logger.info("end of getStockPatterns()")
        return render_template('result.html', trading_date = trading_date, content = converted_output)
    else:
        return "no pattern found"

def _get_stock_output(db, trading_date, sid, pattern_name, bin_volume=False):
    logger.info("getting output for %s "%(sid))
    aastock_code = sid.split(".")[0].zfill(5)
    aastock_dynamic_chart_image_url = "http://www.aastocks.com/tc/stocks/quote/dynamic-chart.aspx?symbol={}".format(aastock_code)
    aastock_chart_image_url = "http://charts.aastocks.com/servlet/Charts?fontsize=12&15MinDelay=T&lang=1&titlestyle=1&vol=1&Indicator=1&indpara1=10&indpara2=20&indpara3=50&indpara4=150&indpara5=200&subChart1=2&ref1para1=14&ref1para2=0&ref1para3=0&subChart2=3&ref2para1=12&ref2para2=26&ref2para3=9&scheme=6&com=100&chartwidth=870&chartheight=700&stockid={}.HK&period=6&type=1".format(aastock_code)

    df = db.query_stock("YAHOO", "HK", sid, start=DB_QUERY_START_DATE, letter_case=True)
    logger.info("[%s] returns len(%s) "%(sid, len(df)))
    df = _compute_pattern_features(pattern_name, df, bin_volume)
    df.rename(columns={"Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)

    plot_df = df.copy()
    plot_df["date"] = plot_df["date"].astype(str)
    pattern_df = db.query_stock_pattern(sid, pattern_name) # to avoid too much patterns to be plot
    logger.info("[%s] returns len(%s) "%(pattern_name, len(pattern_df)))
    # stock_pattern_df = db.query_stock_pattern(sid) # to display all kinds of patterns for single stock
    stock_pattern_df = pd.merge(pattern_df, plot_df, left_on="start_date", right_on="date", how="right")
    stock_pattern_df["start_date"] = stock_pattern_df["start_date"].astype(str)
    stock_pattern_df["pattern_point"] = np.where(stock_pattern_df["start_date"] == stock_pattern_df["date"], stock_pattern_df["close"]*1.02, None)
    past_6_month = (datetime.now() - relativedelta(months=6))
    stock_pattern_df["date"] = pd.to_datetime(stock_pattern_df["date"])
    stock_pattern_df = stock_pattern_df[stock_pattern_df["date"] > past_6_month]
    # stock_pattern_df = stock_pattern_df.iloc[-120:] # show latest 6 months (20 days * 6)
    logger.info("plotting start for [%s]"%(sid))
    fig, ax = plt.subplots(figsize=(10, 4))
    if pattern_name in ["SEPA", "SEPA2", "SEPA3"]:
        stock_pattern_df = stock_pattern_df[(stock_pattern_df["sma_200"] > 0)]
        stock_pattern_df.plot(x="date", y=["close", "sma_50", "ema_50", "sma_150", "ema_150", "sma_200", "ema_200", "52w_low", "52w_high"], ax=ax)
    elif "CUP_HANDLE" == pattern_name:
        stock_pattern_df.plot(x="date", y=["close"], ax=ax)

    if bin_volume:
        ax2 = ax.twiny()
        ax2.hist(stock_pattern_df["close"], bins=30, weights=stock_pattern_df["cum_volume_pect"], orientation="horizontal", alpha=0.3, label="volume", color="orange")
        # ax2.hist(stock_pattern_df['close'], bins=100, weights=stock_pattern_df['volume'], orientation='horizontal', alpha=0.3, label="volume", color="darkblue")[0:2]
        ax2.set_xlim(ax2.get_xlim()[::-1])
        ax2.yaxis.tick_right()
        ax2.set(xlabel="cum_volume_pect")

    ax = stock_pattern_df.plot(x="date", y="pattern_point", ax=ax, kind="scatter", label=pattern_name, marker="v", color="red")
    ax = ax.scatter(stock_pattern_df[stock_pattern_df["date"]==trading_date]["date"], stock_pattern_df[stock_pattern_df["date"]==trading_date]["pattern_point"], label="as at", marker="v", color="darkred")

    logger.info("saving plotting for [%s]"%(sid))
    img = io.BytesIO()
    plt.savefig(img, format="png", bbox_inches="tight")
    img.seek(0)
    pattern_obj = base64.b64encode(img.getvalue()).decode()
    plt.clf()
    plt.close("all")
    logger.info("plotting done for [%s]"%(sid))

    data_df = pd.merge(pattern_df, plot_df, left_on="start_date", right_on="date", how="inner")
    data_df = _post_process_features(data_df)
    data_df.insert(0, "pattern", pattern_name) # loc at first
    data_df.drop(["pattern", "start_date", "end_date", "name", "sid_x", "sid_y"], axis=1, inplace=True)

    converted_output = "<section id='%s.%s'><div class='row'><a target='_blank' href='%s'>%s</a></div><br>" %(sid, pattern_name, aastock_dynamic_chart_image_url, sid)
    converted_output += "<div class='row'>%s</div><br>" %(data_df.to_html(index=False))
    converted_output += "<div class='row'><img src='data:image/png;base64,%s'></div><br>" %(pattern_obj)
    converted_output += "<div class='row'><img src='%s'></div><br>" %(aastock_chart_image_url)
    converted_output += "<div style='height: 50px !important;'></div></section><br>"
    logger.info("total patterns for [%s]: [%s] "%(sid, len(data_df)))
    return converted_output

def _post_process_features(df):
    df["52w_low_pct_chg"] = df["52w_low_pct_chg"].round().astype(int).astype(str) + "%"
    df["52w_high_pct_chg"] = df["52w_high_pct_chg"].round().astype(int).astype(str) + "%"
    df["volume"] = (df['volume'].astype(float)/1000000).round(2).astype(str) + "M"
    df["avg_volume_5d"] = (df['avg_volume_5d'].astype(float)/1000000).round(2).astype(str) + "M"
    df["ema_volume_20d"] = (df['ema_volume_20d'].astype(float)/1000000).round(2).astype(str) + "M"
    # df["avg_volume_5d"] = df["avg_volume_5d"].astype('int64')
    # df["ema_volume_20d"] = df["ema_volume_20d"].astype('int64')
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
        df = vcp.compute_vcp_features(df)
        if bin_volume:
            df = df[["sid", "Date", "Open", "High", "Low", "Close", "sma_10", "sma_50", "sma_150", "sma_200", "ema_50", "ema_150", "ema_200", "52w_low", "52w_low_pct_chg", "52w_high", "52w_high_pct_chg", "Volume", "avg_volume_5d", "ema_volume_20d", "cum_volume", "cum_volume_pect"]]
        else:
            df = df[["sid", "Date", "Open", "High", "Low", "Close", "sma_10", "sma_50", "sma_150", "sma_200", "ema_50", "ema_150", "ema_200", "52w_low", "52w_low_pct_chg", "52w_high", "52w_high_pct_chg", "Volume", "avg_volume_5d", "ema_volume_20d"]]
    elif "CUP_HANDLE" == pattern_name:
        df = df[["sid", "Date", "Open", "High", "Low", "Close", "Volume"]]
    return df

def _append_features_data(db, sid, trading_date):
    end_date = (datetime.strptime(trading_date, "%Y-%m-%d") + timedelta(1)).strftime("%Y-%m-%d")
    df = db.query_stock("YAHOO", "HK", sid, start=DB_QUERY_START_DATE, end=end_date, letter_case=True)
    df = _compute_pattern_features("SEPA", df)
    df.rename(columns={"Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
    df = _post_process_features(df)
    # print(df.tail(1))
    return df.tail(1)

@app.route("/stockScreener")
def stockScreener():
    return app.send_static_file("screener.html")

@app.route("/getIncomeSummary", methods=["GET", "POST"])
def getIncomeSummary():
    db = DBHelper()
    summary_list = []
    min_eps_growth = request.form.get("min_eps_growth")
    min_net_profit_growth = request.form.get("min_net_profit_growth")
    is_increase = request.form.get("is_increase")
    if is_increase and is_increase == "True":
        is_increase = True
    else:
        is_increase = False
    logger.info("start finding stocks with min eps growth [%s] and min net profit growth [%s], [%s]"%(min_eps_growth, min_net_profit_growth, is_increase))
    cnx = sqlite3.connect(DB_PATH)
    cnx.execute("PRAGMA journal_mode=WAL")

    MIN_EPS_GROWTH = min_eps_growth
    MIN_NET_PROFIT_GROWTH = min_net_profit_growth

    present_ticker_ids = pd.read_sql("SELECT DISTINCT sid FROM stocks WHERE provider = '{}' and market = '{}'".format("YAHOO", "HK"), cnx)
    logger.info(present_ticker_ids)
    incomes_df = pd.read_csv(DATA_PATH + 'income_dfs.csv', index_col=False)
    for sid in present_ticker_ids['sid']:
        # print("processing :", sid)
        # sql = "SELECT basic_eps, eps_growth, net_profit, net_profit_growth, frequency, year FROM incomes WHERE sid='{}'".format(sid)
        # income_df = pd.read_sql(sql, cnx)
        income_df = incomes_df[incomes_df["sid"] == sid]
        if len(income_df) > 0:
            # print(income_df)
            # print("*"*20)
            annual_income_df = income_df[income_df["frequency"]=="annual"].sort_values(by="year", ascending=True)
            quarterly_income_df = income_df[income_df["frequency"]=="quarterly"].sort_values(by="year", ascending=True)
            if len(annual_income_df) > 0 and len(quarterly_income_df) > 0:
                annual_income_df["net_profit_growth"] = np.where(annual_income_df["net_profit_growth"] == '-', pattern_utils.compute_growth(annual_income_df, "net_profit"), annual_income_df["net_profit_growth"])
                annual_income_df["net_profit_growth"] = annual_income_df["net_profit_growth"].apply(pattern_utils.value_to_float)
                annual_income_df["net_profit_growth"] = annual_income_df["net_profit_growth"].astype(float)
                annual_income_df["eps_growth"] = np.where(annual_income_df["eps_growth"] == '-', pattern_utils.compute_growth(annual_income_df, "basic_eps"), annual_income_df["eps_growth"])
                annual_income_df["eps_growth"] = annual_income_df["eps_growth"].apply(pattern_utils.value_to_float)
                annual_income_df["eps_growth"] = annual_income_df["eps_growth"].astype(float)
                # print(annual_income_df)
                # print("*"*20)
                quarterly_income_df["net_profit_growth"] = np.where(quarterly_income_df["net_profit_growth"] == '-', pattern_utils.compute_growth(quarterly_income_df, "net_profit"), quarterly_income_df["net_profit_growth"])
                quarterly_income_df["net_profit_growth"] = quarterly_income_df["net_profit_growth"].apply(pattern_utils.value_to_float)
                quarterly_income_df["net_profit_growth"] = quarterly_income_df["net_profit_growth"].astype(float)
                quarterly_income_df["eps_growth"] = np.where(quarterly_income_df["eps_growth"] == '-', pattern_utils.compute_growth(quarterly_income_df, "basic_eps"), quarterly_income_df["eps_growth"])
                quarterly_income_df["eps_growth"] = quarterly_income_df["eps_growth"].apply(pattern_utils.value_to_float)
                quarterly_income_df["eps_growth"] = quarterly_income_df["eps_growth"].astype(float)
                # print(quarterly_income_df)
                if quarterly_income_df.iloc[-1]["eps_growth"] >= float(MIN_EPS_GROWTH) \
                and annual_income_df.iloc[-1]["net_profit_growth"] >= float(MIN_NET_PROFIT_GROWTH) \
                and annual_income_df.iloc[-2]["net_profit_growth"] >= float(MIN_NET_PROFIT_GROWTH) \
                and annual_income_df.iloc[-3]["net_profit_growth"] >= float(MIN_NET_PROFIT_GROWTH) \
                and (is_increase and annual_income_df.iloc[-1]["net_profit_growth"] > annual_income_df.iloc[-2]["net_profit_growth"] > annual_income_df.iloc[-3]["net_profit_growth"]
                    or not is_increase):
                    logger.info("found: " + sid)
                    summary_list.append(sid)
    logger.info(summary_list)
    if len(summary_list) > 0:
        converted_output = ""
        for sid in summary_list:
            aastock_code = sid.split(".")[0].zfill(5)
            aastock_earnings_summary_url = "http://www.aastocks.com/en/stocks/analysis/company-fundamental/earnings-summary?symbol={}".format(aastock_code)
            converted_output += "<a target='_blank' href='%s'>%s</a><br>" %(aastock_earnings_summary_url, sid)
        logger.info(converted_output)
        return converted_output
    else:
        return "no stocks found"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", PORT))
    app.run(host = HOST, port = port, debug = False) # flask is in threaded mode by default

	# from tornado.wsgi import WSGIContainer
	# from tornado.httpserver import HTTPServer
	# from tornado.ioloop import IOLoop

	# http_server = HTTPServer(WSGIContainer(app))
	# http_server.listen(port)
	# IOLoop.instance().start() 
