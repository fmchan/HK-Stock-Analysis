#-*- coding: utf-8 -*-
import os
from flask import Flask, request, jsonify, render_template
from datetime import datetime
from configs.settings import HOST, PORT, DB_QUERY_START_DATE
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

pd.set_option('display.max_columns', None)
app = Flask(__name__, template_folder="static")
app.config["JSON_AS_ASCII"] = False
cache = Cache(app, config={"CACHE_TYPE": "simple"})
logger = Logger("MainLogger").setup_system_logger()

def make_cache_key(*args, **kwargs):
    return request.form.get("trading_date"), request.form.get("pattern_name")

@app.route("/")
def index():
	  return app.send_static_file("index.html")

@app.route("/getStockPatterns", methods=["GET", "POST"])
@cache.cached(timeout=3600, key_prefix=make_cache_key)
def getStockPatterns():
    db = DBHelper()
    trading_date = request.form.get("trading_date")
    pattern_name = request.form.get("pattern_name")
    logger.info("start finding [%s] patterns which dated on [%s]"%(pattern_name, trading_date))

    patterns_df = db.query_pattern(start_date=trading_date, name=pattern_name)
    logger.info("total stocks: [%s] "%(len(patterns_df)))
    if len(patterns_df) > 0:
        sid_set = set(patterns_df["sid"])
        sorted_list = sorted(sid_set)

        # converted_output = "<table border='1'; width='100%' style='border-collapse: collapse'><tr>"
        # converted_output += "<a id='top'></a></tr><tr>"
        # for sid in sorted_list:
        #     converted_output += "<td><a href='#%s.%s'>%s</a></td></tr><tr>" %(sid, pattern_name, sid)
        # converted_output += "</tr></table><br>"
        converted_output = "<div><nav class='sidediv'><ul>"
        for sid in sorted_list:
            converted_output += "<li><a href='#%s.%s'>%s</a></li>" %(sid, pattern_name, sid)
        converted_output += "</ul></nav>"

        converted_output += "<div class='content'>"
        for sid in sorted_list:
            converted_output += _get_stock_output(db, trading_date, sid, pattern_name)
        converted_output += "</div></div><br>"

        # return converted_output
        return render_template('result.html', trading_date = trading_date, content = converted_output)
    else:
        return "no pattern found"

def _get_stock_output(db, trading_date, sid, pattern_name, bin_volume=False):
    logger.info("getting output for %s "%(sid))
    aastock_code = sid.split(".")[0].zfill(5)
    aastock_dynamic_chart_image_url = "http://www.aastocks.com/tc/stocks/quote/dynamic-chart.aspx?symbol={}".format(aastock_code)
    aastock_chart_image_url = "http://charts.aastocks.com/servlet/Charts?fontsize=12&15MinDelay=T&lang=1&titlestyle=1&vol=1&Indicator=1&indpara1=10&indpara2=20&indpara3=50&indpara4=150&indpara5=200&subChart1=2&ref1para1=14&ref1para2=0&ref1para3=0&subChart2=3&ref2para1=12&ref2para2=26&ref2para3=9&scheme=6&com=100&chartwidth=870&chartheight=700&stockid={}.HK&period=6&type=1".format(aastock_code)

    df = db.query_stock("YAHOO", "HK", sid, start=DB_QUERY_START_DATE, letter_case=True)
    df = _compute_pattern_features(pattern_name, df, bin_volume)
    df.rename(columns={"Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)

    plot_df = df.copy()
    plot_df["date"] = plot_df["date"].astype(str)
    pattern_df = db.query_stock_pattern(sid, pattern_name) # to avoid too much patterns to be plot
    # stock_pattern_df = db.query_stock_pattern(sid) # to display all kinds of patterns for single stock
    stock_pattern_df = pd.merge(pattern_df, plot_df, left_on="start_date", right_on="date", how="right")
    stock_pattern_df["start_date"] = stock_pattern_df["start_date"].astype(str)
    stock_pattern_df["pattern_point"] = np.where(stock_pattern_df["start_date"] == stock_pattern_df["date"], stock_pattern_df["close"]*1.02, None)
    past_6_month = (datetime.now() - relativedelta(months=6))
    stock_pattern_df["date"] = pd.to_datetime(stock_pattern_df["date"])
    stock_pattern_df = stock_pattern_df[stock_pattern_df["date"] > past_6_month]
    # stock_pattern_df = stock_pattern_df.iloc[-120:] # show latest 6 months (20 days * 6)
    fig, ax = plt.subplots(figsize=(10, 4))
    if pattern_name in ["VCP", "VCP2"]:
        stock_pattern_df = stock_pattern_df[(stock_pattern_df["sma_200"] > 0)]
        stock_pattern_df.plot(x="date", y=["close", "sma_50", "sma_150", "sma_200", "52w_low", "52w_high"], ax=ax)
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

    img = io.BytesIO()
    plt.savefig(img, format="png", bbox_inches="tight")
    img.seek(0)
    pattern_obj = base64.b64encode(img.getvalue()).decode()
    plt.clf()
    plt.close("all")

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
    df = df.round(3)
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

    if pattern_name in ["VCP", "VCP2"]:
        df = vcp.compute_vcp_features(df)
        if bin_volume:
            df = df[["sid", "Date", "Open", "High", "Low", "Close", "sma_10", "sma_50", "sma_150", "sma_200", "52w_low", "52w_low_pct_chg", "52w_high", "52w_high_pct_chg", "Volume", "avg_volume_5d", "cum_volume", "cum_volume_pect"]]
        else:
            df = df[["sid", "Date", "Open", "High", "Low", "Close", "sma_10", "sma_50", "sma_150", "sma_200", "52w_low", "52w_low_pct_chg", "52w_high", "52w_high_pct_chg", "Volume", "avg_volume_5d"]]
    elif "CUP_HANDLE" == pattern_name:
        df = df[["sid", "Date", "Open", "High", "Low", "Close", "Volume"]]
    return df

if __name__ == "__main__":
	port = int(os.environ.get("PORT", PORT))
	app.run(host = HOST, port = port, debug = False) # flask is in threaded mode by default

	# from tornado.wsgi import WSGIContainer
	# from tornado.httpserver import HTTPServer
	# from tornado.ioloop import IOLoop

	# http_server = HTTPServer(WSGIContainer(app))
	# http_server.listen(port)
	# IOLoop.instance().start() 
