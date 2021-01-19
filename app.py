#-*- coding: utf-8 -*-
import os
from flask import Flask, request, jsonify, render_template
from datetime import datetime
from configs.settings import HOST, PORT
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

app = Flask(__name__, template_folder="static")
app.config["JSON_AS_ASCII"] = False
logger = Logger("MainLogger").setup_system_logger()

@app.route("/")
def index():
	  return app.send_static_file("index.html")

@app.route("/getDatePatterns", methods=["GET", "POST"])
def getDatePatterns():
    trading_date = request.form.get("trading_date")
    logger.info("start finding patterns which dated on [%s]", trading_date)

    # pattern_name = Patterns.VCP.name
    db = DBHelper()
    result_df = pd.DataFrame()
    patterns_df = db.query_pattern(start_date=trading_date)
    for index, pattern_df in patterns_df.iterrows():
        upon_row = _get_stock_slice_details(trading_date, pattern_df, db)
        result_df = pd.concat([result_df, upon_row])

    output = result_df.to_json(orient="records")
    json_input = json_parser.loads(output, object_pairs_hook=OrderedDict)
    column_headers = json_input[0].keys()
    print("total stocks: ", len(patterns_df))
    if len(result_df) > 0:
        # sid_list = result_df["sid"].values.T.tolist()
        sid_list = result_df["sid"].to_list()
        converted_output = "<table border='1'; width='100%' style='border-collapse: collapse'><tr>"
        # converted_output += "<th width='100px'>%s</th><td><a id='top'></a></td></tr><tr>"
        converted_output += "<a id='top'></a></tr><tr>"
        per_column_number = 10
        i = 1
        for sid in sid_list:
            if i % per_column_number == 0:
                converted_output += "<td><a href='#%s'>%s</a></td></tr><tr>" %(sid, sid)
            else:
                converted_output += "<td><a href='#%s'>%s</a></td>" %(sid, sid)
            i += 1
        converted_output += "</tr></table><br>"
        for list_entry in json_input:
            data_df = pd.DataFrame()
            sub_converted_output = ""
            converted_output += "<table border='1'; width='100%' style='border-collapse: collapse'><tr>"
            for header in column_headers:
                value = list_entry[header]
                # if 'pattern_chart' == header:
                #     header = 'chart'
                if "sid" == header and value:
                    converted_output += "<th id='%s' width='100px'>%s</th><td>%s</td></tr><tr>" %(value, header, value)
                if header in ["pattern"] and value:
                    converted_output += "<th width='100px'>%s</th><td>%s</td></tr><tr>" %(header, value)
                elif "pattern_chart" == header and value:
                    sub_converted_output += "<th width='100px'>%s</th><td><img src='data:image/png;base64,%s'></td></tr><tr>" %(header, value)
                elif "aastock_chart" == header and value:
                    sub_converted_output += "<th width='100px'>%s</th><td style='padding: 5px 5px 5px 100px;'><img src='%s'></td></tr><tr>" %(header, value)
                elif "dynamic_chart" == header and value:
                    sub_converted_output += "<th width='100px'>%s</th><td><a target='_blank' href='%s'>%s</a></td></tr><tr>" %(header, value, value)
                elif value:
                    data_df.loc[0, header] = value
            converted_output += "<th width='100px'>data</th><td>%s</td></tr><tr>" %(data_df.to_html(index=False))
            converted_output += sub_converted_output
            converted_output += "<th width='100px'>%s</th><td><a href='#top'>Back to top</a></td></tr><tr>"
            converted_output += "</tr></table><br>"
        return converted_output
        # return "Done"
        # return json2html.convert(json = output, clubbing = False)
    else:
        return "no pattern found"

def _get_stock_slice_details(trading_date, pattern_df, db):
    sid = pattern_df["sid"]
    pattern_name = pattern_df["name"]
    aastock_code = sid.split(".")[0].zfill(5)
    aastock_dynamic_chart_image_url = "http://www.aastocks.com/tc/stocks/quote/dynamic-chart.aspx?symbol={}".format(aastock_code)
    aastock_chart_image_url = "http://charts.aastocks.com/servlet/Charts?fontsize=12&15MinDelay=T&lang=1&titlestyle=1&vol=1&Indicator=1&indpara1=10&indpara2=20&indpara3=50&indpara4=150&indpara5=200&subChart1=2&ref1para1=14&ref1para2=0&ref1para3=0&subChart2=3&ref2para1=12&ref2para2=26&ref2para3=9&scheme=6&com=100&chartwidth=870&chartheight=700&stockid={}.HK&period=6&type=1".format(aastock_code)

    df = db.query_stock("YAHOO", "HK", sid, start="2019-01-01", letter_case=True)
    df = vcp.compute_vcp_features(df)
    df = df[(df["Date"] <= trading_date)]
    df = df[["sid", "Date", "Open", "High", "Low", "Close", "sma_10", "sma_150", "sma_200", "52w_low", "Volume", "avg_volume_5d"]]
    df.rename(columns={"Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
    df = df.round(3)
    upon_row = df.tail(1)
    upon_row.insert(0, "pattern", pattern_name)
    # print(upon_row)

    plot_df = pd.DataFrame()
    plot_df = df[(df["sma_200"] > 0)].copy()
    plot_df["date"] = plot_df["date"].astype(str)
    stock_pattern_df = db.query_stock_pattern(sid, pattern_name)
    stock_pattern_df = pd.merge(stock_pattern_df, plot_df, left_on="start_date", right_on="date", how="right")
    stock_pattern_df["start_date"] = stock_pattern_df["start_date"].astype(str)
    stock_pattern_df["pattern_point"] = np.where(stock_pattern_df["start_date"] == stock_pattern_df["date"], stock_pattern_df["high"], None)
    fig, ax = plt.subplots(figsize=(10, 4))
    stock_pattern_df.plot(x="date", y=["close", "sma_10", "sma_150", "sma_200", "52w_low"], ax=ax)
    ax = stock_pattern_df.plot(x="date", y="pattern_point", ax=ax, kind="scatter", label=pattern_name, marker="v")
    img = io.BytesIO()
    plt.savefig(img, format="png")
    img.seek(0)
    pattern_obj = base64.b64encode(img.getvalue()).decode()

    upon_row.loc[:, "pattern_chart"] = pattern_obj
    upon_row.loc[:, "aastock_chart"] = aastock_chart_image_url
    upon_row.loc[:, "dynamic_chart"] = aastock_dynamic_chart_image_url
    return upon_row

if __name__ == "__main__":
	port = int(os.environ.get("PORT", PORT))
	app.run(host = HOST, port = port, debug = False) # flask is in threaded mode by default

	# from tornado.wsgi import WSGIContainer
	# from tornado.httpserver import HTTPServer
	# from tornado.ioloop import IOLoop

	# http_server = HTTPServer(WSGIContainer(app))
	# http_server.listen(port)
	# IOLoop.instance().start() 
