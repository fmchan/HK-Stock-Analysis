import pandas as pd
import numpy as np
from collections import defaultdict
from patterns.all_patterns import Patterns
import pandas_ta as ta
from dbhelper import DBHelper
from datetime import datetime
import logging

volume_rolling_period = 5

def compute_sepa_features(df):
    # df["avg_volume_{}d".format(volume_rolling_period)] = df["Volume"].rolling(volume_rolling_period).mean()
    df["avg_volume_5d"] = df["Volume"].rolling(5).mean()
    df["ema_volume_20d"] = df["Volume"].ewm(20).mean()
    df["sma_10"] = df["Close"].rolling(10).mean()
    df["sma_50"] = df["Close"].rolling(50).mean()
    df["sma_150"] = df["Close"].rolling(150).mean()
    df["sma_200"] =df["Close"].rolling(200).mean()
    df["ema_50"] = df.ta.ema(close=df["Close"], length=50, append=False)
    df["ema_150"] = df.ta.ema(close=df["Close"], length=150, append=False)
    df["ema_200"] = df.ta.ema(close=df["Close"], length=200, append=False)
    # df["ema_50"] = df["Close"].ewm(50).mean()
    # df["ema_150"] = df["Close"].ewm(150).mean()
    # df["ema_200"] =df["Close"].ewm(200).mean()
    df["52w_low"] = df["Close"].rolling('365D').min() # use 365D is more accurated than using 260 (52*5)
    df["52w_low_pct_chg"] = (df["Close"] - df["52w_low"]) / df["52w_low"] * 100
    df["52w_high"] = df["Close"].rolling('365D').max() # use 365D is more accurated than using 260 (52*5)
    df["52w_high_pct_chg"] = (df["Close"] - df["52w_high"]) / df["52w_high"] * 100
    return df

def find_patterns(df, sid, last_row_no=100):
    db = DBHelper()
    logger = logging.getLogger('MainLogger')
    patterns = defaultdict(list)
    start_window_unit = 3
    temp_df = df.copy()
    temp_df = compute_sepa_features(temp_df)
    temp_df = temp_df.iloc[-last_row_no:]
    # print(temp_df[["Close", "sma_10", "sma_150", "sma_200", "52w_low"]])
    for i in range(start_window_unit, len(temp_df)+1):
        temp_sub_df = temp_df.iloc[i-start_window_unit:i]
        if temp_sub_df.iloc[-1]["sma_200"] < temp_sub_df.iloc[-1]["sma_150"] \
          and temp_sub_df.iloc[-1]["sma_150"] < temp_sub_df.iloc[-1]["Close"] \
          and temp_sub_df.iloc[-1]["52w_low"] * 1.25 < temp_sub_df.iloc[-1]["Close"] \
          and temp_sub_df.iloc[-1]["avg_volume_{}d".format(volume_rolling_period)] < temp_sub_df.iloc[-1]["Volume"] \
          and temp_sub_df.iloc[-2]["Close"] < temp_sub_df.iloc[-1]["Close"]:
            start_date = temp_sub_df.iloc[-1]["Date"]
            name = Patterns.SEPA.name
            patterns[name].append(start_date)
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            db.insert_pattern(start_date, name, sid)
        if temp_sub_df.iloc[-1]["sma_200"] < temp_sub_df.iloc[-1]["sma_150"] \
          and temp_sub_df.iloc[-1]["sma_150"] < temp_sub_df.iloc[-1]["sma_50"] \
          and temp_sub_df.iloc[-1]["sma_50"] < temp_sub_df.iloc[-1]["Close"] \
          and temp_sub_df.iloc[-1]["52w_low"] * 1.3 < temp_sub_df.iloc[-1]["Close"] \
          and temp_sub_df.iloc[-1]["52w_high"] * 0.75 < temp_sub_df.iloc[-1]["Close"] \
          and temp_sub_df.iloc[-1]["avg_volume_{}d".format(volume_rolling_period)] < temp_sub_df.iloc[-1]["Volume"] \
          and temp_sub_df.iloc[-2]["Close"] < temp_sub_df.iloc[-1]["Close"]:
            start_date = temp_sub_df.iloc[-1]["Date"]
            name = "SEPA2"
            patterns[name].append(start_date)
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            db.insert_pattern(start_date, name, sid)
        if temp_sub_df.iloc[-1]["ema_200"] < temp_sub_df.iloc[-1]["ema_150"] \
          and temp_sub_df.iloc[-1]["ema_150"] < temp_sub_df.iloc[-1]["ema_50"] \
          and temp_sub_df.iloc[-1]["ema_50"] < temp_sub_df.iloc[-1]["Close"] \
          and temp_sub_df.iloc[-1]["52w_low"] * 1.3 < temp_sub_df.iloc[-1]["Close"] \
          and temp_sub_df.iloc[-1]["52w_high"] * 0.75 < temp_sub_df.iloc[-1]["Close"] \
          and temp_sub_df.iloc[-1]["ema_volume_20d"] < temp_sub_df.iloc[-1]["Volume"] \
          and temp_sub_df.iloc[-2]["Close"] < temp_sub_df.iloc[-1]["Close"]:
            start_date = temp_sub_df.iloc[-1]["Date"]
            name = "SEPA3"
            patterns[name].append(start_date)
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            db.insert_pattern(start_date, name, sid)
    logger.info("analysed SEPA for : {}".format(sid))
    return patterns