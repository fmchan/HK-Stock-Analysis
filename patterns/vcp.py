import pandas as pd
import numpy as np
from collections import defaultdict
from patterns.all_patterns import Patterns
import pandas_ta as ta
from dbhelper import DBHelper
from datetime import datetime
import logging

volume_rolling_period = 5

def compute_vcp_features(df):
    # df["avg_volume_{}d".format(volume_rolling_period)] = df["Volume"].rolling(volume_rolling_period).mean()
    df["avg_volume_5d"] = df["Volume"].rolling(5).mean()
    df["sma_10"] = df["Close"].rolling(10).mean()
    df["sma_50"] = df["Close"].rolling(50).mean()
    df["sma_150"] = df["Close"].rolling(150).mean()
    df["sma_200"] =df["Close"].rolling(200).mean()
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
    temp_df = compute_vcp_features(temp_df)
    temp_df = temp_df.iloc[-last_row_no:]
    # print(temp_df[["Close", "sma_10", "sma_150", "sma_200", "52w_low"]])
    for i in range(start_window_unit, len(temp_df)+1):
        temp_sub_df = temp_df.iloc[i-start_window_unit:i]
        # print(temp_sub_df)
        if temp_sub_df.iloc[-1]["sma_200"] < temp_sub_df.iloc[-1]["sma_150"] \
          and temp_sub_df.iloc[-1]["sma_150"] < temp_sub_df.iloc[-1]["Close"] \
          and temp_sub_df.iloc[-1]["52w_low"] * 1.25 < temp_sub_df.iloc[-1]["Close"] \
          and temp_sub_df.iloc[-1]["avg_volume_{}d".format(volume_rolling_period)] < temp_sub_df.iloc[-1]["Volume"] \
          and temp_sub_df.iloc[-2]["Close"] < temp_sub_df.iloc[-1]["Close"]:
            start_date = temp_sub_df.iloc[-1]["Date"]
            name = Patterns.VCP.name
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
            # name = Patterns.VCP.name
            name = "VCP2"
            patterns[name].append(start_date)
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            db.insert_pattern(start_date, name, sid)
    logger.info("analysed VCP for : {}".format(sid))
    return patterns