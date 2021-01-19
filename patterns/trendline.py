import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
from patterns.all_patterns import Patterns
from scipy.signal import argrelextrema
from dbhelper import DBHelper
from datetime import datetime

def compute_trendline_features(df, order):
    df["max_close"] = df.iloc[argrelextrema(df["Close"].values, np.greater_equal, order=order)[0]]["Close"]
    # df["max_close_pct_chg"] = df["max_close"].pct_change(periods=1)
    df["min_close"] = df.iloc[argrelextrema(df["Close"].values, np.less_equal, order=order)[0]]["Close"]
    # df["min_close_pct_chg"] = df["min_close"].pct_change(periods=1)
    df = df.loc[(df["max_close"] > 0) | (df["min_close"] > 0)]
    return df

def cal_abs_pect_change(value1, value2):
    return abs(cal_pect_change(value1, value2))

def cal_pect_change(value1, value2):
    return (value1 - value2) / value2 * 100

def find_ascending_triangle(df, order=3, rel_up_tol=1, rel_down_tol=1, min_pattern=10):
    pass

def find_descending_triangle(df, order=3, rel_up_tol=1, rel_down_tol=1, min_pattern=10):
    pass

def find_flat_base_patterns(df, sid, order=3, rel_tol=1, min_pattern=10):
    patterns = defaultdict(list)
    temp_df = df.copy()
    temp_df = compute_trendline_features(temp_df, order)
    start_pattern_index = 0
    flat_base_count = 0
    is_previous_flat_base = False
    is_current_flat_base = False
    db = DBHelper()
    # print("total: ", len(temp_df))
    for i in range(0, (len(temp_df) - 3)):
        # print("-"*10)
        # print(temp_df.iloc[i]["Date"])
        # print("max_close: ", temp_df.iloc[i]["max_close"])
        # print("min_close: ", temp_df.iloc[i]["min_close"])
        # print("start_pattern_index: ", start_pattern_index)
        # print("i: ", i)

        # max max
        if temp_df.iloc[i]["max_close"] > 0 and temp_df.iloc[i+1]["max_close"] > 0 \
        and cal_abs_pect_change(temp_df.iloc[i]["max_close"], temp_df.iloc[i+1]["max_close"]) < rel_tol:
            is_current_flat_base = True
            # print("##########################is_current_flat_base##############################")
            # print(cal_abs_pect_change(temp_df.iloc[i]["max_close"], temp_df.iloc[i+1]["max_close"]))
        # min min
        elif temp_df.iloc[i]["min_close"] > 0 and temp_df.iloc[i+1]["min_close"] > 0 \
        and cal_abs_pect_change(temp_df.iloc[i]["min_close"], temp_df.iloc[i+1]["min_close"]) < rel_tol:
            is_current_flat_base = True
            # print("##########################is_current_flat_base##############################")
            # print(cal_abs_pect_change(temp_df.iloc[i]["min_close"], temp_df.iloc[i+1]["min_close"]))
        # max min max min
        elif temp_df.iloc[i]["max_close"] > 0 and temp_df.iloc[i+1]["min_close"] > 0 \
        and temp_df.iloc[i+2]["max_close"] > 0 and temp_df.iloc[i+3]["min_close"] > 0 \
        and cal_abs_pect_change(temp_df.iloc[i]["max_close"], temp_df.iloc[i+2]["max_close"]) < rel_tol \
        and cal_abs_pect_change(temp_df.iloc[i+1]["min_close"], temp_df.iloc[i+3]["min_close"]) < rel_tol:
            is_current_flat_base = True
            # print("##########################is_current_flat_base##############################")
            # print(cal_abs_pect_change(temp_df.iloc[i]["max_close"], temp_df.iloc[i+2]["max_close"]))
            # print(cal_abs_pect_change(temp_df.iloc[i+1]["min_close"], temp_df.iloc[i+3]["min_close"]))
        # max min min max
        elif temp_df.iloc[i]["max_close"] > 0 and temp_df.iloc[i+1]["min_close"] > 0 \
        and temp_df.iloc[i+2]["min_close"] > 0 and temp_df.iloc[i+3]["max_close"] > 0 \
        and cal_abs_pect_change(temp_df.iloc[i]["max_close"], temp_df.iloc[i+3]["max_close"]) < rel_tol \
        and cal_abs_pect_change(temp_df.iloc[i+1]["min_close"], temp_df.iloc[i+2]["min_close"]) < rel_tol:
            is_current_flat_base = True
            # print("##########################is_current_flat_base##############################")
            # print(cal_abs_pect_change(temp_df.iloc[i]["max_close"], temp_df.iloc[i+3]["max_close"]))
            # print(cal_abs_pect_change(temp_df.iloc[i+1]["min_close"], temp_df.iloc[i+2]["min_close"]))
        # min max min max
        elif temp_df.iloc[i]["min_close"] > 0 and temp_df.iloc[i+1]["max_close"] > 0 \
        and temp_df.iloc[i+2]["min_close"] > 0 and temp_df.iloc[i+3]["max_close"] > 0 \
        and cal_abs_pect_change(temp_df.iloc[i]["min_close"], temp_df.iloc[i+2]["min_close"]) < rel_tol \
        and cal_abs_pect_change(temp_df.iloc[i+1]["max_close"], temp_df.iloc[i+3]["max_close"]) < rel_tol:
            is_current_flat_base = True
            # print("##########################is_current_flat_base##############################")
            # print(cal_abs_pect_change(temp_df.iloc[i]["min_close"], temp_df.iloc[i+2]["min_close"]))
            # print(cal_abs_pect_change(temp_df.iloc[i+1]["max_close"], temp_df.iloc[i+3]["max_close"]))
        # min max max min
        elif temp_df.iloc[i]["min_close"] > 0 and temp_df.iloc[i+1]["max_close"] > 0 \
        and temp_df.iloc[i+2]["max_close"] > 0 and temp_df.iloc[i+3]["min_close"] > 0 \
        and cal_abs_pect_change(temp_df.iloc[i]["min_close"], temp_df.iloc[i+3]["min_close"]) < rel_tol \
        and cal_abs_pect_change(temp_df.iloc[i+1]["max_close"], temp_df.iloc[i+2]["max_close"]) < rel_tol:
            is_current_flat_base = True
            # print("##########################is_current_flat_base##############################")
            # print(cal_abs_pect_change(temp_df.iloc[i]["min_close"], temp_df.iloc[i+3]["min_close"]))
            # print(cal_abs_pect_change(temp_df.iloc[i+1]["max_close"], temp_df.iloc[i+2]["max_close"]))
        else:
            is_current_flat_base = False          

        if not is_current_flat_base and is_previous_flat_base and flat_base_count > min_pattern:
            # patterns["flat base"].append(temp_df.index[-1])
            # patterns[Patterns.FLAT_BASE.name].append((temp_df.iloc[start_pattern_index]["Date"], temp_df.iloc[i]["Date"]))
            start_date = temp_df.iloc[start_pattern_index]["Date"]
            end_date = temp_df.iloc[i]["Date"]
            name = Patterns.FLAT_BASE.name
            patterns[name].append((start_date, end_date))
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
            db.insert_pattern(start_date, name, sid, end_date)
        elif is_current_flat_base and not is_previous_flat_base:
            start_pattern_index = i

        if is_current_flat_base:
            is_previous_flat_base = True
            flat_base_count = flat_base_count + 1
        else:
            is_previous_flat_base = False
            flat_base_count = 0

    return patterns