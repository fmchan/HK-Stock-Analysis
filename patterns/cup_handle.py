import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters
from datetime import datetime, timedelta
import os
import sys
import multiprocessing
from scipy.signal import argrelextrema, argrelmin, argrelmax, find_peaks, find_peaks_cwt
from collections import defaultdict
from configs.settings import DATA_PATH, CURRENT_VOLUME_FILTER
import services.price as price
from patterns.all_patterns import Patterns

smoothing = 0
window = 0
prominence = 0.01 # 0.01 is the best after testing
neighbour = 4 # when using find_peaks()
# neighbour = 3 # when using argrelextrema()

# min_period = 20
min_period = 27
max_radius_difference = 5
peak_depth = 1.05
step_depth = 1.01
start_end_price_ratio_difference = 0.07

# def is_cup_n_handle(df):
#     try:
#         previous_10 = df["sma_10"].shift(1)
#         previous_20 = df["sma_20"].shift(1)
#         previous_20_min = df["min_20"].shift(1)
#         crossing = (((df["sma_10"] <= df["sma_20"]) & (previous_10 >= previous_20))
#                     | ((df["sma_10"] >= df["sma_20"]) & (previous_10 <= previous_20))
#                     & (df["min_20"] >= previous_20_min))
#         cross_list = df.loc[crossing].date.values
#         # print(cross_list)
#         if len(cross_list) == 2:
#             in_range_df = df[df["date"].isin(pd.date_range(cross_list[0], cross_list[1]))]
#             df_count = len(in_range_df)
#             # print(in_range_df)
#             if in_range_df.iloc[0]["sma_20"] > in_range_df.iloc[0]["sma_10"] \
#               and in_range_df.iloc[df_count-1]["sma_20"] < in_range_df.iloc[df_count-1]["sma_10"] \
#               and in_range_df.min()["sma10_over_sma20"] < 0.97 \
#               and df_count >= 10:
#                 return True
#             else:
#                 return False
#         else:
#             return False
#     except Exception as e:
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         print(exc_type, fname, exc_tb.tb_lineno)
#         return False

# def detect_pattern(df, day_delta):
#     if is_cup_n_handle(df):
#         print(sid)

def get_max_min(df, smoothing, window_range, neighbour, prominence, debug=False):
    try:
        if neighbour == 0: # min neighbour is 1
            neighbour = 1
        if smoothing == 0:
            smooth_prices = df["close"]
        else:
            smooth_prices = df["close"].rolling(window=smoothing).mean().dropna()
        # local_max = argrelextrema(smooth_prices.values, np.greater, order=neighbour, mode="clip")[0]
        # local_min = argrelextrema(smooth_prices.values, np.less, order=neighbour, mode="clip")[0]
        local_max = find_peaks(smooth_prices.values, distance=neighbour, prominence=prominence)[0]
        local_max = np.append(local_max, len(df)-1, axis=None)
        local_min = find_peaks(1/smooth_prices.values, distance=neighbour, prominence=prominence)[0]
        # local_max = find_peaks_cwt(smooth_prices.values, np.arange(1, 1))
        # local_min = find_peaks_cwt(1/smooth_prices.values, np.arange(1, 1))
        price_local_max_dt = []
        for i in local_max:
            if window_range == 0:
                price_local_max_dt.append(df.index.values[i])
            elif (i > window_range) and (i < len(df) - window_range):
                price_local_max_dt.append(df.iloc[i - window_range : i + window_range]["close"].idxmax())
        price_local_min_dt = []
        for i in local_min:
            if window_range == 0:
                price_local_min_dt.append(df.index.values[i])
            elif (i > window_range) and (i < len(df) - window_range):
                price_local_min_dt.append(df.iloc[i - window_range : i + window_range]["close"].idxmin())
        maxima = pd.DataFrame(df.loc[price_local_max_dt])
        minima = pd.DataFrame(df.loc[price_local_min_dt])
        max_min = pd.concat([maxima, minima]).sort_index()
        max_min.index.name = "DateTime"
        max_min = max_min.reset_index()
        max_min = max_min[~max_min.DateTime.duplicated()]
        # p = df.reset_index()
        # max_min["day_num"] = p[p["date"].isin(max_min.DateTime)].index.values
        # max_min = max_min.set_index("day_num")["close"]
        if debug:
            print(maxima[["close"]])
            print(minima[["close"]])
        return max_min
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(e)
        return ""

# def find_patterns(max_min):  
#     patterns = defaultdict(list)
#     window_units = 5
#     peak_depth = 1.05
#     step_depth = 1.02
#     max_start_end_difference = 1.07
#     min_start_end_difference = 0.93
#     min_period = 25
#     max_difference = 5
#     for i in range(window_units, len(max_min)+1):  
#         window = max_min.iloc[i - window_units:i]
#         if window.index[-1] - window.index[0] > 100:      
#             continue   

#         # print(window.iloc[0:window_units][["date", "close"]])
#         a_close, b_close, c_close, d_close, e_close = window.iloc[0:window_units]["close"]
#         a_volume, b_volume, c_volume, d_volume, e_volume = window.iloc[0:window_units]["volume"]
#         a_date, b_date, c_date, d_date, e_date = window.iloc[0:window_units]["date"]
#         a_e_day_difference = (e_date - a_date).days
#         a_c_day_difference = (c_date - a_date).days
#         b_e_day_difference = (e_date - b_date).days
#         c_e_day_difference = (e_date - c_date).days
#         d_e_day_difference = (e_date - d_date).days
#         a_d_day_difference = (d_date - a_date).days
#         a_b_day_difference = (b_date - a_date).days
#         b_d_day_difference = (d_date - b_date).days
#         c_d_day_difference = (d_date - c_date).days
#         a_b_ratio = a_close / b_close
#         a_c_ratio = a_close / c_close
#         a_d_ratio = a_close / d_close
#         a_e_ratio = a_close / e_close if a_close > e_close else e_close / a_close
#         b_c_ratio = c_close / b_close if c_close > b_close else b_close / c_close
#         b_d_ratio = d_close / b_close if d_close > b_close else b_close / d_close
#         b_e_ratio = e_close / b_close
#         c_d_ratio = d_close / c_close if d_close > c_close else c_close / d_close
#         c_e_ratio = e_close / c_close
#         d_e_ratio = e_close / d_close
#         # a_min_volume_5, b_min_volume_5, c_min_volume_5 = window.iloc[0:window_units]["min_volume_5"]
#         # print(window.iloc[0:window_units])

#         # b_close is the lowest peak among 5 points
#         if a_close > b_close and c_close > b_close and d_close > c_close and e_close > d_close \
#             and b_volume < a_volume and b_volume < e_volume \
#             and a_b_ratio > peak_depth and b_e_ratio > peak_depth \
#             and b_c_ratio > step_depth and c_d_ratio > step_depth and d_e_ratio > step_depth \
#             and min_start_end_difference < a_e_ratio < max_start_end_difference \
#             and a_e_day_difference >= min_period \
#             and abs(a_b_day_difference - b_e_day_difference) <= max_difference:
#                 patterns["cup_handle"].append((window.iloc[0]["date"], window.iloc[-1]["date"]))
#         # c_close is the lowest peak among 5 points
#         elif a_close > b_close and b_close > c_close and d_close > c_close and e_close > d_close \
#             and c_volume < a_volume and c_volume < e_volume \
#             and a_c_ratio > peak_depth and c_e_ratio > peak_depth \
#             and a_b_ratio > step_depth and b_c_ratio > step_depth and c_d_ratio > step_depth and d_e_ratio > step_depth \
#             and min_start_end_difference < a_e_ratio < max_start_end_difference \
#             and a_e_day_difference >= min_period \
#             and abs(a_c_day_difference - c_e_day_difference) <= max_difference:
#                 patterns["cup_handle"].append((window.iloc[0]["date"], window.iloc[-1]["date"]))
#         # d_close is the lowest peak among 5 points
#         elif a_close > b_close and b_close > c_close and c_close > d_close and e_close > c_close \
#             and d_volume < a_volume and d_volume < e_volume \
#             and a_d_ratio > peak_depth and d_e_ratio > peak_depth \
#             and a_b_ratio > step_depth and b_c_ratio > step_depth and c_d_ratio > step_depth \
#             and min_start_end_difference < a_e_ratio < max_start_end_difference \
#             and a_e_day_difference >= min_period \
#             and abs(a_d_day_difference - d_e_day_difference) <= max_difference:
#                 patterns["cup_handle"].append((window.iloc[0]["date"], window.iloc[-1]["date"]))
#         # b_close is the lowest peak among 4 points (a,b,c,d)
#         elif a_close > b_close and c_close > b_close and d_close > c_close \
#             and b_volume < a_volume and b_volume < d_volume \
#             and a_b_ratio > peak_depth and b_d_ratio > peak_depth \
#             and b_c_ratio > step_depth and c_d_ratio > step_depth \
#             and min_start_end_difference < a_d_ratio < max_start_end_difference \
#             and a_d_day_difference >= min_period \
#             and abs(a_b_day_difference - b_d_day_difference) <= max_difference:
#                 patterns["cup_handle"].append((window.iloc[0]["date"], window.iloc[-2]["date"]))
#         # c_close is the lowest peak among 4 points (a,b,c,d)
#         elif a_close > b_close and b_close > c_close and d_close > b_close \
#             and c_volume < a_volume and c_volume < d_volume \
#             and a_c_ratio > peak_depth and c_d_ratio > peak_depth \
#             and a_b_ratio > step_depth and b_c_ratio > step_depth \
#             and min_start_end_difference < a_d_ratio < max_start_end_difference \
#             and a_d_day_difference >= min_period \
#             and abs(a_c_day_difference - c_d_day_difference) <= max_difference:
#                 patterns["cup_handle"].append((window.iloc[0]["date"], window.iloc[-2]["date"]))
#         # # c_close is the lowest peak among 4 points (b,c,d,e)
#         # elif b_close > c_close and d_close > c_close and e_close > d_close \
#         #     and c_volume < b_volume and c_volume < e_volume \
#         #     and b_e_day_difference >= 25 and b_c_day_difference >=5 and c_e_day_difference >= 5:
#         #         patterns["cup_handle"].append((window.iloc[1]["date"], window.iloc[-1]["date"]))
#         # # d_close is the lowest peak among 4 points (b,c,d,e)
#         # elif b_close > c_close and c_close > d_close and e_close > c_close \
#         #     and d_volume < b_volume and d_volume < e_volume \
#         #     and b_e_day_difference >= 25 and b_d_day_difference >=5 and d_e_day_difference >= 5:
#         #         patterns["cup_handle"].append((window.iloc[1]["date"], window.iloc[-1]["date"]))

#     return patterns

def find_patterns(max_min, min_period=27, peak_depth=1.05, start_end_price_ratio_difference=0.07, max_radius_difference=5, step_depth=1.01):
    try:
        patterns = defaultdict(list)
        # window_units = 6
        for window_units in range(3, 10):
            for i in range(window_units, len(max_min)+1):
                window = max_min.iloc[i - window_units:i]
                if window.index[-1] - window.index[0] > 100:
                    continue   

                closes = []
                volumes = []
                dates = []
                for j in range(0, window_units):
                    closes.append(window.iloc[j]["close"])
                    volumes.append(window.iloc[j]["volume"])
                    dates.append(window.iloc[j]["date"])
                pattern_day_difference = (dates[window_units-1] - dates[0]).days
                start_end_price_ratio = closes[window_units-1] / closes[0] if closes[window_units-1] > closes[0] else closes[0] / closes[window_units-1]

                min_index = closes.index(min(closes))
                min_close = min(closes)
                entry_bottom_day_difference = (dates[min_index] - dates[0]).days
                bottom_end_day_difference = (dates[window_units-1] - dates[min_index]).days
                entry_bottom_price_ratio = closes[0] / min_close
                bottom_end_price_ratio = closes[window_units-1] / min_close
                criteria_1 = False
                criteria_2 = False
                for k in range(0, min_index):
                    if closes[k] > closes[k+1] and closes[k] / closes[k+1] > step_depth:
                        criteria_1 = True
                        continue
                    else:
                        criteria_1 = False
                        break
                if min_index >= 1 and window_units > 3:
                    for l in range(min_index, window_units-1):
                        if closes[l] < closes[l+1] and closes[l+1] / closes[l] > step_depth:
                            criteria_2 = True
                            continue
                        else:
                            criteria_2 = False
                            break
                elif closes[1] < closes[2] and closes[2] / closes[1] > step_depth:
                    criteria_2 = True
                else:
                    criteria_2 = False

                if criteria_1 and criteria_2 \
                    and volumes[min_index] < volumes[0] and volumes[min_index] < volumes[window_units-1] \
                    and entry_bottom_price_ratio > peak_depth and bottom_end_price_ratio > peak_depth \
                    and 1 - start_end_price_ratio_difference < start_end_price_ratio < 1 + start_end_price_ratio_difference \
                    and pattern_day_difference >= min_period \
                    and abs(entry_bottom_day_difference - bottom_end_day_difference) <= max_radius_difference:
                        patterns[Patterns.CUP_HANDLE.name].append((window.iloc[0]["date"], window.iloc[-1]["date"]))

        return patterns
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(e)
        return ''

def find_cup_patterns(df):
    minmax = get_max_min(df, smoothing, window, neighbour, prominence, debug=False)
    # print(minmax[["date", "close", "volume"]].tail(20))
    patterns = find_patterns(minmax, min_period, peak_depth, start_end_price_ratio_difference, max_radius_difference, step_depth)
    return patterns